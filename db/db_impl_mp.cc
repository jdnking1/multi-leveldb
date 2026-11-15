// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The parts of DBImpl that only exist for Options::multi_process.
//
// Overview
// ========
//
// Every process that opens the database has the usual in-memory state: a
// memtable, possibly an immutable memtable, and a VersionSet.  What is new
// is that this state is a *replica* that each process maintains for itself
// from the files, guided by a small shared memory block (SharedState):
//
//   WAL       All processes append to the same log file, one at a time
//             (WRITE.lock).  After each append the writer publishes
//             {log number, committed offset, last sequence}.  Everybody
//             else replays the log up to that offset into its own memtable.
//
//   MANIFEST  One process at a time is the "compactor" (COMPACT.lock).  It
//             alone flushes memtables, compacts, writes the MANIFEST and
//             deletes files.  After each edit it publishes {manifest number,
//             committed offset, epoch}.  Everybody else applies the edits up
//             to that offset to its own VersionSet.
//
// CatchUp() does both kinds of replay and is called before every read, by
// the writer once it holds WRITE.lock, and periodically by a poll thread.
//
// Log rotation.  A writer that finds the memtable full creates a new log
// and publishes its number.  Each process, on seeing the new number, reads
// the old log to its end and turns its memtable into its immutable
// memtable.  The compactor writes *its* immutable memtable to a table and
// logs an edit whose log number tells all processes, when they apply the
// edit, to drop theirs.  Writers do not rotate again before that, so a
// process never has more than one immutable memtable.
//
// Deleting files.  Only the compactor deletes.  Before reading table files
// a process publishes the epoch of the oldest Version it uses ("pin").  The
// compactor keeps every Version it has produced alive until no live process
// pins an epoch that old, so the files stay too.  See MpBeginRead() for why
// this is free of races.

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/log_tailer.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

#include "util/mutexlock.h"
#include "util/shared_state.h"

namespace leveldb {

namespace {

// How often a writable process looks for work that the compactor should do
// (and whether there still is a compactor).
const int kPollMicros = 1000;
// How long a writer that is waiting for the compactor sleeps between looks.
const int kStallMicros = 500;

// Offset just past the last complete record of the log file "fname".
Status ValidLogLength(Env* env, const std::string& fname, uint64_t* length) {
  LogTailer tailer(env, fname);
  Status s = tailer.Open(0);
  if (!s.ok()) return s;
  tailer.SetLimit(LogTailer::kNoLimit);
  Slice record;
  std::string scratch;
  while (tailer.ReadRecord(&record, &scratch)) {
  }
  *length = tailer.Position();
  return Status::OK();
}

}  // namespace

Status DBImpl::MpOpen() {
  env_->CreateDir(dbname_);  // Ignoring errors, as DBImpl::Recover() does.

  bool first = false;
  Status s = SharedState::Open(dbname_, &mp_, &first);
  if (!s.ok()) {
    return s;
  }
  // From here until FinishOpen() no other process can be opening.

  MutexLock l(&mutex_);
  versions_->SetSharedState(mp_);

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (!first) {
      s = Status::Corruption(dbname_, "CURRENT is missing");
    } else if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
    } else {
      s = Status::InvalidArgument(
          dbname_, options_.read_only
                       ? "does not exist (read_only is true)"
                       : "does not exist (create_if_missing is false)");
    }
  } else if (options_.error_if_exists) {
    s = Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
  }

  if (s.ok() && first) {
    mp_->Reset();
  }
  if (s.ok()) {
    // The new slot pins everything, so nothing this process is about to
    // read can be deleted or compacted away under it.
    s = mp_->ClaimSlot();
  }
  if (s.ok()) {
    mem_ = new MemTable(internal_comparator_);
    mem_->Ref();
    s = first ? MpInitStateFromDisk() : CatchUp();
  }
  if (s.ok()) {
    if (first) {
      mp_->MarkValid();
    }
    MpEndRead();  // Replaces the claim-everything pin.
    mp_->SetOldestSnapshot(SharedState::kNone);
    Log(options_.info_log, "Multi-process open: %s, %d live process(es)",
        first ? "first (state rebuilt)" : "joined", mp_->LiveProcesses());
  }
  mp_->FinishOpen();

  if (s.ok() && !options_.read_only) {
    poll_thread_ = std::thread([this] { MpPollLoop(); });
  }
  return s;
}

Status DBImpl::MpInitStateFromDisk() {
  mutex_.AssertHeld();

  // Which manifest, and how much of it is intact?
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);
  uint64_t manifest_number;
  FileType type;
  if (!ParseFileName(current, &manifest_number, &type) ||
      type != kDescriptorFile) {
    return Status::Corruption("CURRENT does not name a manifest", current);
  }
  SharedState::ManifestState manifest;
  manifest.manifest_number = manifest_number;
  manifest.epoch = 1;
  s = ValidLogLength(env_, DescriptorFileName(dbname_, manifest_number),
                     &manifest.manifest_offset);
  if (s.IsNotFound()) {
    return Status::Corruption("CURRENT points to a non-existent file",
                              s.ToString());
  } else if (!s.ok()) {
    return s;
  }
  mp_->PublishManifest(manifest);
  s = versions_->TailManifest(manifest.manifest_number,
                              manifest.manifest_offset, manifest.epoch);
  if (!s.ok()) {
    return s;
  }

  // File numbers in use, and the newest log that is still needed.
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  SharedState::WalState wal;
  for (const std::string& filename : filenames) {
    uint64_t number;
    if (ParseFileName(filename, &number, &type)) {
      if (type == kLogFile || type == kTableFile || type == kDescriptorFile ||
          type == kTempFile) {
        mp_->MarkFileNumberUsed(number);
      }
      if (type == kLogFile && number >= versions_->LogNumber() &&
          number > wal.log_number) {
        wal.log_number = number;
      }
    }
  }
  if (wal.log_number != 0) {
    const std::string fname = LogFileName(dbname_, wal.log_number);
    s = ValidLogLength(env_, fname, &wal.log_offset);
    uint64_t size = 0;
    if (s.ok()) {
      s = env_->GetFileSize(fname, &size);
    }
    if (!s.ok()) {
      return s;
    }
    if (size != wal.log_offset) {
      // A writer died in the middle of an append.  The first writer cuts
      // the incomplete record off before appending behind it.
      mp_->SetAppendInProgress(true);
    }
  }
  mp_->PublishWal(wal);

  // Replay the logs.
  s = CatchUp();
  if (!s.ok()) {
    return s;
  }

  // The last sequence is whatever the manifest and the logs add up to.
  if (versions_->LastSequence() < versions_->ManifestLastSequence()) {
    versions_->SetLastSequence(versions_->ManifestLastSequence());
  }
  wal.last_sequence = versions_->LastSequence();
  wal_version_seen_ = mp_->PublishWal(wal);
  return Status::OK();
}

void DBImpl::MpNewMemTable() {
  mutex_.AssertHeld();
  mem_->Unref();
  mem_ = new MemTable(internal_comparator_);
  mem_->Ref();
  mem_nonempty_ = false;
}

Status DBImpl::CatchUp() {
  mutex_.AssertHeld();
  assert(mp_ != nullptr);

  for (int attempt = 0; attempt < 10000; attempt++) {
    // The WAL state must be read before the manifest state: a log rotation
    // is only published after the edit that flushed the previous log, so
    // this order guarantees that the manifest state read here already
    // includes that edit.
    uint64_t wal_version, manifest_version;
    SharedState::WalState wal = mp_->ReadWal(&wal_version);
    const SharedState::ManifestState manifest =
        mp_->ReadManifest(&manifest_version);
    // The compactor's VersionSet is the source of the manifest state.
    const bool follow_manifest = !versions_->IsManifestWriter();
    if (wal_version == wal_version_seen_ &&
        (!follow_manifest || manifest_version == manifest_version_seen_)) {
      return Status::OK();
    }
    if (attempt > 10) {
      // The state keeps moving under this process (or files were removed
      // while they were being read; the state that made them obsolete has
      // been published by then).  Back off a little.
      env_->SleepForMicroseconds(100);
    }

    Status s;
    if (follow_manifest && manifest_version != manifest_version_seen_) {
      // The opposite constraint also holds: the manifest state must not be
      // ahead of the sequence number this process exposes.  A compaction
      // drops entries that are shadowed as of the compactor's last
      // sequence; a reader that used the resulting version with an older
      // last sequence would see neither the dropped entry nor the newer
      // one.  The WAL state as of now is new enough.  Use it, unless the
      // log was rotated in between, which brings back the first problem.
      uint64_t wal_version_now;
      const SharedState::WalState wal_now = mp_->ReadWal(&wal_version_now);
      if (wal_now.log_number != wal.log_number) {
        continue;
      }
      wal = wal_now;
      wal_version = wal_version_now;

      std::set<uint64_t> live_before;
      versions_->AddLiveFiles(&live_before);
      s = versions_->TailManifest(manifest.manifest_number,
                                  manifest.manifest_offset, manifest.epoch);
      if (s.IsNotFound()) {
        continue;  // The compactor has moved on to a new manifest.
      } else if (!s.ok()) {
        return s;
      }
      // Do not hold on to tables that no version of this process refers to
      // any more: the compactor deletes them, and an open file would keep
      // their disk space allocated.
      std::set<uint64_t> live_after;
      versions_->AddLiveFiles(&live_after);
      for (uint64_t number : live_before) {
        if (live_after.find(number) == live_after.end()) {
          table_cache_->Evict(number);
        }
      }
    }

    // Logs older than this have been flushed to tables that the version
    // just installed refers to.  Only now drop the memtables that mirror
    // them; the other order would leave a moment with the data in neither.
    const uint64_t min_log = versions_->LogNumber();
    if (wal.log_number != 0 && min_log > wal.log_number) {
      continue;  // "wal" is older than the manifest state; read it again.
    }
    if (imm_ != nullptr && imm_log_number_ < min_log) {
      imm_->Unref();
      imm_ = nullptr;
      has_imm_.store(false, std::memory_order_release);
    }
    if (logfile_number_ < min_log) {
      if (mem_nonempty_) {
        MpNewMemTable();
      }
      logfile_number_ = min_log;
      mem_log_offset_ = 0;
    }

    bool retry = false;
    if (wal.log_number != 0 && logfile_number_ != wal.log_number) {
      // The log was rotated.  Everything before wal.log_number is complete;
      // finish reading it and make the result the immutable memtable.
      // (Normally this is one log.  It can be more when the database was
      // last used by a process that died before flushing.)
      std::vector<std::string> filenames;
      s = env_->GetChildren(dbname_, &filenames);
      if (!s.ok()) {
        return s;
      }
      std::vector<uint64_t> logs;
      for (const std::string& filename : filenames) {
        uint64_t number;
        FileType type;
        if (ParseFileName(filename, &number, &type) && type == kLogFile &&
            number >= logfile_number_ && number < wal.log_number) {
          logs.push_back(number);
        }
      }
      std::sort(logs.begin(), logs.end());
      for (uint64_t number : logs) {
        const uint64_t from = (number == logfile_number_) ? mem_log_offset_ : 0;
        s = MpTailLog(number, from, LogTailer::kNoLimit, true /*final*/);
        if (s.IsNotFound()) {
          retry = true;  // Flushed and deleted in the meantime.
          break;
        } else if (!s.ok()) {
          return s;
        }
        // Keep the position exact in case of a retry.
        logfile_number_ = number;
        mem_log_offset_ = wal_tailer_->Position();
      }
      if (!retry) {
        if (mem_nonempty_) {
          if (imm_ != nullptr) {
            return Status::Corruption(
                dbname_, "multi-process: log rotated before previous flush");
          }
          imm_ = mem_;
          imm_log_number_ = wal.log_number - 1;
          has_imm_.store(true, std::memory_order_release);
          mem_ = new MemTable(internal_comparator_);
          mem_->Ref();
          mem_nonempty_ = false;
        }
        logfile_number_ = wal.log_number;
        mem_log_offset_ = 0;
      }
    }

    if (!retry && wal.log_number != 0 && wal.log_offset > mem_log_offset_) {
      s = MpTailLog(wal.log_number, mem_log_offset_, wal.log_offset,
                    false /*final*/);
      if (s.IsNotFound()) {
        retry = true;
      } else if (!s.ok()) {
        return s;
      }
    }
    if (retry) {
      continue;
    }
    if (follow_manifest && mp_->ManifestVersion() != manifest_version) {
      // An edit was published while the logs were being read.  If it was
      // a flush, the log it covers may have been deleted before the
      // directory was listed above, in which case its contents are now
      // neither in mem_/imm_ nor in the version installed above.  Applying
      // the edit fixes that.  (A log is only deleted after the edit that
      // makes it obsolete is published, so if nothing was published
      // nothing that was needed can have been missed.)
      continue;
    }

    if (versions_->LastSequence() < wal.last_sequence) {
      versions_->SetLastSequence(wal.last_sequence);
    }
    wal_version_seen_ = wal_version;
    if (follow_manifest) {
      manifest_version_seen_ = manifest_version;
    }
    return Status::OK();
  }
  return Status::IOError(dbname_, "multi-process catch-up did not converge");
}

Status DBImpl::MpTailLog(uint64_t number, uint64_t from, uint64_t limit,
                         bool final) {
  mutex_.AssertHeld();
  if (wal_tailer_ == nullptr || wal_tailer_number_ != number ||
      wal_tailer_->Position() != from) {
    // (The position differs after this process appended to the log itself:
    // those records went into mem_ directly.)
    delete wal_tailer_;
    wal_tailer_ = new LogTailer(env_, LogFileName(dbname_, number));
    wal_tailer_number_ = number;
    Status s = wal_tailer_->Open(from);
    if (!s.ok()) {
      delete wal_tailer_;
      wal_tailer_ = nullptr;
      return s;
    }
  }
  wal_tailer_->SetLimit(limit);

  Slice record;
  std::string scratch;
  WriteBatch batch;
  Status status;
  while (wal_tailer_->ReadRecord(&record, &scratch)) {
    if (record.size() < 12) {
      status = Status::Corruption("log record too small");
      MaybeIgnoreError(&status);
      if (!status.ok()) return status;
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);
    status = WriteBatchInternal::InsertInto(&batch, mem_);
    MaybeIgnoreError(&status);
    if (!status.ok()) return status;
    mem_nonempty_ = true;
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > versions_->LastSequence()) {
      versions_->SetLastSequence(last_seq);
    }
  }

  status = wal_tailer_->status();
  if (status.ok() && !final && wal_tailer_->Position() != limit) {
    status = Status::Corruption(LogFileName(dbname_, number),
                                "log ends before its committed offset");
  }
  if (!status.ok()) {
    Log(options_.info_log, "%sLog #%llu: %s",
        options_.paranoid_checks ? "" : "(ignoring error) ",
        static_cast<unsigned long long>(number), status.ToString().c_str());
    MaybeIgnoreError(&status);
    if (!status.ok()) return status;
  }
  if (!final) {
    // If an error was ignored the tailer is not at "limit"; the next call
    // then starts a new one there.
    mem_log_offset_ = limit;
  }
  return Status::OK();
}

Status DBImpl::MpBeginRead() {
  mutex_.AssertHeld();
  // A read uses the table files of versions_->current() as it is after
  // CatchUp().  The compactor may delete a table file once (a) the edit
  // that removed the file from the current version is published and (b) it
  // then finds no live process pinning an epoch older than that edit.
  //
  // So: publish a pin that is not newer than the version about to be used
  // *before* looking for newly published edits.  Then either the compactor
  // sees the pin in (b), or its edit was published before the pin and the
  // CatchUp() below applies it, in which case the version used here does
  // not contain the deleted file.
  const uint64_t epoch = versions_->current()->epoch();
  if (epoch < published_pin_) {
    mp_->SetPinnedEpoch(epoch);
    published_pin_ = epoch;
  }
  return CatchUp();
}

void DBImpl::MpEndRead() {
  mutex_.AssertHeld();
  // Raising the pin to what is really in use is always safe: every new use
  // goes through MpBeginRead(), under mutex_, first.
  const uint64_t pin = versions_->OldestPinnedEpoch();
  if (pin != published_pin_) {
    mp_->SetPinnedEpoch(pin);
    published_pin_ = pin;
  }
}

void DBImpl::MpIteratorDone() {
  MutexLock l(&mutex_);
  MpEndRead();
}

// REQUIRES: this thread is at the front of the writer queue and holds the
// cross-process write lock.
Status DBImpl::MpPrepareWrite() {
  mutex_.AssertHeld();
  // With the write lock held nobody else appends, so after this mem_
  // mirrors the log exactly up to its committed end.
  Status s = CatchUp();
  if (!s.ok()) {
    return s;
  }
  SharedState::WalState wal = mp_->ReadWal(nullptr);

  if (mp_->AppendInProgress()) {
    // The previous writer died or failed between starting an append and
    // publishing it.  Cut off whatever it left behind the committed end.
    delete log_;
    log_ = nullptr;
    delete logfile_;
    logfile_ = nullptr;
    if (wal.log_number != 0) {
      s = SharedState::Truncate(LogFileName(dbname_, wal.log_number),
                                wal.log_offset);
      if (!s.ok()) {
        return s;
      }
      Log(options_.info_log, "Log #%llu: removed uncommitted tail at %llu",
          static_cast<unsigned long long>(wal.log_number),
          static_cast<unsigned long long>(wal.log_offset));
    }
    mp_->SetAppendInProgress(false);
  }

  if (wal.log_number == 0) {
    // No log yet (new database, or one that was opened read-only first).
    const uint64_t new_log_number = versions_->NewFileNumber();
    WritableFile* lfile = nullptr;
    s = env_->NewAppendableFile(LogFileName(dbname_, new_log_number), &lfile);
    if (!s.ok()) {
      return s;
    }
    delete log_;
    delete logfile_;
    logfile_ = lfile;
    log_ = new log::Writer(lfile);
    log_handle_number_ = new_log_number;
    logfile_number_ = new_log_number;
    mem_log_offset_ = 0;
    wal.log_number = new_log_number;
    wal.log_offset = 0;
    wal.last_sequence = versions_->LastSequence();
    wal_version_seen_ = mp_->PublishWal(wal);
  }
  return Status::OK();
}

// REQUIRES: this thread is at the front of the writer queue and holds the
// cross-process write lock.
Status DBImpl::MpMakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  bool logged = false;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // As in MakeRoomForWrite().  This holds up the writers of all
      // processes, which is the point.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (force && !mem_nonempty_) {
      // Nothing to flush
      break;
    } else if (imm_ != nullptr ||
               versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // Wait for the compactor, which may be another process, to flush the
      // previous memtable or to reduce the number of level-0 files.  The
      // result arrives as a MANIFEST edit.  If there is no compactor (any
      // more), become it.
      if (!logged) {
        Log(options_.info_log, "%s; waiting...\n",
            imm_ != nullptr ? "Current memtable full" : "Too many L0 files");
        logged = true;
      }
      if (MpTryBecomeCompactor()) {
        MaybeScheduleCompaction();
      }
      mutex_.Unlock();
      env_->SleepForMicroseconds(kStallMicros);
      mutex_.Lock();
      if (shutting_down_.load(std::memory_order_acquire)) {
        s = Status::IOError("Deleting DB during write");
        break;
      }
      s = CatchUp();
      if (!s.ok()) {
        break;
      }
    } else {
      // Switch to a new log.  Publishing its number tells every process to
      // make its memtable immutable; the compactor then flushes its copy.
      const uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewAppendableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      log_ = new log::Writer(lfile);
      log_handle_number_ = new_log_number;

      imm_ = mem_;
      imm_log_number_ = new_log_number - 1;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      mem_nonempty_ = false;
      logfile_number_ = new_log_number;
      mem_log_offset_ = 0;

      SharedState::WalState wal;
      wal.log_number = new_log_number;
      wal.log_offset = 0;
      wal.last_sequence = versions_->LastSequence();
      wal_version_seen_ = mp_->PublishWal(wal);

      force = false;  // Do not force another compaction if have room
      if (MpTryBecomeCompactor()) {
        MaybeScheduleCompaction();
      }
    }
  }
  return s;
}

// Makes log_ append to log file "log_number", whose committed length is
// "log_offset".
// REQUIRES: this thread is at the front of the writer queue and holds the
// cross-process write lock.  mutex_ is not needed.
Status DBImpl::MpOpenLogForAppend(uint64_t log_number, uint64_t log_offset) {
  if (logfile_ == nullptr || log_handle_number_ != log_number) {
    delete log_;
    log_ = nullptr;
    delete logfile_;
    logfile_ = nullptr;
    // O_APPEND, so the data lands at the end of the file no matter which
    // process wrote last.
    Status s =
        env_->NewAppendableFile(LogFileName(dbname_, log_number), &logfile_);
    if (!s.ok()) {
      logfile_ = nullptr;
      return s;
    }
    log_handle_number_ = log_number;
  }
  if (log_ == nullptr || log_->FileOffset() != log_offset) {
    // Another process appended since this one last did; realign with the
    // block structure of the file.
    delete log_;
    log_ = new log::Writer(logfile_, log_offset);
  }
  return Status::OK();
}

bool DBImpl::MpTryBecomeCompactor() {
  mutex_.AssertHeld();
  if (is_compactor_) {
    return true;
  }
  if (options_.read_only || !bg_error_.ok() || !mp_->TryLockCompact()) {
    return false;
  }
  // The previous compactor, if any, is gone, so the published manifest
  // state is final until this process changes it.
  Status s = CatchUp();
  if (!s.ok()) {
    Log(options_.info_log, "Cannot become compactor: %s", s.ToString().c_str());
    mp_->UnlockCompact();
    return false;
  }
  versions_->BecomeManifestWriter();
  is_compactor_ = true;
  // Other processes may be reading files of versions that this process
  // never had.  Until all of them have moved past the current epoch it is
  // unknown which table files are needed.
  safe_epoch_ = versions_->current()->epoch();
  MpRetainCurrentVersion();
  Log(options_.info_log, "This process is now the compactor (epoch %llu)",
      static_cast<unsigned long long>(safe_epoch_));
  return true;
}

void DBImpl::MpRetainCurrentVersion() {
  mutex_.AssertHeld();
  if (mp_ == nullptr || !is_compactor_) {
    return;
  }
  Version* current = versions_->current();
  if (retained_versions_.empty() || retained_versions_.back() != current) {
    current->Ref();
    retained_versions_.push_back(current);
  }
}

bool DBImpl::MpReleaseRetainedVersions() {
  mutex_.AssertHeld();
  assert(is_compactor_);
  // A process that pins epoch E reads the files of the version that was
  // current at E, which is in retained_versions_ if E >= safe_epoch_.
  const uint64_t min_pin = mp_->MinPinnedEpoch();
  while (retained_versions_.size() > 1 &&
         retained_versions_.front()->epoch() < min_pin) {
    retained_versions_.front()->Unref();
    retained_versions_.pop_front();
  }
  return min_pin >= safe_epoch_;
}

void DBImpl::LocalCompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    CatchUp();  // Ignoring errors; the background thread will hit them too.
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::MpCompactRange(const Slice* begin, const Slice* end) {
  // Rotate the log so that everything written so far gets flushed ...
  Status s = Write(WriteOptions(), nullptr);
  if (!s.ok()) {
    return;
  }
  // ... and wait for the flush.
  while (true) {
    {
      MutexLock l(&mutex_);
      if (!CatchUp().ok() || !bg_error_.ok() || imm_ == nullptr ||
          shutting_down_.load(std::memory_order_acquire)) {
        break;
      }
      if (MpTryBecomeCompactor()) {
        MaybeScheduleCompaction();
      }
    }
    env_->SleepForMicroseconds(kStallMicros);
  }

  // Compact the levels here if this process is (or can become) the
  // compactor.  Otherwise ask the compactor, which then compacts the whole
  // key range.
  uint64_t ticket = 0;
  while (!shutting_down_.load(std::memory_order_acquire)) {
    bool local;
    {
      MutexLock l(&mutex_);
      if (!bg_error_.ok()) {
        break;
      }
      local = MpTryBecomeCompactor();
    }
    if (local) {
      LocalCompactRange(begin, end);
      break;
    }
    if (ticket == 0) {
      ticket = mp_->RequestManual();
    } else if (mp_->ManualDone() >= ticket) {
      break;
    }
    env_->SleepForMicroseconds(kPollMicros);
  }
}

void DBImpl::MpPollLoop() {
  while (!shutting_down_.load(std::memory_order_acquire)) {
    env_->SleepForMicroseconds(kPollMicros);
    uint64_t manual_ticket = 0;
    {
      MutexLock l(&mutex_);
      if (shutting_down_.load(std::memory_order_acquire) || !bg_error_.ok()) {
        continue;
      }
      Status s = CatchUp();
      if (!s.ok()) {
        continue;
      }
      const uint64_t requested = mp_->ManualRequested();
      const bool manual_pending = requested > mp_->ManualDone();
      if (imm_ != nullptr || manual_pending || versions_->NeedsCompaction()) {
        if (MpTryBecomeCompactor()) {
          MaybeScheduleCompaction();
          if (manual_pending) {
            manual_ticket = requested;
          }
        }
      }
    }
    if (manual_ticket != 0) {
      // Requested by a process that is not the compactor.
      LocalCompactRange(nullptr, nullptr);
      mp_->SetManualDone(manual_ticket);
    }
  }
}

}  // namespace leveldb
