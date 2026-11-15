// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include <atomic>
#include <deque>
#include <set>
#include <string>
#include <thread>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class LogTailer;
class MemTable;
class SharedState;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

  // Multi-process only: called after an iterator dropped its references.
  void MpIteratorDone();

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // ---- Multi-process support.  See db_impl_mp.cc for the design. ----

  // Open() for Options::multi_process.
  Status MpOpen();
  // As the first process to open the database, rebuild the shared state
  // from the files in the database directory.
  Status MpInitStateFromDisk() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Apply the MANIFEST edits and WAL records that other processes have
  // committed since the last call.  Cheap if there are none.
  Status CatchUp() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Replay log file "number" from offset "from" into mem_.  If "final" the
  // file is complete and is read to its end, otherwise up to "limit".
  Status MpTailLog(uint64_t number, uint64_t from, uint64_t limit, bool final)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void MpNewMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Publish what this process is about to read, then catch up.
  Status MpBeginRead() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Publish what this process still reads after a read is over.
  void MpEndRead() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Write path.  All REQUIRE the cross-process write lock.
  Status MpPrepareWrite() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status MpMakeRoomForWrite(bool force) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status MpOpenLogForAppend(uint64_t log_number, uint64_t log_offset);

  // Compactor role.
  bool MpTryBecomeCompactor() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void MpRetainCurrentVersion() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Returns false if table files must not be deleted right now.
  bool MpReleaseRetainedVersions() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void MpCompactRange(const Slice* begin, const Slice* end);
  void LocalCompactRange(const Slice* begin, const Slice* end);
  void MpPollLoop();

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

  // ---- Multi-process state.  mp_ is null unless options_.multi_process.
  //
  // In multi-process mode:
  //   logfile_number_  is the log that mem_ mirrors (0: none yet)
  //   mem_log_offset_  is how much of that log has been applied to mem_
  //   imm_             mirrors all older logs that are not flushed yet
  //   logfile_, log_   are this process's handle for appending to log
  //                    number log_handle_number_ (opened lazily)
  SharedState* mp_;
  bool is_compactor_ GUARDED_BY(mutex_);
  uint64_t mem_log_offset_ GUARDED_BY(mutex_);
  bool mem_nonempty_ GUARDED_BY(mutex_);
  uint64_t imm_log_number_ GUARDED_BY(mutex_);  // imm_ covers logs <= this
  uint64_t log_handle_number_;
  uint64_t wal_version_seen_ GUARDED_BY(mutex_);
  uint64_t manifest_version_seen_ GUARDED_BY(mutex_);
  LogTailer* wal_tailer_ GUARDED_BY(mutex_);
  uint64_t wal_tailer_number_ GUARDED_BY(mutex_);
  uint64_t published_pin_ GUARDED_BY(mutex_);
  // Compactor only: versions kept alive because another process may still
  // be reading their files, oldest first.
  std::deque<Version*> retained_versions_ GUARDED_BY(mutex_);
  // Compactor only: no table file may be deleted while any process pins an
  // epoch older than this (the epoch at which this process took over).
  uint64_t safe_epoch_ GUARDED_BY(mutex_);
  std::thread poll_thread_;
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
