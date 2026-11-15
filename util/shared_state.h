// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// SharedState is the cross-process coordination block used when a database
// is opened with Options::multi_process.  It consists of
//
//   STATE         a small file mapped MAP_SHARED into every process
//   WRITE.lock    flock()ed by the process that is appending to the WAL
//   COMPACT.lock  flock()ed by the one process that flushes, compacts,
//                 writes the MANIFEST and deletes files
//   LOCK          the classic lock file; multi-process openers hold a shared
//                 lock on it so that a classic (exclusive) opener is refused
//
// Nothing in STATE is needed after every process has exited: the first
// process to open the database rebuilds it from CURRENT, the MANIFEST and
// the log files.  STATE therefore never needs to be synced.
//
// Two groups of fields are published as units:
//
//   WalState       {log number, committed log offset, last sequence}
//                  written only while holding WRITE.lock
//   ManifestState  {manifest number, committed manifest offset, epoch}
//                  written only while holding COMPACT.lock
//
// Each group is kept in two copies plus a version counter.  The (single)
// writer fills the inactive copy and then bumps the counter, so the update is
// atomic for readers and also for a writer that is SIGKILLed half way.
//
// Each process also owns a "slot" in which it publishes
//
//   pinned epoch     the oldest Version (identified by its epoch) that the
//                    process may still read table files from
//   oldest snapshot  the smallest sequence number of its live snapshots
//
// The compactor consults the slots of all *live* processes before dropping
// shadowed entries and before deleting table files.  A slot is live while its
// owner holds an open-file-description lock on the slot's byte of STATE; the
// kernel drops that lock when the owner dies.

#ifndef STORAGE_LEVELDB_UTIL_SHARED_STATE_H_
#define STORAGE_LEVELDB_UTIL_SHARED_STATE_H_

#include <cstdint>
#include <string>

#include "leveldb/status.h"

namespace leveldb {

class SharedState {
 public:
  // "Nothing pinned" / "no snapshot".
  static constexpr uint64_t kNone = ~static_cast<uint64_t>(0);

  struct WalState {
    uint64_t log_number = 0;     // 0: no log file yet
    uint64_t log_offset = 0;     // Bytes of log_number that are committed
    uint64_t last_sequence = 0;  // Last sequence number that is committed
  };

  struct ManifestState {
    uint64_t manifest_number = 0;
    uint64_t manifest_offset = 0;  // Bytes of the manifest that are committed
    uint64_t epoch = 0;            // Incremented for every committed edit
  };

  // Open (creating if needed) the shared state of the database "dbname".
  //
  // On success the caller holds the "open mutex", which serializes opens
  // across processes, until it calls FinishOpen() or deletes the object.
  // *first is set to true if no other live process has the database open;
  // the caller must then rebuild the state (see Reset()/MarkValid()).
  static Status Open(const std::string& dbname, SharedState** result,
                     bool* first);

  SharedState(const SharedState&) = delete;
  SharedState& operator=(const SharedState&) = delete;

  ~SharedState();

  // --- Open protocol (all REQUIRE the open mutex) ---

  // Zero the whole state and mark it invalid.  Only for the first opener.
  void Reset();
  // Declare the state rebuilt.  Only for the first opener.
  void MarkValid();
  // Take a slot.  The slot starts out pinning everything.
  Status ClaimSlot();
  // Release the open mutex.
  void FinishOpen();

  // --- File numbers (lock free) ---

  uint64_t NewFileNumber();
  // Make sure "number" is never returned by NewFileNumber().
  void MarkFileNumberUsed(uint64_t number);
  // The number the next NewFileNumber() call would return.
  uint64_t NextFileNumber() const;

  // --- WAL state ---

  void LockWrite();
  void UnlockWrite();
  WalState ReadWal(uint64_t* version) const;
  uint64_t WalVersion() const;
  // REQUIRES: WRITE.lock (or the open mutex as first opener).
  // Returns the new version.
  uint64_t PublishWal(const WalState& state);
  // Set before appending to the WAL and cleared once the append has been
  // published.  If it is found set by the next holder of WRITE.lock, the
  // previous holder died (or failed) mid-append and the log file may have
  // an uncommitted tail.
  void SetAppendInProgress(bool value);
  bool AppendInProgress() const;

  // --- Manifest state ---

  // Returns true iff this process now is (or already was) the compactor.
  bool TryLockCompact();
  void UnlockCompact();
  ManifestState ReadManifest(uint64_t* version) const;
  uint64_t ManifestVersion() const;
  // REQUIRES: COMPACT.lock (or the open mutex as first opener).
  // Returns the new version.
  uint64_t PublishManifest(const ManifestState& state);

  // --- Per-process slots ---

  void SetPinnedEpoch(uint64_t epoch);
  void SetOldestSnapshot(uint64_t sequence);
  // Minimum over all other live processes; kNone if there is none.
  uint64_t MinPinnedEpoch() const;
  uint64_t MinOldestSnapshot() const;
  // Number of live processes including this one.
  int LiveProcesses() const;

  // --- Manual compaction requests ---

  // Returns a ticket.  The request is served once ManualDone() >= ticket.
  uint64_t RequestManual();
  uint64_t ManualRequested() const;
  uint64_t ManualDone() const;
  void SetManualDone(uint64_t ticket);

  // Shorten "fname" to "size" bytes.
  static Status Truncate(const std::string& fname, uint64_t size);

 private:
  struct Layout;

  SharedState() = default;

  bool SlotLockedByOther(int slot) const;

  std::string dbname_;
  Layout* layout_ = nullptr;
  int lock_fd_ = -1;     // LOCK
  int state_fd_ = -1;    // STATE
  int write_fd_ = -1;    // WRITE.lock
  int compact_fd_ = -1;  // COMPACT.lock
  int slot_ = -1;
  bool open_mutex_held_ = false;
  bool compact_locked_ = false;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_SHARED_STATE_H_
