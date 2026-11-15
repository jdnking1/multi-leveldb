// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/shared_state.h"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cerrno>
#include <cstring>

namespace leveldb {

#if !defined(_WIN32) && defined(F_OFD_SETLK)

namespace {

constexpr uint64_t kMagic = 0x6c64624d50763031ull;  // "ldbMPv01"
constexpr int kMaxSlots = 128;
constexpr size_t kMapSize = 8192;
// Slot locks live on bytes of STATE far beyond its data.
constexpr off_t kSlotLockBase = 1 << 20;

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  }
  return Status::IOError(context, std::strerror(error_number));
}

int OpenFile(const std::string& fname) {
  int fd;
  do {
    fd = ::open(fname.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  } while (fd < 0 && errno == EINTR);
  return fd;
}

int Flock(int fd, int operation) {
  int r;
  do {
    r = ::flock(fd, operation);
  } while (r != 0 && errno == EINTR);
  return r;
}

// Open-file-description lock on [start, start+len) of fd (len 0: to EOF).
int OfdLock(int fd, short type, off_t start, off_t len) {
  struct ::flock fl;
  std::memset(&fl, 0, sizeof(fl));
  fl.l_type = type;
  fl.l_whence = SEEK_SET;
  fl.l_start = start;
  fl.l_len = len;
  int r;
  do {
    r = ::fcntl(fd, F_OFD_SETLK, &fl);
  } while (r != 0 && errno == EINTR);
  return r;
}

// A group of three values that is replaced atomically by a single writer.
struct Triple {
  std::atomic<uint64_t> version;
  std::atomic<uint64_t> copy[2][3];

  void Read(uint64_t out[3], uint64_t* version_out) const {
    while (true) {
      const uint64_t v = version.load();
      const int which = static_cast<int>(v & 1);
      for (int i = 0; i < 3; i++) {
        out[i] = copy[which][i].load(std::memory_order_relaxed);
      }
      if (version.load() == v) {
        if (version_out != nullptr) *version_out = v;
        return;
      }
    }
  }

  uint64_t Write(const uint64_t in[3]) {
    const uint64_t v = version.load();
    const int which = static_cast<int>((v + 1) & 1);
    for (int i = 0; i < 3; i++) {
      copy[which][i].store(in[i], std::memory_order_relaxed);
    }
    version.store(v + 1);
    return v + 1;
  }
};

struct Slot {
  std::atomic<uint64_t> pid;  // 0: never used or cleanly released
  std::atomic<uint64_t> pinned_epoch;
  std::atomic<uint64_t> oldest_snapshot;
  uint64_t padding;
};

}  // namespace

struct SharedState::Layout {
  std::atomic<uint64_t> magic;
  std::atomic<uint64_t> next_file_number;
  Triple wal;
  Triple manifest;
  std::atomic<uint64_t> append_in_progress;
  std::atomic<uint64_t> manual_requested;
  std::atomic<uint64_t> manual_done;
  Slot slots[kMaxSlots];
};

static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t),
              "atomics must be plain words to live in a shared mapping");

Status SharedState::Open(const std::string& dbname, SharedState** result,
                         bool* first) {
  static_assert(sizeof(Layout) <= kMapSize, "STATE layout too large");
  *result = nullptr;
  *first = false;
  SharedState* s = new SharedState;
  s->dbname_ = dbname;

  // Shared lock on LOCK: compatible with other multi-process openers, in
  // conflict with the exclusive fcntl() lock of a classic opener.
  const std::string lock_name = dbname + "/LOCK";
  s->lock_fd_ = OpenFile(lock_name);
  if (s->lock_fd_ < 0) {
    Status st = PosixError(lock_name, errno);
    delete s;
    return st;
  }
  if (OfdLock(s->lock_fd_, F_RDLCK, 0, 0) != 0) {
    Status st = Status::IOError(
        "lock " + lock_name,
        "held by a process that did not open with multi_process");
    delete s;
    return st;
  }

  const std::string state_name = dbname + "/STATE";
  s->state_fd_ = OpenFile(state_name);
  s->write_fd_ = OpenFile(dbname + "/WRITE.lock");
  s->compact_fd_ = OpenFile(dbname + "/COMPACT.lock");
  if (s->state_fd_ < 0 || s->write_fd_ < 0 || s->compact_fd_ < 0) {
    Status st = PosixError(dbname + "/STATE", errno);
    delete s;
    return st;
  }

  // The open mutex.
  if (Flock(s->state_fd_, LOCK_EX) != 0) {
    Status st = PosixError("flock " + state_name, errno);
    delete s;
    return st;
  }
  s->open_mutex_held_ = true;

  struct ::stat sb;
  if (::fstat(s->state_fd_, &sb) != 0 ||
      (static_cast<size_t>(sb.st_size) < kMapSize &&
       ::ftruncate(s->state_fd_, kMapSize) != 0)) {
    Status st = PosixError(state_name, errno);
    delete s;
    return st;
  }
  void* base = ::mmap(nullptr, kMapSize, PROT_READ | PROT_WRITE, MAP_SHARED,
                      s->state_fd_, 0);
  if (base == MAP_FAILED) {
    Status st = PosixError("mmap " + state_name, errno);
    delete s;
    return st;
  }
  s->layout_ = static_cast<Layout*>(base);

  bool any_live = false;
  if (s->layout_->magic.load() == kMagic) {
    for (int i = 0; i < kMaxSlots && !any_live; i++) {
      any_live =
          s->layout_->slots[i].pid.load() != 0 && s->SlotLockedByOther(i);
    }
  }
  *first = !any_live;
  *result = s;
  return Status::OK();
}

SharedState::~SharedState() {
  if (layout_ != nullptr) {
    if (slot_ >= 0) {
      layout_->slots[slot_].pid.store(0);
    }
    ::munmap(layout_, kMapSize);
  }
  // Closing the descriptors releases every lock they hold.
  if (compact_fd_ >= 0) ::close(compact_fd_);
  if (write_fd_ >= 0) ::close(write_fd_);
  if (state_fd_ >= 0) ::close(state_fd_);
  if (lock_fd_ >= 0) ::close(lock_fd_);
}

void SharedState::Reset() {
  layout_->magic.store(0);
  std::memset(static_cast<void*>(layout_), 0, kMapSize);
  layout_->next_file_number.store(2);
}

void SharedState::MarkValid() { layout_->magic.store(kMagic); }

bool SharedState::SlotLockedByOther(int slot) const {
  struct ::flock fl;
  std::memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = kSlotLockBase + slot;
  fl.l_len = 1;
  if (::fcntl(state_fd_, F_OFD_GETLK, &fl) != 0) {
    return true;  // Be conservative: treat the slot as live.
  }
  return fl.l_type != F_UNLCK;
}

Status SharedState::ClaimSlot() {
  for (int i = 0; i < kMaxSlots; i++) {
    if (OfdLock(state_fd_, F_WRLCK, kSlotLockBase + i, 1) == 0) {
      Slot& slot = layout_->slots[i];
      // Pin everything until the caller knows what it needs.
      slot.pinned_epoch.store(0);
      slot.oldest_snapshot.store(0);
      slot.pid.store(static_cast<uint64_t>(::getpid()));
      slot_ = i;
      return Status::OK();
    }
  }
  return Status::IOError(dbname_, "too many processes have the database open");
}

void SharedState::FinishOpen() {
  if (open_mutex_held_) {
    Flock(state_fd_, LOCK_UN);
    open_mutex_held_ = false;
  }
}

uint64_t SharedState::NewFileNumber() {
  return layout_->next_file_number.fetch_add(1);
}

void SharedState::MarkFileNumberUsed(uint64_t number) {
  uint64_t current = layout_->next_file_number.load();
  while (current <= number && !layout_->next_file_number.compare_exchange_weak(
                                  current, number + 1)) {
  }
}

uint64_t SharedState::NextFileNumber() const {
  return layout_->next_file_number.load();
}

void SharedState::LockWrite() { Flock(write_fd_, LOCK_EX); }

void SharedState::UnlockWrite() { Flock(write_fd_, LOCK_UN); }

SharedState::WalState SharedState::ReadWal(uint64_t* version) const {
  uint64_t v[3];
  layout_->wal.Read(v, version);
  WalState state;
  state.log_number = v[0];
  state.log_offset = v[1];
  state.last_sequence = v[2];
  return state;
}

uint64_t SharedState::WalVersion() const { return layout_->wal.version.load(); }

uint64_t SharedState::PublishWal(const WalState& state) {
  const uint64_t v[3] = {state.log_number, state.log_offset,
                         state.last_sequence};
  return layout_->wal.Write(v);
}

void SharedState::SetAppendInProgress(bool value) {
  layout_->append_in_progress.store(value ? 1 : 0);
}

bool SharedState::AppendInProgress() const {
  return layout_->append_in_progress.load() != 0;
}

bool SharedState::TryLockCompact() {
  if (!compact_locked_) {
    compact_locked_ = (Flock(compact_fd_, LOCK_EX | LOCK_NB) == 0);
  }
  return compact_locked_;
}

void SharedState::UnlockCompact() {
  if (compact_locked_) {
    Flock(compact_fd_, LOCK_UN);
    compact_locked_ = false;
  }
}

SharedState::ManifestState SharedState::ReadManifest(uint64_t* version) const {
  uint64_t v[3];
  layout_->manifest.Read(v, version);
  ManifestState state;
  state.manifest_number = v[0];
  state.manifest_offset = v[1];
  state.epoch = v[2];
  return state;
}

uint64_t SharedState::ManifestVersion() const {
  return layout_->manifest.version.load();
}

uint64_t SharedState::PublishManifest(const ManifestState& state) {
  const uint64_t v[3] = {state.manifest_number, state.manifest_offset,
                         state.epoch};
  return layout_->manifest.Write(v);
}

void SharedState::SetPinnedEpoch(uint64_t epoch) {
  layout_->slots[slot_].pinned_epoch.store(epoch);
}

void SharedState::SetOldestSnapshot(uint64_t sequence) {
  layout_->slots[slot_].oldest_snapshot.store(sequence);
}

uint64_t SharedState::MinPinnedEpoch() const {
  uint64_t result = kNone;
  for (int i = 0; i < kMaxSlots; i++) {
    if (i == slot_) continue;
    const Slot& slot = layout_->slots[i];
    if (slot.pid.load() != 0 && SlotLockedByOther(i)) {
      const uint64_t v = slot.pinned_epoch.load();
      if (v < result) result = v;
    }
  }
  return result;
}

uint64_t SharedState::MinOldestSnapshot() const {
  uint64_t result = kNone;
  for (int i = 0; i < kMaxSlots; i++) {
    if (i == slot_) continue;
    const Slot& slot = layout_->slots[i];
    if (slot.pid.load() != 0 && SlotLockedByOther(i)) {
      const uint64_t v = slot.oldest_snapshot.load();
      if (v < result) result = v;
    }
  }
  return result;
}

int SharedState::LiveProcesses() const {
  int n = 0;
  for (int i = 0; i < kMaxSlots; i++) {
    if (i == slot_) {
      n++;
    } else if (layout_->slots[i].pid.load() != 0 && SlotLockedByOther(i)) {
      n++;
    }
  }
  return n;
}

uint64_t SharedState::RequestManual() {
  return layout_->manual_requested.fetch_add(1) + 1;
}

uint64_t SharedState::ManualRequested() const {
  return layout_->manual_requested.load();
}

uint64_t SharedState::ManualDone() const { return layout_->manual_done.load(); }

void SharedState::SetManualDone(uint64_t ticket) {
  uint64_t current = layout_->manual_done.load();
  while (current < ticket &&
         !layout_->manual_done.compare_exchange_weak(current, ticket)) {
  }
}

Status SharedState::Truncate(const std::string& fname, uint64_t size) {
  if (::truncate(fname.c_str(), static_cast<off_t>(size)) != 0) {
    return PosixError(fname, errno);
  }
  return Status::OK();
}

#else  // No open-file-description locks on this platform.

struct SharedState::Layout {};

Status SharedState::Open(const std::string& dbname, SharedState** result,
                         bool* first) {
  *result = nullptr;
  *first = false;
  return Status::NotSupported(
      dbname, "Options::multi_process is not supported on this platform");
}

SharedState::~SharedState() = default;
void SharedState::Reset() {}
void SharedState::MarkValid() {}
Status SharedState::ClaimSlot() {
  return Status::NotSupported("multi_process");
}
void SharedState::FinishOpen() {}
uint64_t SharedState::NewFileNumber() { return 0; }
void SharedState::MarkFileNumberUsed(uint64_t) {}
uint64_t SharedState::NextFileNumber() const { return 0; }
void SharedState::LockWrite() {}
void SharedState::UnlockWrite() {}
SharedState::WalState SharedState::ReadWal(uint64_t*) const { return {}; }
uint64_t SharedState::WalVersion() const { return 0; }
uint64_t SharedState::PublishWal(const WalState&) { return 0; }
void SharedState::SetAppendInProgress(bool) {}
bool SharedState::AppendInProgress() const { return false; }
bool SharedState::TryLockCompact() { return false; }
void SharedState::UnlockCompact() {}
SharedState::ManifestState SharedState::ReadManifest(uint64_t*) const {
  return {};
}
uint64_t SharedState::ManifestVersion() const { return 0; }
uint64_t SharedState::PublishManifest(const ManifestState&) { return 0; }
void SharedState::SetPinnedEpoch(uint64_t) {}
void SharedState::SetOldestSnapshot(uint64_t) {}
uint64_t SharedState::MinPinnedEpoch() const { return kNone; }
uint64_t SharedState::MinOldestSnapshot() const { return kNone; }
int SharedState::LiveProcesses() const { return 1; }
uint64_t SharedState::RequestManual() { return 0; }
uint64_t SharedState::ManualRequested() const { return 0; }
uint64_t SharedState::ManualDone() const { return 0; }
void SharedState::SetManualDone(uint64_t) {}
Status SharedState::Truncate(const std::string& fname, uint64_t) {
  return Status::NotSupported(fname);
}
bool SharedState::SlotLockedByOther(int) const { return false; }

#endif

}  // namespace leveldb
