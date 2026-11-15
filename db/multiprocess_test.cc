// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Tests for Options::read_only and Options::multi_process.
//
// Tests that need several processes start them by re-executing this binary
// with "--worker <role> <args...>" (see main()).  A fresh exec rather than a
// bare fork() gives every worker its own Env with its own background thread.

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <set>
#include <signal.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

#include "util/random.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

namespace {

// ---------------------------------------------------------------------
// Helpers shared by the tests and the workers.

std::string Key(int writer, int i) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "k%02d-%08d", writer, i);
  return buf;
}

std::string Value(int writer, int i) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "v%02d-%08d-", writer, i);
  return std::string(buf) + std::string(100, 'x');
}

std::string GroupKey(int i) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "g-%03d", i);
  return buf;
}

const int kGroupSize = 10;

Options MpOptions(size_t write_buffer_size) {
  Options options;
  options.create_if_missing = true;
  options.multi_process = true;
  options.write_buffer_size = write_buffer_size;
  return options;
}

bool FileExists(const std::string& fname) {
  return Env::Default()->FileExists(fname);
}

// In a worker a failed check ends the process with a message.
#define WORKER_CHECK(cond, ...)                                              \
  do {                                                                       \
    if (!(cond)) {                                                           \
      std::fprintf(stderr, "[worker %d] %s:%d: CHECK(%s): ",                 \
                   static_cast<int>(::getpid()), __FILE__, __LINE__, #cond); \
      std::fprintf(stderr, __VA_ARGS__);                                     \
      std::fprintf(stderr, "\n");                                            \
      ::_exit(1);                                                            \
    }                                                                        \
  } while (0)

#define WORKER_OK(status_expr)                       \
  do {                                               \
    Status worker_status_ = (status_expr);           \
    WORKER_CHECK(worker_status_.ok(), "%s",          \
                 worker_status_.ToString().c_str()); \
  } while (0)

// Checks that for "writer" the keys present are exactly 0..n-1 for some n,
// each with the right value, as seen by one iterator.  Returns n.
int CountPrefix(DB* db, int writer) {
  Iterator* it = db->NewIterator(ReadOptions());
  const std::string prefix = Key(writer, 0).substr(0, 4);  // "kNN-"
  int n = 0;
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
       it->Next()) {
    WORKER_CHECK(it->key() == Slice(Key(writer, n)),
                 "writer %d: expected key %s, found %s", writer,
                 Key(writer, n).c_str(), it->key().ToString().c_str());
    WORKER_CHECK(it->value() == Slice(Value(writer, n)), "bad value for %s",
                 Key(writer, n).c_str());
    n++;
  }
  WORKER_OK(it->status());
  delete it;
  return n;
}

// ---------------------------------------------------------------------
// Workers.

// reader_static <db> <writer> <count>
// Step 1: open read-only without multi_process and check the contents.
int ReaderStaticWorker(int argc, char** argv) {
  WORKER_CHECK(argc == 3, "usage");
  const std::string dbname = argv[0];
  const int writer = std::atoi(argv[1]);
  const int count = std::atoi(argv[2]);
  Options options;
  options.read_only = true;
  DB* db;
  WORKER_OK(DB::Open(options, dbname, &db));
  std::string value;
  for (int i = 0; i < count; i++) {
    WORKER_OK(db->Get(ReadOptions(), Key(writer, i), &value));
    WORKER_CHECK(value == Value(writer, i), "bad value for %d", i);
  }
  WORKER_CHECK(CountPrefix(db, writer) == count, "wrong number of keys");
  WORKER_CHECK(db->Put(WriteOptions(), "a", "b").IsNotSupportedError(),
               "Put must be rejected");
  // Give the sibling readers time to overlap with this one.
  Env::Default()->SleepForMicroseconds(200000);
  delete db;
  return 0;
}

// writer <db> <writer> <start> <count> <write_buffer_size> <ack_fd>
//        <compact_every> <groups>
// Puts keys start..count-1 of "writer", one Write() each.  After each one
// that was acknowledged its index is written to ack_fd (if >= 0).  Every
// compact_every keys (if > 0) calls CompactRange over everything.  If
// "groups" is non-zero every key is accompanied by an atomic update of the
// group keys.
int WriterWorker(int argc, char** argv) {
  WORKER_CHECK(argc == 8, "usage");
  const std::string dbname = argv[0];
  const int writer = std::atoi(argv[1]);
  const int start = std::atoi(argv[2]);
  const int count = std::atoi(argv[3]);
  const size_t write_buffer_size = std::strtoul(argv[4], nullptr, 10);
  const int ack_fd = std::atoi(argv[5]);
  const int compact_every = std::atoi(argv[6]);
  const bool groups = std::atoi(argv[7]) != 0;

  DB* db;
  WORKER_OK(DB::Open(MpOptions(write_buffer_size), dbname, &db));
  for (int i = start; i < count; i++) {
    WORKER_OK(db->Put(WriteOptions(), Key(writer, i), Value(writer, i)));
    if (ack_fd >= 0) {
      const int32_t index = i;
      WORKER_CHECK(::write(ack_fd, &index, sizeof(index)) == sizeof(index),
                   "ack");
    }
    // A key that every writer fights over.
    WORKER_OK(db->Put(WriteOptions(), "shared", Value(writer, i)));
    if (groups) {
      WriteBatch batch;
      const std::string v = Value(writer, i);
      for (int g = 0; g < kGroupSize; g++) {
        batch.Put(GroupKey(g), v);
      }
      WORKER_OK(db->Write(WriteOptions(), &batch));
    }
    if (compact_every > 0 && (i + 1) % compact_every == 0) {
      db->CompactRange(nullptr, nullptr);
    }
  }
  // Read-your-writes, and everybody else's as far as they got.
  WORKER_CHECK(CountPrefix(db, writer) == count, "own keys missing");
  delete db;
  return 0;
}

// follower <db> <stop_file> <read_only> <num_writers> <expect>
// Step 2: follows the writers until stop_file appears.  Each round checks
// that every writer's keys form a prefix that never shrinks, and that the
// group keys were updated atomically.  If expect >= 0, exits successfully
// as soon as all writers have reached "expect" keys.
int FollowerWorker(int argc, char** argv) {
  WORKER_CHECK(argc == 5, "usage");
  const std::string dbname = argv[0];
  const std::string stop_file = argv[1];
  const bool read_only = std::atoi(argv[2]) != 0;
  const int num_writers = std::atoi(argv[3]);
  const int expect = std::atoi(argv[4]);

  Options options = MpOptions(4 << 20);
  options.read_only = read_only;
  options.create_if_missing = false;
  DB* db;
  WORKER_OK(DB::Open(options, dbname, &db));
  std::vector<int> seen(num_writers, 0);
  uint64_t rounds = 0;
  while (true) {
    // Sample this before reading: once it exists all writers are done, so
    // the round that follows must see everything.
    const bool stopping = FileExists(stop_file);
    bool all_done = (expect >= 0);
    for (int w = 0; w < num_writers; w++) {
      const int n = CountPrefix(db, w);
      WORKER_CHECK(n >= seen[w], "writer %d went back from %d to %d", w,
                   seen[w], n);
      seen[w] = n;
      if (n < expect) all_done = false;
      // Point lookups agree with the scan.
      if (n > 0) {
        std::string value;
        WORKER_OK(db->Get(ReadOptions(), Key(w, n - 1), &value));
        WORKER_CHECK(value == Value(w, n - 1), "Get disagrees with scan");
      }
    }

    // Group keys: all or nothing, both through an iterator and through a
    // snapshot.
    {
      Iterator* it = db->NewIterator(ReadOptions());
      std::string first;
      int n = 0;
      for (it->Seek("g-"); it->Valid() && it->key().starts_with("g-");
           it->Next()) {
        if (n == 0) first = it->value().ToString();
        WORKER_CHECK(it->value() == Slice(first), "torn group (iterator)");
        n++;
      }
      WORKER_OK(it->status());
      WORKER_CHECK(n == 0 || n == kGroupSize, "group has %d keys", n);
      delete it;
    }
    {
      const Snapshot* snapshot = db->GetSnapshot();
      // Give compactions in other processes a chance to run over the
      // versions this snapshot needs.
      if (rounds % 8 == 0) Env::Default()->SleepForMicroseconds(20000);
      ReadOptions ro;
      ro.snapshot = snapshot;
      std::string first, value;
      Status s = db->Get(ro, GroupKey(0), &first);
      if (s.ok()) {
        for (int g = 1; g < kGroupSize; g++) {
          WORKER_OK(db->Get(ro, GroupKey(g), &value));
          WORKER_CHECK(value == first, "torn group (snapshot)");
        }
      } else {
        WORKER_CHECK(s.IsNotFound(), "%s", s.ToString().c_str());
      }
      db->ReleaseSnapshot(snapshot);
    }

    rounds++;
    if (all_done || stopping) break;
  }
  if (expect >= 0) {
    for (int w = 0; w < num_writers; w++) {
      WORKER_CHECK(seen[w] >= expect, "writer %d: saw only %d of %d keys", w,
                   seen[w], expect);
    }
  }
  delete db;
  return 0;
}

int WorkerMain(int argc, char** argv) {
  const std::string role = argv[0];
  if (role == "reader_static") return ReaderStaticWorker(argc - 1, argv + 1);
  if (role == "writer") return WriterWorker(argc - 1, argv + 1);
  if (role == "follower") return FollowerWorker(argc - 1, argv + 1);
  std::fprintf(stderr, "unknown worker role %s\n", role.c_str());
  return 2;
}

// ---------------------------------------------------------------------
// Process control.

pid_t Spawn(const std::vector<std::string>& args) {
  std::vector<std::string> full = {"/proc/self/exe", "--worker"};
  full.insert(full.end(), args.begin(), args.end());
  std::vector<char*> argv;
  for (std::string& a : full) argv.push_back(&a[0]);
  argv.push_back(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    ::execv(argv[0], argv.data());
    std::perror("execv");
    ::_exit(127);
  }
  return pid;
}

// Returns true iff the process exited normally with status 0.
bool WaitOk(pid_t pid) {
  int status = 0;
  if (::waitpid(pid, &status, 0) != pid) return false;
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

std::string Num(long long n) { return std::to_string(n); }

pid_t SpawnWriter(const std::string& dbname, int writer, int start, int count,
                  size_t write_buffer_size, int ack_fd = -1,
                  int compact_every = 0, bool groups = false) {
  return Spawn({"writer", dbname, Num(writer), Num(start), Num(count),
                Num(write_buffer_size), Num(ack_fd), Num(compact_every),
                Num(groups ? 1 : 0)});
}

pid_t SpawnFollower(const std::string& dbname, const std::string& stop_file,
                    bool read_only, int num_writers, int expect) {
  return Spawn({"follower", dbname, stop_file, Num(read_only ? 1 : 0),
                Num(num_writers), Num(expect)});
}

void Touch(const std::string& fname) {
  ASSERT_LEVELDB_OK(WriteStringToFile(Env::Default(), "x", fname));
}

// ---------------------------------------------------------------------
// Checks on the files.

std::vector<uint64_t> FilesOfType(const std::string& dbname, FileType want) {
  std::vector<std::string> filenames;
  Env::Default()->GetChildren(dbname, &filenames);
  std::vector<uint64_t> result;
  for (const std::string& f : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == want) {
      result.push_back(number);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

// Checks that the write-ahead logs hold a gapless run of sequence numbers
// with none used twice.  Sets *first and *last to the range found.
void CheckLogSequences(const std::string& dbname, uint64_t* first,
                       uint64_t* last, int* records) {
  struct Reporter : public log::Reader::Reporter {
    void Corruption(size_t bytes, const Status& s) override {
      ADD_FAILURE() << "log corruption: " << bytes << " bytes; "
                    << s.ToString();
    }
  };
  *first = 0;
  *last = 0;
  *records = 0;
  for (uint64_t number : FilesOfType(dbname, kLogFile)) {
    SequentialFile* file;
    ASSERT_LEVELDB_OK(
        Env::Default()->NewSequentialFile(LogFileName(dbname, number), &file));
    Reporter reporter;
    log::Reader reader(file, &reporter, true, 0);
    Slice record;
    std::string scratch;
    WriteBatch batch;
    while (reader.ReadRecord(&record, &scratch)) {
      ASSERT_GE(record.size(), 12u);
      WriteBatchInternal::SetContents(&batch, record);
      const uint64_t seq = WriteBatchInternal::Sequence(&batch);
      const uint64_t count = WriteBatchInternal::Count(&batch);
      ASSERT_GT(count, 0u);
      if (*records == 0) {
        *first = seq;
      } else {
        ASSERT_EQ(*last + 1, seq)
            << "log " << number << ": sequence numbers reused or skipped";
      }
      *last = seq + count - 1;
      (*records)++;
    }
    delete file;
  }
}

void CheckWriterKeys(DB* db, int writer, int count) {
  Iterator* it = db->NewIterator(ReadOptions());
  const std::string prefix = Key(writer, 0).substr(0, 4);
  int n = 0;
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
       it->Next()) {
    if (Key(writer, n) != it->key().ToString()) {
      // (An iterator skips a table it cannot read and reports it here.)
      ADD_FAILURE() << "expected " << Key(writer, n) << ", found "
                    << it->key().ToString() << "; iterator status "
                    << it->status().ToString();
      break;
    }
    EXPECT_EQ(Value(writer, n), it->value().ToString());
    n++;
  }
  EXPECT_LEVELDB_OK(it->status());
  delete it;
  ASSERT_EQ(count, n) << "writer " << writer;
}

class MultiProcessTest : public testing::Test {
 public:
  MultiProcessTest() {
    dbname_ = testing::TempDir() + "multiprocess_test";
    stop_file_ = testing::TempDir() + "multiprocess_test.stop";
    DestroyDB(dbname_, Options());
    Env::Default()->RemoveFile(stop_file_);
  }

  ~MultiProcessTest() override {
    Env::Default()->RemoveFile(stop_file_);
    // Set this to look at the database (its LOG, mostly) after a test.
    if (std::getenv("LEVELDB_MP_TEST_KEEP_DB") == nullptr) {
      DestroyDB(dbname_, Options());
    }
  }

  // Opens the database the classic way with paranoid checks, which fails if
  // a table file named in the MANIFEST is missing.
  void CheckWithClassicOpen(int num_writers, int count) {
    Options options;
    options.paranoid_checks = true;
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db));
    for (int w = 0; w < num_writers; w++) {
      CheckWriterKeys(db, w, count);
    }
    delete db;
  }

  std::string dbname_;
  std::string stop_file_;
};

}  // namespace

// ---------------------------------------------------------------------
// Step 1: read-only open.

TEST_F(MultiProcessTest, ReadOnlyOpenByManyProcesses) {
  const int kCount = 3000;
  {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100000;  // Some tables, some log.
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db));
    for (int i = 0; i < kCount; i++) {
      ASSERT_LEVELDB_OK(db->Put(WriteOptions(), Key(0, i), Value(0, i)));
    }
    delete db;
  }
  std::vector<std::string> before;
  ASSERT_LEVELDB_OK(Env::Default()->GetChildren(dbname_, &before));
  std::sort(before.begin(), before.end());

  std::vector<pid_t> readers;
  for (int i = 0; i < 3; i++) {
    readers.push_back(Spawn({"reader_static", dbname_, "0", Num(kCount)}));
  }
  for (pid_t pid : readers) {
    ASSERT_TRUE(WaitOk(pid));
  }

  // The readers left the directory alone.
  std::vector<std::string> after;
  ASSERT_LEVELDB_OK(Env::Default()->GetChildren(dbname_, &after));
  std::sort(after.begin(), after.end());
  ASSERT_EQ(before, after);
  CheckWithClassicOpen(1, kCount);
}

TEST_F(MultiProcessTest, ReadOnlyOpenOfMissingDb) {
  Options options;
  options.read_only = true;
  options.create_if_missing = true;  // Ignored.
  DB* db = nullptr;
  ASSERT_TRUE(DB::Open(options, dbname_, &db).IsInvalidArgument());
  ASSERT_TRUE(db == nullptr);
  ASSERT_FALSE(FileExists(dbname_ + "/CURRENT"));
}

// ---------------------------------------------------------------------
// Two handles in one process share nothing but the files, exactly like two
// processes, which makes the interleavings easy to control.

TEST_F(MultiProcessTest, TwoHandlesSeeEachOthersWrites) {
  DB* a;
  DB* b;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &a));
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &b));

  std::string value;
  ASSERT_LEVELDB_OK(a->Put(WriteOptions(), "k", "from-a"));
  ASSERT_LEVELDB_OK(b->Get(ReadOptions(), "k", &value));
  ASSERT_EQ("from-a", value);
  ASSERT_LEVELDB_OK(b->Put(WriteOptions(), "k", "from-b"));
  ASSERT_LEVELDB_OK(a->Get(ReadOptions(), "k", &value));
  ASSERT_EQ("from-b", value);
  ASSERT_LEVELDB_OK(a->Delete(WriteOptions(), "k"));
  ASSERT_TRUE(b->Get(ReadOptions(), "k", &value).IsNotFound());

  // Alternate writers across many log rotations, flushes and compactions.
  const int kCount = 6000;
  for (int i = 0; i < kCount; i++) {
    DB* w = (i % 2 == 0) ? a : b;
    DB* r = (i % 2 == 0) ? b : a;
    ASSERT_LEVELDB_OK(w->Put(WriteOptions(), Key(0, i), Value(0, i)));
    if (i % 97 == 0) {
      ASSERT_LEVELDB_OK(r->Get(ReadOptions(), Key(0, i), &value));
      ASSERT_EQ(Value(0, i), value);
    }
  }
  CheckWriterKeys(a, 0, kCount);
  CheckWriterKeys(b, 0, kCount);
  ASSERT_GT(FilesOfType(dbname_, kTableFile).size(), 0u);

  delete a;
  // b carries on alone, taking over compaction if a was doing it.
  for (int i = kCount; i < 2 * kCount; i++) {
    ASSERT_LEVELDB_OK(b->Put(WriteOptions(), Key(0, i), Value(0, i)));
  }
  CheckWriterKeys(b, 0, 2 * kCount);
  delete b;

  CheckWithClassicOpen(1, 2 * kCount);
}

TEST_F(MultiProcessTest, SnapshotSurvivesCompactionByOtherHandle) {
  DB* a;
  DB* b;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &a));
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &b));

  ASSERT_LEVELDB_OK(a->Put(WriteOptions(), "k", "old"));
  ASSERT_LEVELDB_OK(a->Put(WriteOptions(), "gone", "still-here"));
  const Snapshot* snapshot = b->GetSnapshot();
  Iterator* old_iter = b->NewIterator(ReadOptions());

  // a overwrites, deletes and compacts everything, repeatedly.
  for (int round = 0; round < 3; round++) {
    for (int i = 0; i < 2000; i++) {
      ASSERT_LEVELDB_OK(a->Put(WriteOptions(), Key(0, i), Value(round, i)));
    }
    ASSERT_LEVELDB_OK(a->Put(WriteOptions(), "k", "new" + Num(round)));
    ASSERT_LEVELDB_OK(a->Delete(WriteOptions(), "gone"));
    a->CompactRange(nullptr, nullptr);
  }

  std::string value;
  ReadOptions ro;
  ro.snapshot = snapshot;
  ASSERT_LEVELDB_OK(b->Get(ro, "k", &value));
  ASSERT_EQ("old", value);
  ASSERT_LEVELDB_OK(b->Get(ro, "gone", &value));
  ASSERT_EQ("still-here", value);

  // The iterator was created before any table existed; it still works.
  int n = 0;
  for (old_iter->SeekToFirst(); old_iter->Valid(); old_iter->Next()) n++;
  ASSERT_LEVELDB_OK(old_iter->status());
  ASSERT_EQ(2, n);
  delete old_iter;

  ASSERT_LEVELDB_OK(b->Get(ReadOptions(), "k", &value));
  ASSERT_EQ("new2", value);
  ASSERT_TRUE(b->Get(ReadOptions(), "gone", &value).IsNotFound());
  b->ReleaseSnapshot(snapshot);

  // CompactRange from the handle that is not the compactor.
  for (int i = 0; i < 2000; i++) {
    ASSERT_LEVELDB_OK(b->Put(WriteOptions(), Key(1, i), Value(1, i)));
  }
  b->CompactRange(nullptr, nullptr);
  CheckWriterKeys(a, 1, 2000);

  delete b;
  delete a;
}

TEST_F(MultiProcessTest, ObsoleteFilesAreDeleted) {
  DB* a;
  DB* b;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &a));
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &b));
  std::string value;
  for (int round = 0; round < 5; round++) {
    for (int i = 0; i < 2000; i++) {
      ASSERT_LEVELDB_OK(a->Put(WriteOptions(), Key(0, i), Value(round, i)));
    }
    ASSERT_LEVELDB_OK(b->Get(ReadOptions(), Key(0, 0), &value));
    a->CompactRange(nullptr, nullptr);
    b->CompactRange(nullptr, nullptr);
  }
  // 2000 keys of ~120 bytes fit in one table once fully compacted; the
  // files of the earlier rounds must be gone although b read from them.
  ASSERT_LE(FilesOfType(dbname_, kTableFile).size(), 2u);
  ASSERT_LE(FilesOfType(dbname_, kLogFile).size(), 2u);
  ASSERT_EQ(1u, FilesOfType(dbname_, kDescriptorFile).size());
  delete a;
  delete b;
}

TEST_F(MultiProcessTest, ReadOnlyHandleFollows) {
  DB* w;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &w));
  Options options = MpOptions(64 << 10);
  options.read_only = true;
  DB* r;
  ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &r));
  ASSERT_TRUE(r->Put(WriteOptions(), "a", "b").IsNotSupportedError());
  for (int i = 0; i < 3000; i++) {
    ASSERT_LEVELDB_OK(w->Put(WriteOptions(), Key(0, i), Value(0, i)));
  }
  CheckWriterKeys(r, 0, 3000);
  delete w;
  CheckWriterKeys(r, 0, 3000);
  delete r;
}

TEST_F(MultiProcessTest, ReadOnlyHandleOpensFirst) {
  // Leave a database with unflushed data in its log.
  {
    Options options;
    options.create_if_missing = true;
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db));
    for (int i = 0; i < 100; i++) {
      ASSERT_LEVELDB_OK(db->Put(WriteOptions(), Key(0, i), Value(0, i)));
    }
    delete db;
  }
  Options options = MpOptions(64 << 10);
  options.read_only = true;
  DB* r;
  ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &r));
  CheckWriterKeys(r, 0, 100);
  DB* w;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &w));
  for (int i = 100; i < 3000; i++) {
    ASSERT_LEVELDB_OK(w->Put(WriteOptions(), Key(0, i), Value(0, i)));
  }
  CheckWriterKeys(r, 0, 3000);
  delete r;
  delete w;
  CheckWithClassicOpen(1, 3000);
}

TEST_F(MultiProcessTest, ClassicAndMultiProcessOpenExcludeEachOther) {
  DB* mp;
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(4 << 20), dbname_, &mp));
  DB* classic = nullptr;
  Options options;
  ASSERT_FALSE(DB::Open(options, dbname_, &classic).ok());
  ASSERT_TRUE(classic == nullptr);
  delete mp;

  ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &classic));
  mp = nullptr;
  ASSERT_FALSE(DB::Open(MpOptions(4 << 20), dbname_, &mp).ok());
  ASSERT_TRUE(mp == nullptr);
  delete classic;
}

TEST_F(MultiProcessTest, ConcurrentThreadsOnTwoHandles) {
  DB* dbs[2];
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &dbs[0]));
  ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &dbs[1]));
  const int kThreads = 4;
  const int kCount = 3000;
  std::atomic<bool> failed(false);
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&, t] {
      DB* db = dbs[t % 2];
      DB* other = dbs[(t + 1) % 2];
      std::string value;
      for (int i = 0; i < kCount && !failed; i++) {
        if (!db->Put(WriteOptions(), Key(t, i), Value(t, i)).ok()) {
          failed = true;
        }
        if (i % 50 == 0) {
          Status s = other->Get(ReadOptions(), Key(t, i), &value);
          if (!s.ok() || value != Value(t, i)) failed = true;
        }
      }
    });
  }
  for (std::thread& t : threads) t.join();
  ASSERT_FALSE(failed);
  for (int t = 0; t < kThreads; t++) {
    CheckWriterKeys(dbs[t % 2], t, kCount);
    CheckWriterKeys(dbs[(t + 1) % 2], t, kCount);
  }
  delete dbs[0];
  delete dbs[1];
  CheckWithClassicOpen(kThreads, kCount);
}

// ---------------------------------------------------------------------
// Step 2: one writer, many followers.

TEST_F(MultiProcessTest, OneWriterManyFollowers) {
  const int kCount = 15000;
  // Create the database first so the followers can open it.
  {
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(MpOptions(64 << 10), dbname_, &db));
    delete db;
  }
  // A small write buffer: about 30 log rotations plus compactions.
  const pid_t writer = SpawnWriter(dbname_, 0, 0, kCount, 64 << 10);
  std::vector<pid_t> followers;
  followers.push_back(SpawnFollower(dbname_, stop_file_, true, 1, kCount));
  followers.push_back(SpawnFollower(dbname_, stop_file_, true, 1, kCount));
  followers.push_back(SpawnFollower(dbname_, stop_file_, false, 1, kCount));
  ASSERT_TRUE(WaitOk(writer));
  Touch(stop_file_);
  for (pid_t pid : followers) {
    ASSERT_TRUE(WaitOk(pid));
  }
  CheckWithClassicOpen(1, kCount);
}

// ---------------------------------------------------------------------
// Step 3: several writers.

TEST_F(MultiProcessTest, ManyWritersUseEachSequenceOnce) {
  const int kWriters = 4;
  const int kCount = 2500;
  // A write buffer large enough that the log is never rotated, so that it
  // holds every write when the writers are done.
  std::vector<pid_t> writers;
  for (int w = 0; w < kWriters; w++) {
    writers.push_back(SpawnWriter(dbname_, w, 0, kCount, 64 << 20));
  }
  for (pid_t pid : writers) {
    ASSERT_TRUE(WaitOk(pid));
  }

  uint64_t first, last;
  int records;
  CheckLogSequences(dbname_, &first, &last, &records);
  ASSERT_EQ(1u, first);
  // Each writer did two one-key writes per key.
  ASSERT_EQ(static_cast<uint64_t>(kWriters * kCount * 2), last);
  ASSERT_LE(records, kWriters * kCount * 2);  // Group commit merges some.
  ASSERT_EQ(1u, FilesOfType(dbname_, kLogFile).size());

  CheckWithClassicOpen(kWriters, kCount);
}

TEST_F(MultiProcessTest, ManyWritersWithRotationAndFollowers) {
  const int kWriters = 4;
  const int kCount = 5000;
  std::vector<pid_t> pids;
  for (int w = 0; w < kWriters; w++) {
    pids.push_back(SpawnWriter(dbname_, w, 0, kCount, 64 << 10, -1, 0, true));
  }
  // Give a writer time to create the database.
  while (!FileExists(dbname_ + "/CURRENT")) {
    Env::Default()->SleepForMicroseconds(1000);
  }
  std::vector<pid_t> followers;
  followers.push_back(
      SpawnFollower(dbname_, stop_file_, true, kWriters, kCount));
  followers.push_back(
      SpawnFollower(dbname_, stop_file_, false, kWriters, kCount));
  for (pid_t pid : pids) {
    ASSERT_TRUE(WaitOk(pid));
  }
  Touch(stop_file_);
  for (pid_t pid : followers) {
    ASSERT_TRUE(WaitOk(pid));
  }
  uint64_t first, last;
  int records;
  CheckLogSequences(dbname_, &first, &last, &records);  // What is left of it.
  CheckWithClassicOpen(kWriters, kCount);
}

// ---------------------------------------------------------------------
// Step 4: compaction while others read and write.

TEST_F(MultiProcessTest, ForcedCompactionsWhileReadersIterate) {
  const int kWriters = 3;
  const int kCount = 4000;
  std::vector<pid_t> pids;
  for (int w = 0; w < kWriters; w++) {
    // Each writer also forces full compactions, from whichever process it
    // happens to be.
    pids.push_back(
        SpawnWriter(dbname_, w, 0, kCount, 64 << 10, -1, 500 + 100 * w, true));
  }
  while (!FileExists(dbname_ + "/CURRENT")) {
    Env::Default()->SleepForMicroseconds(1000);
  }
  std::vector<pid_t> followers;
  for (int i = 0; i < 3; i++) {
    followers.push_back(
        SpawnFollower(dbname_, stop_file_, i != 0, kWriters, -1));
  }
  for (pid_t pid : pids) {
    ASSERT_TRUE(WaitOk(pid));
  }
  Touch(stop_file_);
  for (pid_t pid : followers) {
    ASSERT_TRUE(WaitOk(pid));
  }
  CheckWithClassicOpen(kWriters, kCount);
}

// ---------------------------------------------------------------------
// Step 5: processes die.

class MultiProcessCrashTest : public MultiProcessTest,
                              public testing::WithParamInterface<size_t> {};

TEST_P(MultiProcessCrashTest, KillWritersAtRandom) {
  const size_t write_buffer_size = GetParam();
  const int kWriters = 3;
  const int kCount = 1000000;  // Never reached.
  const int kKills = 25;

  struct WriterProc {
    pid_t pid = -1;
    int ack_fd = -1;  // Read end.
    int acked = -1;   // Highest index acknowledged so far.
  };
  std::vector<WriterProc> procs(kWriters);
  auto start = [&](int w) {
    int fds[2];
    ASSERT_EQ(0, ::pipe(fds));
    procs[w].pid = SpawnWriter(dbname_, w, procs[w].acked + 1, kCount,
                               write_buffer_size, fds[1]);
    ::close(fds[1]);
    procs[w].ack_fd = fds[0];
  };
  // Kills writer w and collects what it had acknowledged.
  auto kill = [&](int w) {
    ASSERT_EQ(0, ::kill(procs[w].pid, SIGKILL));
    int status;
    ASSERT_EQ(procs[w].pid, ::waitpid(procs[w].pid, &status, 0));
    ASSERT_TRUE(WIFSIGNALED(status))
        << "writer " << w << " had already exited with "
        << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    int32_t index;
    while (::read(procs[w].ack_fd, &index, sizeof(index)) == sizeof(index)) {
      ASSERT_EQ(procs[w].acked + 1, index);
      procs[w].acked = index;
    }
    ::close(procs[w].ack_fd);
  };

  for (int w = 0; w < kWriters; w++) start(w);
  while (!FileExists(dbname_ + "/CURRENT")) {
    Env::Default()->SleepForMicroseconds(1000);
  }
  // Followers check throughout that what they see is consistent.
  std::vector<pid_t> followers;
  // They are read-only so that the compactor is always one of the writers
  // that get killed.
  followers.push_back(SpawnFollower(dbname_, stop_file_, true, kWriters, -1));
  followers.push_back(SpawnFollower(dbname_, stop_file_, true, kWriters, -1));

  Random rnd(testing::UnitTest::GetInstance()->random_seed() + 301);
  for (int i = 0; i < kKills; i++) {
    Env::Default()->SleepForMicroseconds(20000 + rnd.Uniform(120000));
    const int w = rnd.Uniform(kWriters);
    kill(w);
    if (testing::Test::HasFatalFailure()) return;
    start(w);
  }
  Env::Default()->SleepForMicroseconds(100000);
  for (int w = 0; w < kWriters; w++) {
    kill(w);
    if (testing::Test::HasFatalFailure()) return;
    ASSERT_GT(procs[w].acked, 0) << "writer " << w << " made no progress";
  }
  Touch(stop_file_);
  for (pid_t pid : followers) {
    ASSERT_TRUE(WaitOk(pid));
  }

  // Everything acknowledged is there, plus at most the one write that each
  // writer had not acknowledged yet.  First as seen by a new group ...
  auto check = [&](DB* db) {
    for (int w = 0; w < kWriters; w++) {
      Iterator* it = db->NewIterator(ReadOptions());
      const std::string prefix = Key(w, 0).substr(0, 4);
      int n = 0;
      for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
           it->Next()) {
        ASSERT_EQ(Key(w, n), it->key().ToString());
        ASSERT_EQ(Value(w, n), it->value().ToString());
        n++;
      }
      ASSERT_LEVELDB_OK(it->status());
      delete it;
      ASSERT_GE(n, procs[w].acked + 1) << "writer " << w << " lost writes";
      ASSERT_LE(n, procs[w].acked + 2) << "writer " << w;
    }
  };
  {
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(MpOptions(write_buffer_size), dbname_, &db));
    check(db);
    if (testing::Test::HasFatalFailure()) return;
    // ... which can also keep writing, compacting and cleaning up.
    for (int i = 0; i < 2000; i++) {
      ASSERT_LEVELDB_OK(db->Put(WriteOptions(), Key(50, i), Value(50, i)));
    }
    db->CompactRange(nullptr, nullptr);
    CheckWriterKeys(db, 50, 2000);
    delete db;
  }
  uint64_t first, last;
  int records;
  CheckLogSequences(dbname_, &first, &last, &records);
  // ... then by a classic open, which also verifies that no table named in
  // the MANIFEST is missing.
  {
    Options options;
    options.paranoid_checks = true;
    DB* db;
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db));
    check(db);
    delete db;
  }
}

INSTANTIATE_TEST_SUITE_P(
    WriteBufferSizes, MultiProcessCrashTest,
    testing::Values(static_cast<size_t>(64 << 10),    // Flushes, compactions
                    static_cast<size_t>(64 << 20)));  // Log only

}  // namespace leveldb

int main(int argc, char** argv) {
  if (argc >= 3 && std::strcmp(argv[1], "--worker") == 0) {
    return leveldb::WorkerMain(argc - 2, argv + 2);
  }
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
