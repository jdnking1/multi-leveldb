// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// LogTailer reads records from a log file (a WAL or a MANIFEST) that another
// process may still be appending to.
//
// log::Reader on its own is not suitable for this: when it runs into a
// partially written record at the end of the file it drops the record and
// stops for good.  LogTailer avoids ever showing the reader a partial record
// by only reading up to a caller supplied limit, which is the "committed"
// offset that the writer publishes after each complete append.  When the
// limit is raised the reader is told to look at the file again.

#ifndef STORAGE_LEVELDB_DB_LOG_TAILER_H_
#define STORAGE_LEVELDB_DB_LOG_TAILER_H_

#include "db/log_reader.h"
#include <cstdint>
#include <string>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class LogTailer {
 public:
  static constexpr uint64_t kNoLimit = ~static_cast<uint64_t>(0);

  LogTailer(Env* env, const std::string& fname);

  LogTailer(const LogTailer&) = delete;
  LogTailer& operator=(const LogTailer&) = delete;

  ~LogTailer();

  // Open the file.  The first record returned will be the one that starts at
  // file offset "start", which must be a record boundary (0, or a value
  // previously returned by Position(), or a committed offset).
  // Returns a NotFound status if the file does not exist.
  Status Open(uint64_t start);

  // Allow reading up to file offset "limit", which must be a record boundary
  // and must not be smaller than the previous limit.  kNoLimit reads to the
  // physical end of the file, which is only safe once the file is no longer
  // being appended to.  The initial limit is 0.
  void SetLimit(uint64_t limit);

  // Like log::Reader::ReadRecord().  Returns false at the limit.
  bool ReadRecord(Slice* record, std::string* scratch);

  // File offset just past the last record returned ("start" if none yet).
  uint64_t Position() const { return position_; }

  // The first corruption seen, if any.
  Status status() const { return reporter_.status; }

 private:
  // Exposes [base, limit) of the underlying file.
  class BoundedFile : public SequentialFile {
   public:
    Status Read(size_t n, Slice* result, char* scratch) override;
    Status Skip(uint64_t n) override;

    SequentialFile* file = nullptr;
    uint64_t offset = 0;  // Offset in "file" of the next byte to read
    uint64_t limit = 0;
  };

  struct Reporter : public log::Reader::Reporter {
    void Corruption(size_t bytes, const Status& s) override;

    LogTailer* tailer = nullptr;
    Status status;
  };

  Env* const env_;
  const std::string fname_;
  BoundedFile file_;
  Reporter reporter_;
  log::Reader* reader_;
  uint64_t base_;   // File offset at which reader_ started (a block start)
  uint64_t start_;  // Records that end at or before this are skipped
  uint64_t position_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_TAILER_H_
