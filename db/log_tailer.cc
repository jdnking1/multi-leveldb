// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_tailer.h"

#include "db/log_format.h"

namespace leveldb {

Status LogTailer::BoundedFile::Read(size_t n, Slice* result, char* scratch) {
  if (offset >= limit) {
    *result = Slice();
    return Status::OK();
  }
  if (limit - offset < n) {
    n = static_cast<size_t>(limit - offset);
  }
  Status s = file->Read(n, result, scratch);
  offset += result->size();
  return s;
}

Status LogTailer::BoundedFile::Skip(uint64_t n) {
  Status s = file->Skip(n);
  if (s.ok()) offset += n;
  return s;
}

void LogTailer::Reporter::Corruption(size_t bytes, const Status& s) {
  // Fragments at the start of the first block belong to a record that
  // began before the starting offset; dropping them is expected.
  if (tailer->reader_ != nullptr &&
      tailer->base_ + tailer->reader_->Position() <= tailer->start_) {
    return;
  }
  if (status.ok()) status = s;
}

LogTailer::LogTailer(Env* env, const std::string& fname)
    : env_(env),
      fname_(fname),
      reader_(nullptr),
      base_(0),
      start_(0),
      position_(0) {
  reporter_.tailer = this;
}

LogTailer::~LogTailer() {
  delete reader_;
  delete file_.file;
}

Status LogTailer::Open(uint64_t start) {
  assert(reader_ == nullptr);
  Status s = env_->NewSequentialFile(fname_, &file_.file);
  if (!s.ok()) {
    file_.file = nullptr;
    return s;
  }
  // Start the reader at the beginning of the block that contains "start" so
  // that it sees block boundaries where the writer put them, and skip the
  // records in that block that end at or before "start".
  start_ = start;
  position_ = start;
  base_ = start - (start % log::kBlockSize);
  if (base_ > 0) {
    s = file_.file->Skip(base_);
    if (!s.ok()) return s;
  }
  file_.offset = base_;
  file_.limit = base_;
  reader_ = new log::Reader(&file_, &reporter_, true /*checksum*/,
                            0 /*initial_offset*/);
  return Status::OK();
}

void LogTailer::SetLimit(uint64_t limit) {
  assert(reader_ != nullptr);
  if (limit > file_.limit) {
    file_.limit = limit;
    reader_->UnmarkEOF();
  }
}

bool LogTailer::ReadRecord(Slice* record, std::string* scratch) {
  while (reader_->ReadRecord(record, scratch)) {
    const uint64_t end = base_ + reader_->Position();
    if (end <= start_) {
      continue;  // Before the starting offset.
    }
    position_ = end;
    return true;
  }
  return false;
}

}  // namespace leveldb
