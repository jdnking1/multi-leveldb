# Multi-process access

By default a database can be open in one process at a time. Two options in
`include/leveldb/options.h` relax that.

## `Options::read_only`

Opens the database without taking the `LOCK` and without changing anything in
the directory: no new log, no new MANIFEST, no info log, no compactions, no
file deletion. Updates found in the log files are replayed into memory.
`Write()` returns `NotSupported` and `CompactRange()` does nothing.

Any number of processes may open a database this way at once, but only while
no process modifies it. The handle is a view as of `Open()`.

## `Options::multi_process`

Several processes open the same database, all with this option, and any of
them may read and write (unless it also sets `read_only`, which then means "a
follower that never writes"). Linux only.

    leveldb::Options options;
    options.create_if_missing = true;
    options.multi_process = true;
    leveldb::DB* db;
    leveldb::Status s = leveldb::DB::Open(options, "/tmp/db", &db);

Guarantees:

* A read (`Get`, `NewIterator`, `GetSnapshot`) sees every write that
  completed, in any process, before the read started.
* Snapshots and iterators stay valid while other processes write and compact.
* A process that is killed does not take the database down: writers continue,
  and if it was the compactor another writable process takes over. A killed
  process loses nothing that `Write()` had acknowledged.
* The files stay in the standard format. Once all processes have closed the
  database it can be opened without the option again.

### Files

Besides the usual files the directory contains

| File | Purpose |
| --- | --- |
| `STATE` | 8 KiB, mapped `MAP_SHARED` by every process. Holds the shared counters and one slot per process. Rebuilt by the first opener; never needs to survive a reboot. |
| `WRITE.lock` | `flock`ed by the process that is appending to the log. |
| `COMPACT.lock` | `flock`ed by the compactor for as long as it has the database open. |
| `LOCK` | Multi-process openers hold a shared lock on it, so a classic open (exclusive lock) fails while they exist, and vice versa. |

### How it works

Each process keeps the normal in-memory state (memtable, immutable memtable,
`VersionSet`) but as a replica that it maintains from the files.
`DBImpl::CatchUp()` (`db/db_impl_mp.cc`) brings the replica up to date; it
runs before every read, in the writer once it holds `WRITE.lock`, and in a
poll thread. When nothing has changed it costs a few loads from `STATE`.

**Writes.** The group-commit leader takes `WRITE.lock`, catches up to the
end of the log, assigns sequence numbers, appends, and publishes `{log
number, committed offset, last sequence}` in `STATE`. Other processes replay
the log only up to the committed offset (`db/log_tailer.h`), so they never see
a half-written record. If a writer dies mid-append, the next writer truncates
the log back to the committed offset.

**Log rotation and flush.** A writer that finds the memtable full creates a
new log and publishes its number. Every process then finishes reading the old
log and makes its memtable immutable. The compactor writes its copy to a
table and logs a MANIFEST edit; when the other processes apply the edit they
drop their copies. Writers wait for that edit before rotating again.

**MANIFEST.** Only the compactor writes it, and publishes `{manifest number,
committed offset, epoch}` after each edit. The others apply edits up to the
committed offset (`VersionSet::TailManifest`). A process that becomes the
compactor starts a new MANIFEST with a full snapshot, so an edit that a dead
compactor wrote but did not publish is never built upon.

**Deleting files.** Only the compactor deletes. Before a process reads table
files it publishes, in its slot, the epoch of the oldest version it uses; the
compactor keeps every version it produced until no live process pins an epoch
that old. A slot is live while its owner holds a lock on the slot's byte of
`STATE`, which the kernel releases when the process dies. Snapshots work the
same way: each process publishes its oldest snapshot and compactions keep
whatever the oldest one across all processes can see.

### Limits

* Linux only (needs open-file-description locks); the default `Env`.
* At most 128 processes per database.
* Every process holds its own copy of the unflushed data and its own caches.
* Writes from different processes are serialized and each process replays the
  others' writes, so total write throughput goes down, not up, with the
  number of writing processes.
* `CompactRange(begin, end)` called in a process that is not the compactor
  flushes, then asks the compactor to compact the whole key range.
* A compactor whose background work fails (for example, disk full) keeps the
  role until it closes the database; writers in other processes wait.
* Each writable process runs a thread that wakes up once per millisecond.

### Tests

`db/multiprocess_test.cc` starts real processes (it re-executes itself as a
worker) and covers read-only opens, followers, concurrent writers, forced
compactions under readers, and `SIGKILL` at random points.
