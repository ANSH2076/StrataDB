# Stage 2 — Learning Notes

These are my notes on how Stage 2 of Strata works.  Stage 2 extends the
database from an in-memory WAL/MemTable store into an LSM-tree that
flushes to sorted on-disk files (**SSTables**) and reads across both memory
and disk generations.

---

## 1. What Each File Does

| File | Job |
|------|-----|
| `strata/record.py` | Shared line framing logic (`encode_record` and `decode_record`). Computes and verifies CRC32 checksums over JSON payloads for both the WAL and SSTables. |
| `strata/sstable.py` | Immutable on-disk sorted string tables. Implements atomic writes (`write_sstable`) via temporary files, and sparse-indexed reads (`SSTable.get`) scanning at most 16 records. |
| `strata/memtable.py` | In-memory `dict` holding recent updates and tombstones (`None` values), tracking approximate byte usage and key counts. |
| `strata/wal.py` | Append-only log recording mutations prior to memory. Includes `reset()` to truncate the log after a successful SSTable flush. |
| `strata/engine.py` | Public `Strata` API. Orchestrates multi-level reads (MemTable $\to$ SSTables newest-to-oldest), triggers flushes on size limits, and performs crash recovery. |
| `tests/test_engine.py` | Unit test suite covering flushing, shadowing, tombstones, sparse indexing, crash windows, and recovery. |
| `tests/crash_writer.py` | Subprocess writer that writes keys continuously with `memtable_limit_bytes=2048`, printing acknowledged writes after fsync. |
| `tests/crash_test.py` | Durability harness killing the writer mid-flight across 10 rounds to prove zero data loss and check .sst file creation. |

---

## 2. Architecture & Data Flow

```
                      User Request
                           │
                 put() / delete() / get()
                           │
                           ▼
                  Strata (engine.py)
                 ┌─────────┴─────────┐
          Write Path                 Read Path
                 │                   1. Check MemTable (O(1))
                 ▼                      - found value -> return it
         1. WAL Append                  - found tombstone -> return None (STOP)
            (fsync to disk)             - missing -> continue to SSTables
                 │
                 ▼                   2. Check SSTables (Newest to Oldest)
         2. MemTable Update             - for sst in [000003, 000002, 000001]:
                 │                        * range check [first_key, last_key]
                 ▼                        * bisect sparse index -> seek offset
            Size >= Limit?                * read <= 16 sorted records
             ├─ No  -> return             * if found value -> return it
             └─ Yes -> flush()            * if found tombstone -> return None (STOP)
                         │           3. Return None (not found anywhere)
                         ▼
                 [ flush() sequence ]
                 1. write_sstable(.tmp -> .sst)
                 2. Prepend SSTable to list
                 3. wal.reset() & fresh MemTable
```

---

## 3. SSTable File Format & Sparse Indexing

### On-Disk Format
Each SSTable (`000001.sst`, `000002.sst`, ...) is an immutable text file where each line is formatted by `strata/record.py`:

```
<crc32 hex 8 chars> {"k": "key", "v": "value"}\n
<crc32 hex 8 chars> {"k": "deleted_key", "v": null}\n
```

- **Sorted**: Records are written strictly ascending by key.
- **Deduplicated**: Every key appears at most once in a single SSTable.
- **Tombstones**: Deleted keys are written with `"v": null`.

### The Sparse Index (`INDEX_INTERVAL = 16`)
Instead of keeping every key in memory (a dense index), we store an in-memory index entry for **every 16th record**:

- `index_keys`: `["k0000", "k0016", "k0032", ...]`
- `index_offsets`: `[0, 384, 768, ...]` (exact byte offsets in the file)
- `first_key` and `last_key` bounding the entire file.

### How Point Lookups Work (`SSTable.get`)
1. **Range Check**: If `key < first_key` or `key > last_key`, return `(False, None)` immediately without touching disk.
2. **Binary Search (Bisect)**: `pos = bisect.bisect_right(index_keys, key) - 1`. This finds the largest indexed key $\le$ target key.
3. **Seek & Scan**: Seek to `index_offsets[pos]`. Read sequentially line by line:
   - If line's key equals target $\to$ return `(True, value)`.
   - If line's key is greater than target $\to$ stop immediately and return `(False, None)`.
   - If EOF is reached $\to$ return `(False, None)`.

**Why this is efficient**: Because the data on disk is sorted, the key *cannot* appear after a record with a larger key. Therefore, we read at most 16 records from disk per lookup.

---

## 4. Atomic SSTable Writes

When flushing to disk, writing directly to `000001.sst` would mean a crash mid-write leaves a corrupted or half-written file with a valid `.sst` extension.

To prevent this:
1. Write records to `000001.sst.tmp`.
2. `f.flush()` and `os.fsync(f.fileno())` to guarantee file content is physically on disk.
3. `os.replace(tmp_path, sst_path)`: An atomic rename provided by the OS filesystem directory table. The file appears under its permanent name instantly and completely.
4. `_fsync_directory(data_dir)`: Best-effort directory fsync so the directory entry rename itself survives sudden power loss (wrapped in `try/except` for Windows support).

---

## 5. Flush Ordering & Crash Windows

The order of operations in `Strata.flush()` is:
1. Write MemTable to the next `.sst` file atomically.
2. Prepend the new `SSTable` instance to `self._sstables` (newest first).
3. Reset the WAL (`wal.reset()`, truncating to 0 bytes and fsyncing) and instantiate an empty `MemTable`.

### Crash Windows Analysis

| Crash Point | State on Disk | Recovery Behavior | Outcome |
|-------------|---------------|-------------------|---------|
| **Window A: Crash before Step 1 finishes** | Stale `.tmp` file exists; no `.sst` created; WAL is intact. | Startup deletes `*.tmp`. Replays full WAL into MemTable. | **Zero data lost.** |
| **Window B: Crash after Step 1/2, before Step 3** | New `.sst` exists; WAL still holds unflushed records. | Startup loads `.sst`, then replays WAL into MemTable. | Replay overwrites SSTable data idempotently with identical/newer data. **Zero data lost.** |
| **Window C: Crash after Step 3** | New `.sst` exists; WAL is empty; fresh MemTable. | Startup loads `.sst`; WAL replay is a clean no-op. | **Zero data lost.** |

---

## 6. Multi-Level Reads & Tombstone Shadowing

When `db.get(key)` is called:
1. Look in the active `MemTable`. If found (even if value is `None`), return it immediately.
2. If not found, iterate through `_sstables` from newest to oldest (`000003.sst`, `000002.sst`, `000001.sst`). Stop at the first table where `sstable.get(key)` returns `found=True`.
3. If not found in any SSTable, return `None`.

### Why Tombstones MUST Stop the Search
In an LSM-tree, updates and deletes do not modify older files. If key `"user"` existed in `000001.sst` and was deleted in `000002.sst`, `000002.sst` holds a tombstone (`{"k": "user", "v": null}`).

If the read path did not stop on a tombstone, it would see `None`, keep looking, and return the stale value from `000001.sst` — resurrecting deleted data!

---

## 7. Startup Recovery Sequence

When opening `Strata(data_dir)`:
1. **Clean Stale Files**: Scan `data_dir` and delete any leftover `*.tmp` files from interrupted flushes.
2. **Discover SSTables**: Find all `*.sst` files and sort descending by their numerical sequence number (`000003.sst` before `000002.sst`).
3. **Initialize Next Sequence**: Set `next_sst_id = max(existing_ids) + 1` (or `1` if no tables exist).
4. **Replay WAL**: Replay all valid records from `wal.log` into the active `MemTable`.

---

## 8. Decisions Not Specified in Prompt

Here is every implementation decision made to complete the requirements:

1. **Default Memtable Flush Threshold**: Set to $4\text{ MB}$ (`DEFAULT_MEMTABLE_LIMIT_BYTES = 4 * 1024 * 1024`).
2. **SSTable Sequence Number Formatting**: Used 6-digit zero-padding (`f"{id:06d}.sst"`), parsed via `int(filename[:-4])` to ensure correct numerical order up to 999,999 files.
3. **SSTable Metadata Properties**: Provided both `.file_size` and `.file_size_bytes` on `SSTable` for compatibility.
4. **`stats()` Dictionary Schema**: Standardized keys to:
   - `memtable_keys`: count of keys in the memtable (`len(self._mem)`).
   - `memtable_bytes`: approximate bytes in memtable (`self._mem.size_bytes`).
   - `sstable_count`: number of active SSTables (`len(self._sstables)`).
   - `sstables`: list of dicts with `name`, `record_count`, `file_size_bytes`, `first_key`, `last_key`.
5. **`MemTable.__len__`**: Added `__len__` returning `len(self._data)` to provide clean key counting without exposing internals.
6. **Empty MemTable Flush Protection**: If `len(self._mem) == 0`, `flush()` exits immediately, preventing zero-record or empty `.sst` files.
7. **Safe Directory Fsync**: Wrapped `os.open` and `os.fsync` on directory paths in `try ... finally: os.close(fd)` with an outer `try ... except OSError` for Windows compatibility.
8. **Point Lookup Search Loop**: Range-checks `[first_key, last_key]` before seeking; scans sequentially within the sparse index block until finding the key, encountering a larger key, or reaching EOF.
9. **Directory Filtering**: Explicitly filters by `.endswith(".sst")` and `.endswith(".tmp")`, ignoring unrelated files like `wal.log` or `.DS_Store`.
10. **`Strata.close()` Behavior**: Flushes and closes the WAL without forcing an unneeded SSTable flush, relying on WAL replay on next startup.

---

## 9. Interview Questions & Answers

**Q1: What is an SSTable and why is it immutable?**
An SSTable (Sorted String Table) is an on-disk file containing key-value pairs sorted by key. Immutability eliminates concurrency issues (no file locking or in-place overwrites), makes crash recovery straightforward (files are never half-modified), and allows sequential streaming during compaction.

**Q2: Why use a sparse index instead of indexing every key?**
A dense index holding every key in memory consumes large amounts of RAM as data grows. With a sparse index of 1 key every 16 records, memory usage drops by $94\%$, while lookups only require scanning at most 16 consecutive records from disk, which is fast due to sequential read speeds.

**Q3: How do you handle a crash while writing an SSTable?**
We write to a `.tmp` file, flush and fsync, and then use `os.replace` to atomically rename it to `.sst`. If a crash occurs before the rename finishes, only an incomplete `.tmp` file exists, which startup recovery deletes. The WAL still contains all mutations, so nothing is lost.

**Q4: Why does a tombstone in the MemTable stop the search rather than letting it check SSTables?**
In an LSM-tree, deletions are written as tombstones. If a tombstone did not stop the search, the read would fall through to older SSTables where the key previously existed with an old value, resurrecting the deleted data.

**Q5: What happens if the system crashes after an SSTable is created but before the WAL is reset?**
On restart, the engine loads the new SSTable, and then replays the WAL into the MemTable. Because the replayed operations are identical to or newer than what was in the SSTable, overwriting the state is completely idempotent. No data is duplicated or lost.
