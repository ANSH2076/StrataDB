# Stage 4 — Learning Notes: Compaction, MANIFEST, and Crash Safety

These notes cover Stage 4 of Strata: implementing an on-disk **MANIFEST**
for atomic state transitions, **compaction** to merge SSTables and drop
obsolete data, and an in-memory **event log** for monitoring.

---

## 1. What Compaction Does and Why

In an LSM-tree (Log-Structured Merge-tree):
- Every `put` and `delete` is appended sequentially to disk without modifying existing files.
- Over time, SSTables accumulate. A single key might exist in multiple SSTables if it was updated or deleted.
- Having many SSTables hurts read performance (reads must search multiple files in age order) and wastes disk space storing obsolete data.

**Compaction** solves this by:
1. Merging multiple sorted SSTables into a single sorted SSTable.
2. Dropping older, shadowed versions of overwritten keys.
3. Permanently removing tombstones (delete markers).
4. Reclaiming disk space and reducing the number of files to search.

---

## 2. K-Way Merge with Age Ranking

Because each SSTable is already sorted by key ascending, merging $K$ SSTables can be done efficiently in $O(N \log K)$ time using a min-heap (`heapq.merge`), where $N$ is the total number of records.

### How Age Ranking Works
We pass SSTables in **newest-first** order:
- Index 0 is the newest table.
- Index 1 is the second newest, etc.

Each table's iterator yields tagged tuples: `(key, age_rank, value)`.
Tuples sort lexicographically:
1. `key` (ascending: `"a"`, `"b"`, `"c"`, ...)
2. `age_rank` (ascending: `0` before `1`)

For any duplicate key, the entry from `age_rank = 0` (the newest table) is guaranteed to appear **first** in the merged stream!

### Tiny Worked Example ($K = 2$ tables)

Suppose we have two SSTables:

- **Table 0 (Newer)**:
  - `("apple", "honeycrisp")`
  - `("banana", None)` *(tombstone)*
- **Table 1 (Older)**:
  - `("apple", "gala")`
  - `("banana", "yellow")`
  - `("cherry", "bing")`

#### Merged Stream:
1. `("apple", 0, "honeycrisp")` $\to$ First time seeing `"apple"`. **Yield `("apple", "honeycrisp")`**.
2. `("apple", 1, "gala")` $\to$ `key == last_key`. Older version! **Drop as overwritten**.
3. `("banana", 0, None)` $\to$ First time seeing `"banana"`. Value is `None` (tombstone). **Drop as tombstone**.
4. `("banana", 1, "yellow")` $\to$ `key == last_key`. Older version! **Drop as overwritten**.
5. `("cherry", 1, "bing")` $\to$ First time seeing `"cherry"`. **Yield `("cherry", "bing")`**.

#### Output:
Only `("apple", "honeycrisp")` and `("cherry", "bing")`.
`"banana"` is completely gone, and `"gala"` was discarded.

---

## 3. Why Tombstones Can ONLY Be Dropped in a Full Merge

A tombstone (`{"k": key, "v": null}`) is a marker that says:
> *"This key was deleted. Ignore any older values in older tables."*

If you ran a **partial compaction** (merging only Table 3 and Table 2, but leaving older Table 1 untouched):
- If you dropped the tombstone in Table 2, a subsequent read for that key would fall through to Table 1 and find the old pre-deletion value!
- **The deleted key would be resurrected.**

In Strata Stage 4, compaction always merges **ALL** SSTables:
1. Nothing older exists below the tables being merged.
2. The MemTable is strictly *newer* than every SSTable.
Therefore, if a tombstone is the newest entry across all SSTables, no older version can possibly exist anywhere in the database. Discarding the tombstone is 100% safe.

---

## 4. The MANIFEST: Atomic State Transitions

### Why Directory Listings are Unsafe for Crash Recovery
If a database reboots and simply scans `data_dir` for `*.sst` files:
- It cannot distinguish between:
  - An SSTable created by a compaction that finished.
  - A partially written SSTable from a compaction that crashed.
  - An old SSTable that was supposed to be deleted after compaction.
- If a crash occurs after writing a compacted file but before deleting the old ones, scanning the directory would load *both* the old and new files, duplicating data or corrupting age ordering.

### The MANIFEST Solution
A file named `MANIFEST` contains the exact JSON list of live files in age order:
```json
{"sstables": ["000001.sst", "000004.sst"]}
```

- **Atomic Writes**: Written to `MANIFEST.tmp`, flushed, fsync'd, and atomically replaced (`os.replace`) into `MANIFEST`.
- **Single Source of Truth**: The `MANIFEST` is the atomic **commit point**. A file is only real if it appears in the manifest.
- **Orphan Cleanup**: On startup, any `.sst` file on disk not listed in `MANIFEST` is deleted as an orphan.

---

## 5. Crash Windows and Recovery Proofs

### Compaction Crash Windows

```
Step 1: Write merged SSTable (atomic via .tmp)
Step 2: Open new SSTable
Step 3: Write MANIFEST listing ONLY new SSTable  <-- COMMIT POINT
Step 4: Swap in-memory tables
Step 5: Delete old SSTables from disk
```

| Crash Window | State on Disk | Recovery Action | Outcome |
|:---|:---|:---|:---|
| **Crash before Step 3** | Merged `.sst` file exists on disk, but `MANIFEST` still lists the old tables. | Startup reads `MANIFEST`, sees old tables, and deletes the unlisted merged file as an orphan. | **No data lost.** The database state remains exactly as it was before compaction. |
| **Crash after Step 3** | `MANIFEST` lists only the new merged file. Old `.sst` files still exist on disk. | Startup reads `MANIFEST`, loads only the merged file, and deletes the unlisted old files as orphans. | **No data lost.** Deletions and overwrites remain permanent. Deleted keys cannot reappear. |

### Flush Crash Windows

```
Step 1: Write memtable.sorted_items() to new .sst
Step 2: Update MANIFEST adding new .sst        <-- COMMIT POINT
Step 3: Open and insert SSTable in-memory
Step 4: Reset WAL (truncate to 0 and fsync)
```

| Crash Window | State on Disk | Recovery Action | Outcome |
|:---|:---|:---|:---|
| **Crash before Step 2** | New `.sst` is on disk, but `MANIFEST` still has old list. WAL has all writes. | Startup deletes unlisted `.sst` as an orphan. Replays WAL into MemTable. | **No data lost.** All writes recovered from WAL. |
| **Crash after Step 2** | `MANIFEST` has new `.sst`. WAL has not been truncated yet. | Startup loads new `.sst` from `MANIFEST`. Replays WAL into MemTable. | Replayed writes overwrite SSTable with identical data (idempotent). **No data lost.** |

---

## 6. Real Benchmark Results (`tests/bench_reads.py`)

Benchmark running 5,000 lookups across 20,000 keys (with 1,841 overwrites and 500 deletes):

- **SSTables before compaction**: 50
- **SSTables after compaction**: 1
- **Compaction duration**: 318.14 ms
- **Records in**: 21,841 | **Records out**: 20,000
- **Overwrites dropped**: 1,841

| Configuration | Absent Keys Time (5,000) | Present Keys Time (5,000) | Notes |
|:---|:---:|:---:|:---|
| **Uncompacted (Bloom OFF)** | 0.8165 s | 0.5797 s | Must search through up to 50 files on disk |
| **Uncompacted (Bloom ON)** | 0.1093 s | 0.3378 s | 7.47x speedup for absent keys |
| **Compacted (Bloom ON)** | 0.0224 s | 0.2841 s | **1.19x faster present lookups, 4.88x faster absent lookups** |

---

## 7. Known Limitations

1. **Full Stop-the-World Merge**: Compaction rewrites all existing SSTables into one file. On large datasets, this causes write amplification and temporary I/O spikes.
2. **Synchronous Execution**: Compaction runs inline on the main thread during `flush()`, temporarily pausing client writes while files are merged.
3. **No Tiered/Leveled Compaction**: Production systems (like RocksDB or LevelDB) partition tables into levels ($L_0, L_1, L_2$) so compaction only merges small subsets of tables at a time.

---

## 8. Decisions Not Specified in Prompt

1. **Sequence ID Preservation**: Computed `self._next_sst_id = 1 + max(all_seen_ids)` before orphan deletion, ensuring monotonic file IDs are never reused even after crashes.
2. **Empty Surviving Records**: If all keys in all tables were tombstones, compaction outputs no file and writes `{"sstables": []}` to the manifest, leaving 0 SSTables.
3. **Directory Creation in `write_manifest`**: Added `os.makedirs(data_dir, exist_ok=True)` in `write_manifest` to prevent errors when initializing fresh or test directories.
4. **Event Log Structure**: Built `self.events` as a `collections.deque(maxlen=500)` storing dictionaries with incremental `id`, Unix `time`, and `type` (`"flush"`, `"compaction"`, `"recovery"`).
5. **Recovery Metrics**: Added `recovery_info` capturing `wal_records_replayed`, `damaged_bytes_truncated`, `sstables_loaded`, `orphans_removed`, `tmp_removed`, and `duration_ms`.
6. **Existing Tests Compatibility**: Kept all existing 34 Stage 1–3 tests intact; all 34 passed without requiring `auto_compact=False` modifications because existing tests operate below the threshold ($< 4$ flushes).

---

## 9. Five Interview Questions & Answers

**Q1: Why does an LSM-tree need compaction?**
Because LSM-trees append all writes and deletes to immutable files, obsolete data accumulates and SSTable counts grow. Compaction merges sorted files, removes overwritten versions and tombstones, and bounds read latency and disk usage.

**Q2: What is the purpose of the MANIFEST file?**
The MANIFEST is the atomic commit point for on-disk file state. It explicitly lists which SSTable files are active. Without it, the engine cannot safely determine whether files on disk belong to completed operations or interrupted crashes.

**Q3: Why can tombstones only be removed during a full compaction?**
In a partial compaction, an older unmerged SSTable might still hold an earlier value for the key. Dropping the tombstone would allow future reads to find that old value, resurrecting deleted data. When merging all tables, no older records exist, making tombstone removal safe.

**Q4: How does k-way merge sort work with age ranking?**
Each SSTable is already sorted by key. We tag each table's records with an `age_rank` (0 = newest) and use a min-heap (`heapq.merge`). For duplicate keys, the entry from the newest table emerges first, allowing the engine to yield the latest value and discard all subsequent duplicates.

**Q5: What happens if the database crashes halfway through writing a compacted SSTable?**
Because the MANIFEST has not been updated yet, the new compacted file is uncommitted. On restart, the engine reads the existing MANIFEST (which still references the old tables) and deletes the partially written file as an orphan. No data is lost or corrupted.
