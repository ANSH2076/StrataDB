"""Strata — the public API for the database.

Usage:
    with Strata("my_data") as db:
        db.put("name", "Alice")
        print(db.get("name"))   # "Alice"
        db.delete("name")
        print(db.get("name"))   # None
"""

from __future__ import annotations

import collections
import os
import time
from typing import Any, Deque, Dict, List, Optional

from strata.compaction import merge_tables
from strata.manifest import read_manifest, write_manifest
from strata.memtable import MemTable
from strata.sstable import SSTable, write_sstable
from strata.wal import WriteAheadLog

WAL_FILENAME = "wal.log"
DEFAULT_MEMTABLE_LIMIT_BYTES = 4 * 1024 * 1024  # 4 MB default flush threshold
DEFAULT_COMPACTION_THRESHOLD = 4               # Compact when >= 4 SSTables


class Strata:
    """A key-value store backed by a WAL, MemTable, immutable SSTables, and compaction.

    Args:
        data_dir: Directory where the database files live. Created if
                  it does not exist.
        sync: If True, every write is fsync'd to disk. Slower but crash-safe.
        memtable_limit_bytes: In-memory byte threshold before the memtable is
                              automatically flushed to a new SSTable on disk.
        use_bloom: If True, uses in-memory Bloom filters to avoid disk reads
                   for absent keys.
        compaction_threshold: Number of SSTables that triggers auto compaction.
        auto_compact: Whether to trigger compaction automatically after flushes.
    """

    def __init__(
        self,
        data_dir: str,
        sync: bool = True,
        memtable_limit_bytes: int = DEFAULT_MEMTABLE_LIMIT_BYTES,
        use_bloom: bool = True,
        compaction_threshold: int = DEFAULT_COMPACTION_THRESHOLD,
        auto_compact: bool = True,
    ) -> None:
        self._data_dir = data_dir
        self._sync = sync
        self._memtable_limit_bytes = memtable_limit_bytes
        self.memtable_limit_bytes = memtable_limit_bytes
        self.use_bloom = use_bloom
        self.auto_compact = auto_compact
        self._compaction_threshold = compaction_threshold

        # Performance counters
        self._bloom_skips: int = 0
        self._disk_lookups: int = 0
        self._false_positives: int = 0
        self._compactions_count: int = 0

        # Event log
        self._next_event_id: int = 1
        self.events: Deque[Dict[str, Any]] = collections.deque(maxlen=500)
        self.recovery_info: Dict[str, Any] = {}

        os.makedirs(data_dir, exist_ok=True)

        wal_path = os.path.join(data_dir, WAL_FILENAME)
        self._wal = WriteAheadLog(wal_path, sync=sync)
        self._mem = MemTable()
        self._sstables: List[SSTable] = []  # Ordered newest-to-oldest in memory
        self._next_sst_id: int = 1

        # Recover on-disk SSTables and replay un-flushed WAL records.
        self._recover()

    # -- Event Logging ---------------------------------------------------------

    def _record_event(self, event_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Append an event to the ring buffer and return it."""
        event = {
            "id": self._next_event_id,
            "time": time.time(),
            "type": event_type,
            **data,
        }
        self._next_event_id += 1
        self.events.append(event)
        return event

    def events_since(self, event_id: int) -> List[Dict[str, Any]]:
        """Return all events with id > event_id currently in the log buffer."""
        return [ev for ev in self.events if ev["id"] > event_id]

    # -- Recovery --------------------------------------------------------------

    def _recover(self) -> None:
        """Recover state from disk on startup.

        Order of operations:
          a) Delete leftover *.tmp files in data_dir (including MANIFEST.tmp
             and any *.sst.tmp from interrupted flushes or compactions).
          b) Read the MANIFEST. If it is None (fresh directory or legacy data),
             ADOPT every *.sst file sorted by number (oldest to newest) and
             write a MANIFEST.
          c) Otherwise load exactly the listed files. A listed file that is
             missing raises a clear FileNotFoundError.
          d) Delete any *.sst file that is NOT in the manifest (orphans left
             by a crash during flush or compaction).
          e) Compute next file id = 1 + the highest number among ALL .sst
             names seen BEFORE deleting orphans, so IDs are NEVER reused.
          f) THEN replay the WAL into the memtable as before.
        """
        t0 = time.perf_counter()
        tmp_removed = 0
        orphans_removed = 0

        # a) Clean up all *.tmp files.
        for fname in os.listdir(self._data_dir):
            if fname.endswith(".tmp"):
                try:
                    os.remove(os.path.join(self._data_dir, fname))
                    tmp_removed += 1
                except OSError:
                    pass

        # Helper to extract integer sequence from "000005.sst"
        def _sst_seq(filename: str) -> int:
            base = filename[:-4] if filename.endswith(".sst") else filename
            try:
                return int(base)
            except ValueError:
                return 0

        # Discover ALL .sst files currently on disk (before orphan cleanup).
        all_disk_sst_files = [
            f for f in os.listdir(self._data_dir)
            if f.endswith(".sst")
        ]

        # e) Compute next file id = 1 + max seen among ALL .sst files BEFORE orphan deletion.
        if all_disk_sst_files:
            highest_seen = max(_sst_seq(f) for f in all_disk_sst_files)
            self._next_sst_id = highest_seen + 1
        else:
            self._next_sst_id = 1

        # b) Read MANIFEST
        manifest_tables = read_manifest(self._data_dir)

        if manifest_tables is None:
            # Legacy or fresh directory: adopt all existing .sst files sorted oldest to newest.
            all_disk_sst_files.sort(key=_sst_seq)
            manifest_tables = list(all_disk_sst_files)
            write_manifest(self._data_dir, manifest_tables, sync=self._sync)

        # c) Load exactly the files listed in the manifest.
        # Manifest lists tables oldest-to-newest; in-memory self._sstables is newest-to-oldest!
        loaded_tables: List[SSTable] = []
        manifest_set = set(manifest_tables)

        # Check for missing files listed in the manifest
        for name in manifest_tables:
            path = os.path.join(self._data_dir, name)
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"MANIFEST lists SSTable {name!r}, but it does not exist on disk in {self._data_dir}"
                )

        # In-memory list is ordered NEWEST first (reversed manifest order)
        for name in reversed(manifest_tables):
            path = os.path.join(self._data_dir, name)
            loaded_tables.append(SSTable(path))

        self._sstables = loaded_tables

        # d) Delete any *.sst file that is NOT in the manifest (orphans).
        for f in all_disk_sst_files:
            if f not in manifest_set:
                try:
                    os.remove(os.path.join(self._data_dir, f))
                    orphans_removed += 1
                except OSError:
                    pass

        # f) Replay the WAL into the memtable.
        records = self._wal.replay()
        for record in records:
            op = record.get("op")
            if op == "put":
                self._mem.put(record["k"], record["v"])
            elif op == "del":
                self._mem.delete(record["k"])
            else:
                raise ValueError(f"Unknown WAL operation: {op!r}")

        duration_ms = (time.perf_counter() - t0) * 1000.0

        # Record recovery event and info
        self.recovery_info = {
            "wal_records_replayed": len(records),
            "damaged_bytes_truncated": self._wal.damaged_bytes_truncated,
            "sstables_loaded": len(self._sstables),
            "orphans_removed": orphans_removed,
            "tmp_removed": tmp_removed,
            "duration_ms": duration_ms,
        }
        self._record_event("recovery", self.recovery_info)

    # -- Flush -----------------------------------------------------------------

    def flush(self) -> None:
        """Flush the in-memory MemTable to a new sorted SSTable on disk.

        Order of operations:
          1. Write memtable.sorted_items() to the next numbered .sst file
             (write_sstable writes to .tmp, fsyncs, then atomically renames).
          2. Update the MANIFEST with the new file added to the list.
             *** THIS IS THE COMMIT POINT ***
          3. Open it as an SSTable and insert at the FRONT of self._sstables
             (newest first).
          4. Only AFTER the file and manifest are safely on disk, reset the
             WAL (truncate to empty and fsync) and replace the MemTable with a
             fresh empty one.
          5. Check auto_compact: if enabled and table count >= threshold,
             trigger compact().

        Crash windows analysis:
          - Crash BEFORE step 2 (manifest update):
            The .sst file exists on disk (or its .tmp is present), but the
            MANIFEST still lists only the old tables.  On startup, the unlisted
            .sst is treated as an orphan and deleted.  The WAL still has
            everything, so no acknowledged data is lost.
          - Crash AFTER step 2 but BEFORE step 4 (WAL reset):
            The MANIFEST already lists the new SSTable, but the WAL has not
            been truncated yet.  On restart, the SSTable is loaded, and then
            the WAL is replayed on top of it.  This is harmless and idempotent
            because the replayed writes are identical to (or newer than) the
            flushed SSTable records.  NO DATA IS LOST.
          - Crash AFTER step 4:
            The SSTable is in the manifest, the WAL is clean/empty, and the
            fresh MemTable is ready.  NO DATA IS LOST.
        """
        # Never flush an empty memtable.
        if len(self._mem) == 0:
            return

        sst_filename = f"{self._next_sst_id:06d}.sst"
        sst_path = os.path.join(self._data_dir, sst_filename)
        items = self._mem.sorted_items()
        records_count = len(items)

        # 1. Write sorted items to .sst atomically.
        write_sstable(sst_path, items, sync=self._sync)
        file_bytes = os.path.getsize(sst_path)

        # 2. Update MANIFEST (COMMIT POINT)
        # Manifest order is oldest-to-newest, so new SSTable is appended at the end.
        current_names_oldest_first = [s.name for s in reversed(self._sstables)]
        current_names_oldest_first.append(sst_filename)
        write_manifest(self._data_dir, current_names_oldest_first, sync=self._sync)

        # 3. Open and insert at front (newest first).
        sstable = SSTable(sst_path)
        self._sstables.insert(0, sstable)
        self._next_sst_id += 1

        # 4. Safely reset the WAL and start a fresh empty MemTable.
        self._wal.reset()
        self._mem = MemTable()

        # Log flush event
        self._record_event("flush", {
            "sstable": sst_filename,
            "records": records_count,
            "bytes": file_bytes,
        })

        # 5. Auto-compaction check
        if self.auto_compact and len(self._sstables) >= self._compaction_threshold:
            self.compact()

    # -- Compaction ------------------------------------------------------------

    def compact(self) -> Optional[Dict[str, Any]]:
        """Merge ALL existing SSTables into a single sorted SSTable.

        If there are fewer than 2 SSTables, returns None.

        Order of operations:
          1. Stream merge_tables(self._sstables, stats) into a new numbered
             .sst file using the existing atomic write_sstable().
             (If zero records survive because all were deleted/tombstones,
             write no .sst file).
          2. Open the new table (if created).
          3. Write the MANIFEST listing ONLY the new table (or empty list []).
             *** THIS IS THE COMMIT POINT ***
          4. Swap the in-memory table list: self._sstables = [new_table] (or []).
          5. Delete the old obsolete files from disk (ignore FileNotFoundError).

        Crash windows analysis:
          - Crash BEFORE step 3 (manifest update):
            The merged file (or its .tmp) exists on disk, but the MANIFEST
            still lists the old tables.  On startup, the uncommitted merged file
            is detected as an orphan and deleted.  The old tables and the WAL
            remain intact, exactly as before compaction.  NO DATA IS LOST.
          - Crash AFTER step 3 but BEFORE step 5 (deleting old files):
            The MANIFEST lists ONLY the new merged table.  The old tables are
            still sitting on disk.  On startup, recovery loads only the merged
            table from the manifest, and deletes the old unlisted tables as
            orphans.  Because the old files are purged, deleted keys can NEVER
            come back to life.  NO DATA IS LOST.
        """
        if len(self._sstables) < 2:
            return None

        t0 = time.perf_counter()
        old_tables = list(self._sstables)  # Newest to oldest
        input_names = [t.name for t in old_tables]
        bytes_in = sum(t.file_size_bytes for t in old_tables)

        merge_stats: Dict[str, Any] = {}
        merged_generator = merge_tables(old_tables, merge_stats)

        # 1. Stream merge into new .sst file
        new_sst_name: Optional[str] = None
        new_sst_path: Optional[str] = None
        new_table: Optional[SSTable] = None
        bytes_out = 0

        # We need to know if any records survive. materialize generator into items
        surviving_items = list(merged_generator)
        records_out = len(surviving_items)

        if records_out > 0:
            new_sst_name = f"{self._next_sst_id:06d}.sst"
            new_sst_path = os.path.join(self._data_dir, new_sst_name)
            self._next_sst_id += 1

            write_sstable(new_sst_path, surviving_items, sync=self._sync)
            bytes_out = os.path.getsize(new_sst_path)

            # 2. Open the new table
            new_table = SSTable(new_sst_path)
            new_manifest_list = [new_sst_name]
        else:
            # Zero records survived (all keys had tombstones)
            new_manifest_list = []

        # 3. Write MANIFEST (COMMIT POINT)
        write_manifest(self._data_dir, new_manifest_list, sync=self._sync)

        # 4. Swap in-memory table list
        if new_table is not None:
            self._sstables = [new_table]
        else:
            self._sstables = []

        # 5. Delete old files
        for tbl in old_tables:
            try:
                os.remove(tbl.path)
            except FileNotFoundError:
                pass
            except OSError:
                pass

        self._compactions_count += 1
        duration_ms = (time.perf_counter() - t0) * 1000.0

        summary = {
            "inputs": input_names,
            "output": new_sst_name,
            "records_in": merge_stats["records_in"],
            "records_out": records_out,
            "overwritten_dropped": merge_stats["overwritten_dropped"],
            "tombstones_dropped": merge_stats["tombstones_dropped"],
            "bytes_in": bytes_in,
            "bytes_out": bytes_out,
            "duration_ms": duration_ms,
        }

        # Log compaction event
        self._record_event("compaction", summary)

        return summary

    # -- Public API ------------------------------------------------------------

    def put(self, key: str, value: str) -> None:
        """Store a key-value pair.

        We write to the WAL FIRST, then update the memtable.
        If the memtable exceeds memtable_limit_bytes, it is flushed.
        """
        _validate_key(key)
        _validate_value(value)
        self._wal.append({"op": "put", "k": key, "v": value})
        self._mem.put(key, value)
        if self._mem.size_bytes >= self._memtable_limit_bytes:
            self.flush()

    def delete(self, key: str) -> None:
        """Delete a key (writes a tombstone).

        Same WAL-first order as put(), for the same reason.
        If the memtable exceeds memtable_limit_bytes, it is flushed.
        """
        _validate_key(key)
        self._wal.append({"op": "del", "k": key})
        self._mem.delete(key)
        if self._mem.size_bytes >= self._memtable_limit_bytes:
            self.flush()

    def _lookup(
        self,
        key: str,
        trace: Optional[List[Dict[str, str]]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Internal lookup shared by get() and trace_get() so they can never disagree.

        When trace is None:
          - Regular read path.
          - Updates engine performance counters (bloom_skips, disk_lookups, false_positives).

        When trace is a list:
          - Tracing mode.
          - Appends step dictionaries to trace list.
          - Does NOT modify engine counters.

        Returns (found, value):
          - (True, "str")  -> Key found with value.
          - (True, None)   -> Tombstone found (key was deleted).
          - (False, None)  -> Key not found anywhere.
        """
        _validate_key(key)
        is_tracing = trace is not None

        # 1. Check MemTable
        found, val = self._mem.get(key)
        if found:
            if val is not None:
                if is_tracing:
                    trace.append({
                        "where": "memtable",
                        "action": "hit",
                        "detail": f"Found key in memory with value: {val!r}",
                    })
                return (True, val)
            else:
                # Tombstone in MemTable
                if is_tracing:
                    trace.append({
                        "where": "memtable",
                        "action": "tombstone",
                        "detail": "Found tombstone in memory (key deleted)",
                    })
                return (True, None)
        else:
            if is_tracing:
                trace.append({
                    "where": "memtable",
                    "action": "miss",
                    "detail": "Key not present in memory",
                })

        # 2. Check SSTables from newest to oldest
        for sstable in self._sstables:
            sst_name = sstable.name

            # Step a: Range check
            if sstable.record_count == 0:
                if is_tracing:
                    trace.append({
                        "where": sst_name,
                        "action": "range_skip",
                        "detail": "Table is empty",
                    })
                continue

            if key < sstable.first_key or key > sstable.last_key:
                if is_tracing:
                    trace.append({
                        "where": sst_name,
                        "action": "range_skip",
                        "detail": f"Key outside bounds [{sstable.first_key!r} .. {sstable.last_key!r}]",
                    })
                continue

            # Step b: Bloom filter check
            if self.use_bloom and not sstable.might_contain(key):
                if not is_tracing:
                    self._bloom_skips += 1
                else:
                    trace.append({
                        "where": sst_name,
                        "action": "bloom_skip",
                        "detail": "Bloom filter says definitely absent (disk read saved)",
                    })
                continue

            # Step c: Real disk read
            if not is_tracing:
                self._disk_lookups += 1

            found, val = sstable.get(key)
            if found:
                if val is not None:
                    if is_tracing:
                        trace.append({
                            "where": sst_name,
                            "action": "disk_read_hit",
                            "detail": f"Read from disk; found value: {val!r}",
                        })
                    return (True, val)
                else:
                    # Tombstone in SSTable
                    if is_tracing:
                        trace.append({
                            "where": sst_name,
                            "action": "tombstone",
                            "detail": "Read from disk; found tombstone (key deleted)",
                        })
                    return (True, None)
            else:
                # Key was inside range and passed bloom filter, but was not on disk!
                if self.use_bloom:
                    if not is_tracing:
                        self._false_positives += 1
                    else:
                        trace.append({
                            "where": sst_name,
                            "action": "disk_read_miss",
                            "detail": "Bloom filter false positive; read from disk but key was absent",
                        })
                else:
                    if is_tracing:
                        trace.append({
                            "where": sst_name,
                            "action": "disk_read_miss",
                            "detail": "Read from disk; key was absent",
                        })

        # Not found in any level
        return (False, None)

    def get(self, key: str) -> Optional[str]:
        """Return the value for a key, or None if missing or deleted."""
        found, val = self._lookup(key, trace=None)
        if found:
            return val
        return None

    def trace_get(self, key: str) -> Dict[str, Any]:
        """Trace the exact lookup path for a key across MemTable and SSTables.

        Does NOT update performance counters (bloom_skips, disk_lookups, false_positives).
        Guaranteed to return the exact same outcome as get(key).

        Returns:
            {
                "key": key,
                "found": bool,
                "value": Optional[str],
                "steps": list of step dicts
            }
        """
        steps: List[Dict[str, str]] = []
        found, val = self._lookup(key, trace=steps)
        return {
            "key": key,
            "found": found and (val is not None),
            "value": val,
            "steps": steps,
        }

    def _abandon(self) -> None:
        """Close only the WAL file handle with NO flush of the memtable and no cleanup.

        Simulates an abrupt process crash for testing crash recovery.
        """
        self._wal.close()


    def stats(self) -> Dict[str, Any]:
        """Return database statistics for monitoring and Stage 5 dashboard."""
        total_sst_bytes = sum(s.file_size_bytes for s in self._sstables)
        return {
            "memtable_keys": len(self._mem),
            "memtable_bytes": self._mem.size_bytes,
            "sstable_count": len(self._sstables),
            "bloom_enabled": self.use_bloom,
            "bloom_skips": self._bloom_skips,
            "disk_lookups": self._disk_lookups,
            "false_positives": self._false_positives,
            "compactions": self._compactions_count,
            "compaction_threshold": self._compaction_threshold,
            "auto_compact": self.auto_compact,
            "wal_bytes": self._wal.file_size_bytes,
            "wal_records": self._wal.record_count,
            "total_sstable_bytes": total_sst_bytes,
            "sstables": [
                {
                    "name": sst.name,
                    "record_count": sst.record_count,
                    "file_size_bytes": sst.file_size_bytes,
                    "first_key": sst.first_key,
                    "last_key": sst.last_key,
                }
                for sst in self._sstables
            ],
        }

    def close(self) -> None:
        """Close the database, flushing the WAL."""
        self._wal.close()

    # -- Context manager -------------------------------------------------------

    def __enter__(self) -> "Strata":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return None  # do not suppress exceptions


# -- Validation helpers --------------------------------------------------------

def _validate_key(key: object) -> None:
    """Raise TypeError / ValueError for bad keys."""
    if not isinstance(key, str):
        raise TypeError(f"Key must be a string, got {type(key).__name__}")
    if len(key) == 0:
        raise ValueError("Key must not be empty")


def _validate_value(value: object) -> None:
    """Raise TypeError / ValueError for bad values."""
    if not isinstance(value, str):
        raise TypeError(f"Value must be a string, got {type(value).__name__}")
    if len(value) == 0:
        raise ValueError("Value must not be empty")
