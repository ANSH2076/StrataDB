"""Strata — the public API for the database.

Usage:
    with Strata("my_data") as db:
        db.put("name", "Alice")
        print(db.get("name"))   # "Alice"
        db.delete("name")
        print(db.get("name"))   # None
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from strata.memtable import MemTable
from strata.sstable import SSTable, write_sstable
from strata.wal import WriteAheadLog

WAL_FILENAME = "wal.log"
DEFAULT_MEMTABLE_LIMIT_BYTES = 4 * 1024 * 1024  # 4 MB default flush threshold


class Strata:
    """A key-value store backed by a WAL, MemTable, and immutable SSTables.

    Args:
        data_dir: Directory where the database files live.  Created if
                  it does not exist.
        sync:     If True, every write is fsync'd to disk.  Slower but
                  crash-safe.
        memtable_limit_bytes: In-memory byte threshold before the
                              memtable is automatically flushed to a
                              new SSTable on disk.
    """

    def __init__(
        self,
        data_dir: str,
        sync: bool = True,
        memtable_limit_bytes: int = DEFAULT_MEMTABLE_LIMIT_BYTES,
    ) -> None:
        self._data_dir = data_dir
        self._sync = sync
        self._memtable_limit_bytes = memtable_limit_bytes

        os.makedirs(data_dir, exist_ok=True)

        wal_path = os.path.join(data_dir, WAL_FILENAME)
        self._wal = WriteAheadLog(wal_path, sync=sync)
        self._mem = MemTable()
        self._sstables: List[SSTable] = []
        self._next_sst_id: int = 1

        # Recover on-disk SSTables and replay un-flushed WAL records.
        self._recover()

    # -- Recovery --------------------------------------------------------------

    def _recover(self) -> None:
        """Recover state from disk on startup.

        Order of operations:
          1. Delete leftover *.tmp files in data_dir (unfinished flushes
             interrupted by a crash).
          2. Load every *.sst file, sorted by file sequence number
             descending (newest first).  Age order matters: newest must
             be checked first so newer updates and tombstones shadow older data.
          3. Determine next SSTable number = highest existing + 1.
          4. THEN replay the WAL into the memtable as before to reconstruct
             all writes that happened after the last successful flush.
        """
        # 1. Clean up stale temporary files from interrupted writes.
        for fname in os.listdir(self._data_dir):
            if fname.endswith(".tmp"):
                try:
                    os.remove(os.path.join(self._data_dir, fname))
                except OSError:
                    pass

        # 2. Find and load all existing *.sst files.
        sst_files: List[str] = [
            f for f in os.listdir(self._data_dir)
            if f.endswith(".sst")
        ]

        def _sst_seq(filename: str) -> int:
            base = filename[:-4]
            try:
                return int(base)
            except ValueError:
                return 0

        # Sort descending by sequence number: e.g. 000003.sst before 000001.sst.
        sst_files.sort(key=_sst_seq, reverse=True)

        self._sstables = [
            SSTable(os.path.join(self._data_dir, f))
            for f in sst_files
        ]

        # 3. Next file number = highest existing + 1 (starts at 1).
        if sst_files:
            self._next_sst_id = max(_sst_seq(f) for f in sst_files) + 1
        else:
            self._next_sst_id = 1

        # 4. Replay the WAL into the memtable.
        records = self._wal.replay()
        for record in records:
            op = record.get("op")
            if op == "put":
                self._mem.put(record["k"], record["v"])
            elif op == "del":
                self._mem.delete(record["k"])
            else:
                # An unknown op means the WAL is damaged or was written
                # by a newer version we don't understand.  Fail loudly
                # so the problem is noticed immediately.
                raise ValueError(
                    f"Unknown WAL operation: {op!r}"
                )

    # -- Flush -----------------------------------------------------------------

    def flush(self) -> None:
        """Flush the in-memory MemTable to a new sorted SSTable on disk.

        The order of operations is critical for crash safety:
          1. Write memtable.sorted_items() to the next numbered .sst file
             (write_sstable writes to .tmp, fsyncs, then atomically renames).
          2. Open it as an SSTable and insert at the FRONT of self._sstables
             (newest first).
          3. Only AFTER the file is safely on disk, reset the WAL (truncate
             to empty and fsync) and replace the MemTable with a fresh empty one.

        Crash windows analysis:
          - Crash BEFORE step 1 finishes:
            The .tmp file is incomplete; no .sst file was created.
            The WAL is intact and contains all mutations.  On recovery,
            the leftover .tmp is deleted and the WAL is replayed into
            the MemTable.  NO DATA IS LOST.
          - Crash AFTER step 1/2 but BEFORE step 3 completes:
            The SSTable is already safely on disk, but the WAL has not been
            reset yet.  On restart, the engine loads the SSTable, then
            replays the WAL into the MemTable.  Replaying the WAL entries
            on top of the SSTable is harmless and idempotent because the
            replayed values are identical to (or newer than) what was
            persisted in the SSTable.  NO DATA IS LOST.
          - Crash AFTER step 3:
            The SSTable is on disk, WAL is clean/empty, and the fresh
            MemTable starts clean.  NO DATA IS LOST.
        """
        # Never flush an empty memtable.
        if len(self._mem) == 0:
            return

        sst_filename = f"{self._next_sst_id:06d}.sst"
        sst_path = os.path.join(self._data_dir, sst_filename)

        # 1. Write sorted items to .sst atomically.
        write_sstable(sst_path, self._mem.sorted_items(), sync=self._sync)

        # 2. Open and insert at front (newest first).
        sstable = SSTable(sst_path)
        self._sstables.insert(0, sstable)
        self._next_sst_id += 1

        # 3. Safely reset the WAL and start a fresh empty MemTable.
        self._wal.reset()
        self._mem = MemTable()

    # -- Public API ------------------------------------------------------------

    def put(self, key: str, value: str) -> None:
        """Store a key-value pair.

        We write to the WAL FIRST, then update the memtable.  Why?
        If we updated the memtable first and crashed before the WAL
        write, the in-memory change would be lost and the WAL would
        have no record of it — so the write silently disappears.
        By writing the WAL first, any write that the caller saw
        complete is guaranteed to be on disk and recoverable.

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

    def get(self, key: str) -> Optional[str]:
        """Return the value for a key, or None if missing or deleted.

        Search order:
          1. In-memory MemTable (most recent mutations).
          2. SSTables from newest to oldest.

        Why tombstones must stop the search:
          If a key is found with value None (a tombstone), that means the
          key was explicitly deleted in that generation.  We MUST return
          None immediately without inspecting older SSTables.  If we kept
          searching older SSTables, an older non-deleted value would be
          found and resurrect the deleted key!
        """
        _validate_key(key)

        # Check MemTable first.
        found, value = self._mem.get(key)
        if found:
            return value

        # Check SSTables newest to oldest.
        for sstable in self._sstables:
            found, value = sstable.get(key)
            if found:
                return value

        # Not found anywhere.
        return None

    def stats(self) -> Dict[str, Any]:
        """Return database statistics for monitoring and Stage 5 dashboard."""
        return {
            "memtable_keys": len(self._mem),
            "memtable_bytes": self._mem.size_bytes,
            "sstable_count": len(self._sstables),
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
