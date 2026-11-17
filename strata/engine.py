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
from typing import Optional

from strata.memtable import MemTable
from strata.wal import WriteAheadLog

WAL_FILENAME = "wal.log"


class Strata:
    """A simple key-value store backed by a WAL and an in-memory table.

    Args:
        data_dir: Directory where the database files live.  Created if
                  it does not exist.
        sync:     If True, every write is fsync'd to disk.  Slower but
                  crash-safe.
    """

    def __init__(self, data_dir: str, sync: bool = True) -> None:
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        wal_path = os.path.join(data_dir, WAL_FILENAME)
        self._wal = WriteAheadLog(wal_path, sync=sync)
        self._mem = MemTable()

        # Rebuild the memtable from the WAL.  This is how we recover
        # after a crash — every acknowledged write is in the WAL, so
        # replaying it reconstructs the exact pre-crash state.
        self._recover()

    # -- Recovery --------------------------------------------------------------

    def _recover(self) -> None:
        """Replay the WAL to rebuild the memtable."""
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

    # -- Public API ------------------------------------------------------------

    def put(self, key: str, value: str) -> None:
        """Store a key-value pair.

        We write to the WAL FIRST, then update the memtable.  Why?
        If we updated the memtable first and crashed before the WAL
        write, the in-memory change would be lost and the WAL would
        have no record of it — so the write silently disappears.
        By writing the WAL first, any write that the caller saw
        complete is guaranteed to be on disk and recoverable.
        """
        _validate_key(key)
        _validate_value(value)
        self._wal.append({"op": "put", "k": key, "v": value})
        self._mem.put(key, value)

    def delete(self, key: str) -> None:
        """Delete a key (writes a tombstone).

        Same WAL-first order as put(), for the same reason.
        """
        _validate_key(key)
        self._wal.append({"op": "del", "k": key})
        self._mem.delete(key)

    def get(self, key: str) -> Optional[str]:
        """Return the value for a key, or None if missing or deleted.

        In Stage 2, if the memtable says "not found" we would also
        check SSTables on disk.  For now the memtable is the only
        source of truth.
        """
        _validate_key(key)
        found, value = self._mem.get(key)
        if found:
            return value  # None here means "deleted" (tombstone)
        return None

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
