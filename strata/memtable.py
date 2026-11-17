"""In-memory table (MemTable) for Strata.

The memtable holds all recent writes in a plain Python dict.
In a full LSM-tree (Stage 2+), the memtable is periodically flushed
to an on-disk sorted file (SSTable).  For now it just lives in memory
and is rebuilt from the WAL on startup.
"""

from __future__ import annotations

from typing import List, Optional, Tuple


# We use None as the value to represent a *tombstone* — a marker that
# says "this key was deleted".  We need tombstones (instead of just
# removing the key from the dict) because in Stage 2 there will be
# older SSTables on disk that still contain the key.  The tombstone
# tells the read path "ignore whatever you find in older tables".

_TOMBSTONE = None


class MemTable:
    """Sorted in-memory key-value table with tombstone support.

    Internally just a Python dict.  We sort on demand rather than
    maintaining order, because dict operations are O(1) and sorting
    is only needed when flushing (Stage 2).
    """

    def __init__(self) -> None:
        self._data: dict[str, Optional[str]] = {}
        self._size_bytes: int = 0

    # -- Writes ----------------------------------------------------------------

    def put(self, key: str, value: str) -> None:
        """Insert or overwrite a key with the given value."""
        old_value = self._data.get(key)
        if key not in self._data:
            # New key: count the key's bytes plus the value's bytes.
            self._size_bytes += len(key) + len(value)
        else:
            # Overwrite: subtract old value size, add new value size.
            old_len = len(old_value) if old_value is not None else 0
            self._size_bytes += len(value) - old_len
        self._data[key] = value

    def delete(self, key: str) -> None:
        """Mark a key as deleted by storing a tombstone (None).

        We do NOT remove the key from the dict — see the module
        docstring for why tombstones matter.
        """
        if key not in self._data:
            # New key (tombstone-only): count the key's bytes.
            self._size_bytes += len(key)
        else:
            old_value = self._data.get(key)
            old_len = len(old_value) if old_value is not None else 0
            self._size_bytes -= old_len
        self._data[key] = _TOMBSTONE

    # -- Reads -----------------------------------------------------------------

    def get(self, key: str) -> Tuple[bool, Optional[str]]:
        """Look up a key.

        Returns (found, value):
          - (True, "some string") — the key exists with this value.
          - (True, None)          — the key was explicitly deleted
                                    (tombstone).  This distinction
                                    matters in Stage 2: it tells the
                                    read path to stop looking in older
                                    SSTables.
          - (False, None)         — this table knows nothing about the
                                    key, so the caller should check
                                    older tables.
        """
        if key in self._data:
            return (True, self._data[key])
        return (False, None)

    # -- Iteration (for future SSTable flushing) -------------------------------

    def sorted_items(self) -> List[Tuple[str, Optional[str]]]:
        """Return all (key, value) pairs sorted by key.

        Tombstones (value=None) are included because they must be
        written to the SSTable so that deleted keys are properly
        shadowed in older tables.
        """
        return sorted(self._data.items())

    # -- Size tracking ---------------------------------------------------------

    @property
    def size_bytes(self) -> int:
        """Approximate memory usage in bytes.

        This is a rough estimate (just key + value lengths).  Good
        enough for deciding when to flush the memtable in Stage 2.
        """
        return self._size_bytes
