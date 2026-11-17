"""SSTable (Sorted String Table) for Strata.

An SSTable is an immutable, sorted file of key-value records.  Once
written it is never modified — new data goes into new SSTables.

File format (one record per line, same CRC format as the WAL):
    <crc32 hex 8 chars> {"k": key, "v": value_or_null}\n

Records are sorted by key ascending.  Each key appears exactly once.
A tombstone is stored as {"k": key, "v": null}.

Being sorted lets us use binary-style searching (via a sparse index)
instead of scanning the whole file for every get().
"""

from __future__ import annotations

import bisect
import os
from typing import Generator, List, Optional, Tuple

from strata.record import decode_record, encode_record

# We store a byte offset for every INDEX_INTERVAL-th record.  This
# means a get() reads at most INDEX_INTERVAL records from disk — a
# tradeoff between index size in memory and disk reads per lookup.
INDEX_INTERVAL = 16


# --- Writing ------------------------------------------------------------------

def write_sstable(
    path: str,
    sorted_items: List[Tuple[str, Optional[str]]],
    sync: bool = True,
) -> None:
    """Write sorted (key, value) pairs to an SSTable file atomically.

    Atomic write strategy:
      1. Write to path + ".tmp".
      2. flush + fsync the data.
      3. os.replace(tmp, path) — this is atomic on POSIX and modern Windows.
         If we crash mid-write, only the .tmp file is left, never a
         half-written .sst.  On next startup we delete stale .tmp files.
      4. fsync the directory so the rename is durable.  Windows does
         not support fsync on directories, so we wrap it in try/except.
    """
    tmp_path = path + ".tmp"

    with open(tmp_path, "wb") as f:
        for key, value in sorted_items:
            record = {"k": key, "v": value}
            f.write(encode_record(record))
        f.flush()
        if sync:
            os.fsync(f.fileno())

    # Atomic rename: the .sst file either fully exists or not at all.
    os.replace(tmp_path, path)

    # Best-effort directory fsync so the rename survives a power loss.
    # Windows does not let you fsync a directory, so we catch the error.
    if sync:
        _fsync_directory(os.path.dirname(path))


def _fsync_directory(dir_path: str) -> None:
    """fsync a directory to make recent renames durable.

    Not supported on Windows — we catch and ignore the error because
    the data file itself is already fsync'd.
    """
    try:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        # Windows (and some other platforms) cannot fsync directories.
        pass


# --- Reading ------------------------------------------------------------------

class SSTable:
    """A single SSTable file on disk.

    On open, we scan the file once to build a SPARSE INDEX: the key and
    byte offset of every INDEX_INTERVAL-th record.  This lets get() jump
    close to the target key and read at most INDEX_INTERVAL records.

    We open the file per get() call.  This is simple and correct, though
    it means one open+close per lookup.  A file-handle pool would be
    faster but adds complexity we do not need yet.

    Args:
        path: Absolute path to the .sst file.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._index_keys: List[str] = []     # key at each index point
        self._index_offsets: List[int] = []   # byte offset of that line
        self._record_count: int = 0
        self._first_key: Optional[str] = None
        self._last_key: Optional[str] = None
        self._file_size: int = os.path.getsize(path)

        self._build_index()

    # -- Index building --------------------------------------------------------

    def _build_index(self) -> None:
        """Scan the file once to build the sparse index.

        We record the key and byte offset of every INDEX_INTERVAL-th
        record.  This is O(n) in the number of records, but it only
        happens once when the SSTable is opened.
        """
        count = 0
        with open(self._path, "rb") as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break

                record = decode_record(line)
                if record is None:
                    # SSTables are written atomically, so a bad checksum
                    # here is real corruption, not a torn write.
                    raise ValueError(
                        f"Corrupt SSTable record at byte {offset} "
                        f"in {self._path}"
                    )

                key = record["k"]

                if count == 0:
                    self._first_key = key

                # Store an index entry every INDEX_INTERVAL records.
                if count % INDEX_INTERVAL == 0:
                    self._index_keys.append(key)
                    self._index_offsets.append(offset)

                self._last_key = key
                count += 1

        self._record_count = count

    # -- Point lookup ----------------------------------------------------------

    def get(self, key: str) -> Tuple[bool, Optional[str]]:
        """Look up a key in this SSTable.

        Returns (found, value) with the same semantics as MemTable.get:
          - (True, "value") — found it.
          - (True, None)    — found a tombstone (key was deleted).
          - (False, None)   — this SSTable does not contain the key.

        How it works:
          1. Quick range check: if the key is outside [first_key,
             last_key], it cannot be here.
          2. Use bisect on index_keys to find the index block that
             could contain our key.
          3. Seek to that offset and read records until the key is
             found, a key greater than target is seen, or EOF is reached.
             Because records are sorted ascending, we never read past
             the first key greater than our target, reading at most
             INDEX_INTERVAL records.
        """
        # Range check — avoids any disk I/O for obviously absent keys.
        if self._record_count == 0:
            return (False, None)
        if key < self._first_key or key > self._last_key:
            return (False, None)

        # Find the right index block.  bisect_right gives us the index
        # of the first index_key GREATER than our key, so the block we
        # want is one before that.
        pos = bisect.bisect_right(self._index_keys, key) - 1
        if pos < 0:
            pos = 0

        offset = self._index_offsets[pos]

        with open(self._path, "rb") as f:
            f.seek(offset)
            while True:
                line = f.readline()
                if not line:
                    break

                record = decode_record(line)
                if record is None:
                    raise ValueError(
                        f"Corrupt SSTable record in {self._path}"
                    )

                rec_key = record["k"]
                if rec_key == key:
                    return (True, record["v"])
                if rec_key > key:
                    # Because records are sorted, the key cannot appear
                    # after this point.
                    break

        return (False, None)

    # -- Full scan (for compaction in Stage 4) ---------------------------------

    def items(self) -> Generator[Tuple[str, Optional[str]], None, None]:
        """Yield all (key, value) pairs in sorted order.

        This is a generator so we do not load the whole file into memory.
        Needed for compaction (Stage 4) where we merge multiple SSTables.
        """
        with open(self._path, "rb") as f:
            for line in f:
                record = decode_record(line)
                if record is None:
                    raise ValueError(
                        f"Corrupt SSTable record in {self._path}"
                    )
                yield (record["k"], record["v"])

    # -- Metadata --------------------------------------------------------------

    @property
    def path(self) -> str:
        return self._path

    @property
    def name(self) -> str:
        return os.path.basename(self._path)

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def file_size_bytes(self) -> int:
        return self._file_size

    @property
    def first_key(self) -> Optional[str]:
        return self._first_key

    @property
    def last_key(self) -> Optional[str]:
        return self._last_key
