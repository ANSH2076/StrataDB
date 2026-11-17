"""Write-Ahead Log for Strata.

The WAL is an append-only file that records every mutation before it
reaches the in-memory table.  If the process crashes, we can replay
this file to rebuild the exact state we had.

File format (one record per line):
    <crc32 hex 8 chars> <JSON payload>\n

Example:
    a1b2c3d4 {"op":"put","k":"name","v":"Alice"}
"""

from __future__ import annotations

import json
import os
import zlib
from typing import Dict, List, Optional


# --- Record helpers -----------------------------------------------------------

def _encode_record(record: Dict[str, str]) -> bytes:
    """Turn a record dict into a WAL line: '<crc32> <json>\n' as bytes."""
    payload = json.dumps(record, ensure_ascii=False)
    payload_bytes = payload.encode("utf-8")
    crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF  # ensure unsigned 32-bit
    line = f"{crc:08x} {payload}\n"
    return line.encode("utf-8")


def _decode_line(raw: bytes) -> Optional[Dict[str, str]]:
    """Parse one WAL line.  Returns the record dict, or None if it is
    corrupt (bad CRC, bad JSON, or wrong format)."""
    # A valid line looks like: "a1b2c3d4 {....}\n"
    text = raw.decode("utf-8", errors="replace")
    text = text.rstrip("\n")

    # Need at least "XXXXXXXX " (9 characters) before the JSON
    if len(text) < 10 or text[8] != " ":
        return None

    crc_hex = text[:8]
    payload = text[9:]

    # Verify the CRC
    try:
        expected_crc = int(crc_hex, 16)
    except ValueError:
        return None

    actual_crc = zlib.crc32(payload.encode("utf-8")) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        return None

    # Parse the JSON
    try:
        record = json.loads(payload)
    except json.JSONDecodeError:
        return None

    return record


# --- WriteAheadLog class ------------------------------------------------------

class WriteAheadLog:
    """Append-only write-ahead log backed by a single file.

    Args:
        path: Full path to the WAL file (e.g. "<data_dir>/wal.log").
        sync: If True, every append is followed by os.fsync so the OS
              actually flushes to disk.  Slower but crash-safe.
    """

    def __init__(self, path: str, sync: bool = True) -> None:
        self._path = path
        self._sync = sync
        # Open in append-binary mode so writes always go to the end.
        # 'ab' also creates the file if it does not exist.
        self._file = open(path, "ab")

    # -- Writing ---------------------------------------------------------------

    def append(self, record: Dict[str, str]) -> None:
        """Write one record to the log and ensure it reaches the OS.

        flush() pushes Python's internal buffer to the OS.
        fsync()  asks the OS to push its buffer to the physical disk.
        Without fsync, a power loss could lose data that flush() already
        handed to the OS but that the OS had not yet written to disk.
        """
        data = _encode_record(record)
        self._file.write(data)
        self._file.flush()
        if self._sync:
            os.fsync(self._file.fileno())

    # -- Replay / Recovery -----------------------------------------------------

    def replay(self) -> List[Dict[str, str]]:
        """Read the WAL and return all valid records.

        We stop at the first bad line.  A bad line can happen because:

        1. TORN WRITE — The process crashed in the middle of writing a
           line.  The last line in the file may be incomplete (no
           trailing newline) or only partially written.  The CRC will
           not match the partial JSON, so we detect it.

        2. CORRUPT DATA — A disk error flipped some bits.  Again the
           CRC will not match.

        After finding the last good record, we truncate the file to
        remove any trailing garbage.  This way new appends start right
        after the last known-good record instead of following junk bytes
        that would stay broken forever.
        """
        good_records: List[Dict[str, str]] = []
        last_good_offset = 0  # byte position after the last valid line

        with open(self._path, "rb") as f:
            while True:
                line = f.readline()
                if not line:
                    # Reached the end of the file cleanly.
                    break

                # An incomplete last line has no trailing newline.
                # This is the classic torn-write case.
                if not line.endswith(b"\n"):
                    break

                record = _decode_line(line)
                if record is None:
                    # CRC mismatch or bad JSON — stop here.
                    break

                good_records.append(record)
                last_good_offset = f.tell()

        # Truncate any trailing garbage so the next append is clean.
        with open(self._path, "r+b") as f:
            f.truncate(last_good_offset)

        return good_records

    # -- Lifecycle -------------------------------------------------------------

    def close(self) -> None:
        """Flush and close the WAL file."""
        self._file.flush()
        self._file.close()
