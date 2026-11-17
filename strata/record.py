"""Record encoding and decoding for Strata.

Both the WAL and SSTables store one record per line in the same format:
    <crc32 hex 8 chars> <JSON payload>\n

This module provides the shared encode/decode logic so both can reuse it.
"""

from __future__ import annotations

import json
import zlib
from typing import Dict, Optional


def encode_record(record: Dict[str, object]) -> bytes:
    """Turn a record dict into a checksummed line: '<crc32> <json>\n'.

    The CRC32 is computed over the JSON bytes so we can detect
    corruption or torn writes during recovery.
    """
    payload = json.dumps(record, ensure_ascii=False)
    payload_bytes = payload.encode("utf-8")
    crc = zlib.crc32(payload_bytes) & 0xFFFFFFFF  # ensure unsigned 32-bit
    line = f"{crc:08x} {payload}\n"
    return line.encode("utf-8")


def decode_record(raw: bytes) -> Optional[Dict[str, object]]:
    """Parse one checksummed line.

    Returns the record dict, or None if the line is corrupt (bad CRC,
    bad JSON, or wrong format).
    """
    # A valid line looks like: "a1b2c3d4 {....}\n"
    text = raw.decode("utf-8", errors="replace")
    text = text.rstrip("\n")

    # Need at least "XXXXXXXX " (9 characters) before the JSON.
    if len(text) < 10 or text[8] != " ":
        return None

    crc_hex = text[:8]
    payload = text[9:]

    # Verify the CRC.
    try:
        expected_crc = int(crc_hex, 16)
    except ValueError:
        return None

    actual_crc = zlib.crc32(payload.encode("utf-8")) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        return None

    # Parse the JSON.
    try:
        record = json.loads(payload)
    except json.JSONDecodeError:
        return None

    return record
