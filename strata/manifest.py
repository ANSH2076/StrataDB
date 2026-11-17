"""Manifest management for Strata.

The MANIFEST file in data_dir records the exact list of active, live
SSTables in age order (oldest to newest).

Why the manifest is critical for crash safety:
Directory listings alone cannot tell you whether a file was created by
a compaction that finished, an unfinished compaction that crashed, or
an unfinished flush that crashed.  The MANIFEST is the atomic COMMIT POINT.
If a file is on disk but not in the MANIFEST, it is an orphan and can be
safely deleted on startup.

Format:
    {"sstables": ["000001.sst", "000002.sst"]}
"""

from __future__ import annotations

import json
import os
from typing import List, Optional

MANIFEST_FILENAME = "MANIFEST"
MANIFEST_TMP_FILENAME = "MANIFEST.tmp"


def write_manifest(data_dir: str, sstable_names: List[str], sync: bool = True) -> None:
    """Atomically write the list of live SSTable file names to MANIFEST.

    Steps:
      1. Write JSON to MANIFEST.tmp.
      2. flush() and os.fsync() the file so bytes reach disk.
      3. os.replace(MANIFEST.tmp, MANIFEST) — atomic rename on POSIX and Windows.
         This guarantees that MANIFEST is always either the exact previous
         version or the exact new version, never half-written.
      4. fsync the directory (best-effort; Windows does not support directory
         fsync so we catch and ignore OSError).
    """
    tmp_path = os.path.join(data_dir, MANIFEST_TMP_FILENAME)
    target_path = os.path.join(data_dir, MANIFEST_FILENAME)

    payload = json.dumps({"sstables": sstable_names}, ensure_ascii=False)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()
        if sync:
            os.fsync(f.fileno())

    # Atomic replace: the manifest transitions from old to new instantaneously.
    os.replace(tmp_path, target_path)

    if sync:
        _fsync_directory(data_dir)


def read_manifest(data_dir: str) -> Optional[List[str]]:
    """Read the list of live SSTables from MANIFEST.

    Returns:
      - list[str]: The list of SSTable filenames (oldest to newest).
      - None: If MANIFEST does not exist (e.g. fresh database or legacy data).

    Raises:
      - ValueError: If the manifest file exists but contains invalid JSON
        or an unexpected structure.  Because MANIFEST is written atomically via
        os.replace, corruption is not a torn write — it indicates real damage.
    """
    manifest_path = os.path.join(data_dir, MANIFEST_FILENAME)
    if not os.path.exists(manifest_path):
        return None

    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ValueError(f"Corrupt or invalid MANIFEST file: {e}") from e

    if not isinstance(data, dict) or "sstables" not in data or not isinstance(data["sstables"], list):
        raise ValueError("Invalid MANIFEST format: expected {'sstables': [...]}")

    return list(data["sstables"])


def _fsync_directory(dir_path: str) -> None:
    """fsync a directory so recent renames and file creations are durable."""
    try:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        # Windows does not support directory fsync; catch and ignore.
        pass
