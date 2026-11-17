"""Unit tests for the Strata database (Stages 1–2).

Run with:  python3 -m pytest tests/test_engine.py -v
"""

import os
import json
import zlib

import pytest

from strata import Strata


# -- Basic operations ----------------------------------------------------------

def test_put_and_get(tmp_path):
    """put stores a value; get retrieves it."""
    with Strata(str(tmp_path / "db")) as db:
        db.put("fruit", "apple")
        assert db.get("fruit") == "apple"


def test_overwrite(tmp_path):
    """Overwriting a key returns the newest value."""
    with Strata(str(tmp_path / "db")) as db:
        db.put("fruit", "apple")
        db.put("fruit", "banana")
        assert db.get("fruit") == "banana"


def test_missing_key(tmp_path):
    """Getting a key that was never written returns None."""
    with Strata(str(tmp_path / "db")) as db:
        assert db.get("ghost") is None


def test_delete(tmp_path):
    """Deleting a key makes get return None."""
    with Strata(str(tmp_path / "db")) as db:
        db.put("fruit", "apple")
        db.delete("fruit")
        assert db.get("fruit") is None


# -- Recovery (reopen) --------------------------------------------------------

def test_recovery_put(tmp_path):
    """Data survives closing and reopening the database."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("a", "1")
        db.put("b", "2")

    # Reopen — the WAL should reconstruct the memtable.
    with Strata(db_dir) as db:
        assert db.get("a") == "1"
        assert db.get("b") == "2"


def test_recovery_delete(tmp_path):
    """Deletes are recovered correctly after reopening."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("a", "1")
        db.delete("a")

    with Strata(db_dir) as db:
        assert db.get("a") is None


def test_recovery_overwrite(tmp_path):
    """Overwrites are recovered: last value wins."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("a", "old")
        db.put("a", "new")

    with Strata(db_dir) as db:
        assert db.get("a") == "new"


# -- WAL corruption -----------------------------------------------------------

def _wal_path(db_dir: str) -> str:
    return os.path.join(db_dir, "wal.log")


def test_torn_write(tmp_path):
    """An incomplete last record (torn write) is safely discarded.

    We write a few good records, then append half a record (no newline).
    On reopen, the good records should survive and the damaged tail
    should be removed.
    """
    db_dir = str(tmp_path / "db")

    # Write two good records.
    with Strata(db_dir) as db:
        db.put("x", "10")
        db.put("y", "20")

    # Append garbage that looks like a torn write (no trailing newline).
    with open(_wal_path(db_dir), "ab") as f:
        f.write(b"deadbeef {\"op\":\"put\",\"k\":\"z\",\"v\":")

    # Reopen — the two good records should survive.
    with Strata(db_dir) as db:
        assert db.get("x") == "10"
        assert db.get("y") == "20"
        assert db.get("z") is None  # torn write was discarded


def test_corrupt_checksum(tmp_path):
    """A record with a flipped byte stops replay at that point.

    Records before the corruption survive; the corrupt record and
    everything after it are discarded.
    """
    db_dir = str(tmp_path / "db")

    with Strata(db_dir) as db:
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")

    # Read the WAL and corrupt the second record.
    wal = _wal_path(db_dir)
    with open(wal, "rb") as f:
        lines = f.readlines()

    assert len(lines) == 3
    # Flip a byte in the JSON payload of line 1 (the "b" record).
    bad_line = bytearray(lines[1])
    # Find a byte in the payload area and change it.
    for i in range(10, len(bad_line)):
        if bad_line[i] != ord("X"):
            bad_line[i] = ord("X")
            break
    lines[1] = bytes(bad_line)

    with open(wal, "wb") as f:
        f.writelines(lines)

    # Reopen — only the first record should survive.
    with Strata(db_dir) as db:
        assert db.get("a") == "1"
        assert db.get("b") is None  # corrupt, discarded
        assert db.get("c") is None  # after corruption, also discarded


# -- Special characters -------------------------------------------------------

def test_special_characters(tmp_path):
    """Keys and values with newlines, quotes, and unicode round-trip."""
    db_dir = str(tmp_path / "db")

    cases = {
        "new\nline":   "val\nue",
        'quo"te':      'va"lue',
        "emoji_🔥":    "value_🎉",
        "tab\there":   "tab\tthere",
        "backslash\\": "back\\slash",
    }

    with Strata(db_dir) as db:
        for k, v in cases.items():
            db.put(k, v)

    # Reopen to verify recovery handles these characters too.
    with Strata(db_dir) as db:
        for k, v in cases.items():
            assert db.get(k) == v, f"Failed for key {k!r}"


# -- Validation ----------------------------------------------------------------

def test_empty_key_raises(tmp_path):
    """An empty key should raise ValueError."""
    with Strata(str(tmp_path / "db")) as db:
        try:
            db.put("", "value")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass


def test_non_string_key_raises(tmp_path):
    """A non-string key should raise TypeError."""
    with Strata(str(tmp_path / "db")) as db:
        try:
            db.put(123, "value")
            assert False, "Should have raised TypeError"
        except TypeError:
            pass

def test_unknown_wal_op_raises(tmp_path):
    """An unknown WAL operation raises ValueError on recovery."""
    db_dir = str(tmp_path / "db")

    # Write one good record, then close normally.
    with Strata(db_dir) as db:
        db.put("a", "1")

    # Manually append a record with a bogus "op" to the WAL.
    from strata.record import encode_record
    bad_record = encode_record({"op": "xyzzy", "k": "b"})
    with open(os.path.join(db_dir, "wal.log"), "ab") as f:
        f.write(bad_record)

    # Reopening should raise ValueError.
    with pytest.raises(ValueError, match="Unknown WAL operation"):
        Strata(db_dir)
