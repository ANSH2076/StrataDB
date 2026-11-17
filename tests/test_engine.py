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


# -- Stage 2: SSTables, Flushing, and Multi-level Reads ------------------------

def test_flush_creates_sstable_and_resets_memtable(tmp_path):
    """Writing enough data triggers an automatic flush to an SSTable."""
    db_dir = str(tmp_path / "db")
    # Low limit so a few writes force flushes
    with Strata(db_dir, memtable_limit_bytes=50) as db:
        for i in range(10):
            db.put(f"key{i:02d}", f"val{i:02d}")

        st = db.stats()
        assert st["sstable_count"] > 0
        # Memtable was reset on the last flush and holds only recent writes
        assert st["memtable_bytes"] < 50

    # Every key should still be readable
    with Strata(db_dir) as db:
        for i in range(10):
            assert db.get(f"key{i:02d}") == f"val{i:02d}"


def test_read_across_memtable_and_sstables(tmp_path):
    """Reads find keys whether they live in the MemTable or an SSTable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, memtable_limit_bytes=1000) as db:
        db.put("sstable_key1", "val1")
        db.flush()

        db.put("sstable_key2", "val2")
        db.flush()

        # In-memory only (not flushed yet)
        db.put("memtable_key", "val3")

        assert db.get("sstable_key1") == "val1"
        assert db.get("sstable_key2") == "val2"
        assert db.get("memtable_key") == "val3"
        assert db.get("nonexistent") is None


def test_shadowing_memtable_over_sstable(tmp_path):
    """A newer value in the MemTable shadows an older value in an SSTable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("user", "Alice")
        db.flush()

        # Overwrite in memtable without flushing
        db.put("user", "Bob")
        assert db.get("user") == "Bob"


def test_shadowing_sstable_over_sstable(tmp_path):
    """A newer SSTable shadows an older SSTable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("version", "v1")
        db.flush()

        db.put("version", "v2")
        db.flush()

        assert db.get("version") == "v2"


def test_tombstone_memtable_hides_sstable(tmp_path):
    """A tombstone in the MemTable hides an older value in an SSTable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("target", "persisted_value")
        db.flush()

        # Delete in memtable: should return None and not fall through to SSTable
        db.delete("target")
        assert db.get("target") is None


def test_tombstone_sstable_hides_older_sstable(tmp_path):
    """A tombstone in a newer SSTable hides a value in an older SSTable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("target", "older_value")
        db.flush()

        db.delete("target")
        db.flush()

        assert db.get("target") is None


def test_reopen_after_flushes(tmp_path):
    """Reopening the database preserves flushed SSTables, WAL state, and tombstones."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, memtable_limit_bytes=100) as db:
        for i in range(20):
            db.put(f"item{i:02d}", f"content{i:02d}")
        db.delete("item05")
        db.delete("item15")
        db.put("item00", "overwritten")

    # Reopen and check all data
    with Strata(db_dir) as db:
        assert db.get("item05") is None
        assert db.get("item15") is None
        assert db.get("item00") == "overwritten"
        assert db.get("item01") == "content01"
        assert db.get("item19") == "content19"


def test_sparse_index_queries(tmp_path):
    """Sparse index correctly locates keys across multiple 16-record blocks.

    Tests:
      - First key
      - Last key
      - Keys on index boundaries (count % 16 == 0)
      - Keys between index boundaries
      - Key smaller than first_key
      - Key larger than last_key
      - Non-existent key between two stored keys
    """
    db_dir = str(tmp_path / "db")
    # Write 150 items so we have ceil(150 / 16) = 10 sparse index blocks
    total = 150
    with Strata(db_dir) as db:
        for i in range(total):
            db.put(f"k{i:04d}", f"val{i:04d}")
        db.flush()

        # 1. First and last key
        assert db.get(f"k{0:04d}") == f"val{0:04d}"
        assert db.get(f"k{total - 1:04d}") == f"val{total - 1:04d}"

        # 2. Keys on exact index boundaries (multiples of 16: 0, 16, 32, 48...)
        for idx in range(0, total, 16):
            assert db.get(f"k{idx:04d}") == f"val{idx:04d}"

        # 3. Keys between index boundaries (e.g. 1, 15, 17, 31...)
        for idx in [1, 15, 17, 31, 63, 100, 148]:
            assert db.get(f"k{idx:04d}") == f"val{idx:04d}"

        # 4. Key smaller than first_key
        assert db.get("k-too-small") is None

        # 5. Key larger than last_key
        assert db.get("k9999-too-large") is None

        # 6. Non-existent key between two stored keys
        assert db.get("k0010_middle") is None


def test_crash_window_wal_replayed_over_sstable(tmp_path):
    """Simulate crash window: SSTable was written, but crash happened before WAL reset.

    On recovery, the WAL is replayed on top of the SSTable.  The replayed
    values are identical or newer, so no data is lost and state is consistent.
    """
    db_dir = str(tmp_path / "db")

    # Step A: write items and flush normally
    with Strata(db_dir) as db:
        db.put("k1", "v1")
        db.put("k2", "v2")
        db.flush()

    # Step B: manually put entries into the WAL simulating writes that were
    # about to be flushed or were flushed right before a crash before WAL reset.
    from strata.record import encode_record
    wal_path = os.path.join(db_dir, "wal.log")
    with open(wal_path, "ab") as f:
        # Re-write k2 with a newer value and add k3
        f.write(encode_record({"op": "put", "k": "k2", "v": "v2_updated"}))
        f.write(encode_record({"op": "put", "k": "k3", "v": "v3"}))

    # Reopen: should cleanly replay WAL on top of SSTable
    with Strata(db_dir) as db:
        assert db.get("k1") == "v1"
        assert db.get("k2") == "v2_updated"
        assert db.get("k3") == "v3"


def test_corrupt_sstable_raises_value_error(tmp_path):
    """A corrupted record in an SSTable raises ValueError when opened or read."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("corrupt_test", "12345")
        db.flush()

    sst_path = os.path.join(db_dir, "000001.sst")
    with open(sst_path, "r+b") as f:
        # Corrupt the CRC by changing the first byte
        f.seek(0)
        f.write(b"00000000")

    # Reopening should fail when reading the corrupt SSTable
    with pytest.raises(ValueError, match="Corrupt SSTable record"):
        Strata(db_dir)


def test_leftover_tmp_file_cleaned_up_on_startup(tmp_path):
    """Leftover *.tmp files from an interrupted flush are deleted on startup."""
    db_dir = str(tmp_path / "db")
    os.makedirs(db_dir, exist_ok=True)

    tmp_file = os.path.join(db_dir, "000005.sst.tmp")
    with open(tmp_file, "wb") as f:
        f.write(b"half written garbage")

    assert os.path.exists(tmp_file)

    # Opening Strata should remove the stale .tmp file
    with Strata(db_dir) as db:
        assert not os.path.exists(tmp_file)
        db.put("k", "v")
        assert db.get("k") == "v"


def test_special_characters_survive_flush_and_reload(tmp_path):
    """Special characters (newlines, quotes, unicode, emojis) survive flushes."""
    db_dir = str(tmp_path / "db")

    cases = {
        "new\nline":   "val\nue",
        'quo"te':      'va"lue',
        "emoji_🚀":    "sparkle_✨",
        "tab\there":   "tab\tthere",
        "backslash\\": "back\\slash",
    }

    with Strata(db_dir) as db:
        for k, v in cases.items():
            db.put(k, v)
        db.flush()

    # Reopen and check
    with Strata(db_dir) as db:
        for k, v in cases.items():
            assert db.get(k) == v


def test_stats(tmp_path):
    """stats() returns correct metadata about memtable and SSTables."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.put("alpha", "1")
        db.put("beta", "2")
        st1 = db.stats()
        assert st1["memtable_keys"] == 2
        assert st1["sstable_count"] == 0

        db.flush()

        st2 = db.stats()
        assert st2["memtable_keys"] == 0
        assert st2["sstable_count"] == 1
        sst_info = st2["sstables"][0]
        assert sst_info["name"] == "000001.sst"
        assert sst_info["record_count"] == 2
        assert sst_info["first_key"] == "alpha"
        assert sst_info["last_key"] == "beta"
        assert sst_info["file_size_bytes"] > 0


def test_empty_memtable_flush_noop(tmp_path):
    """Flushing an empty memtable is a no-op and does not create an empty .sst file."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir) as db:
        db.flush()
        assert db.stats()["sstable_count"] == 0
        assert len([f for f in os.listdir(db_dir) if f.endswith(".sst")]) == 0


def test_trace_get_and_counters(tmp_path):
    """trace_get() and get() agree on results, and trace_get does not increment counters."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, memtable_limit_bytes=100, auto_compact=False) as db:
        db.put("k01", "v01")
        db.flush()
        db.put("k02", "v02")
        db.delete("k02")
        db.flush()
        db.put("k03", "v03")

        # Initial counter state
        initial_stats = db.stats()
        skips0 = initial_stats["bloom_skips"]
        lookups0 = initial_stats["disk_lookups"]
        fp0 = initial_stats["false_positives"]

        # Test trace_get on existing, deleted, and absent keys
        for key in ["k01", "k02", "k03", "nonexistent"]:
            tr = db.trace_get(key)
            val = db.get(key)
            if tr["found"]:
                assert tr["value"] == val
            else:
                assert val is None
            assert len(tr["steps"]) > 0

        # Now test that trace_get specifically does not alter counters
        st_after_get = db.stats()
        skips1 = st_after_get["bloom_skips"]
        lookups1 = st_after_get["disk_lookups"]
        fp1 = st_after_get["false_positives"]

        # Run 50 trace_get lookups
        for _ in range(50):
            db.trace_get("k01")
            db.trace_get("nonexistent_key")

        st_after_trace = db.stats()
        assert st_after_trace["bloom_skips"] == skips1
        assert st_after_trace["disk_lookups"] == lookups1
        assert st_after_trace["false_positives"] == fp1


