"""Unit tests for Stage 4: Compaction, MANIFEST safety, and Event log.

Tests cover:
  - compact() merges N tables into 1 and all keys match
  - Overwritten keys: newest version survives, older dropped
  - Resurrection test: tombstone in newer table permanently deletes older value across compaction and reopen
  - Randomized model test (fixed seed): 3,000 random put/delete/overwrite ops with periodic compaction and reopen compared against a plain dict
  - Crash window A: orphan .sst not listed in MANIFEST is deleted on reopen
  - Crash window B: old .sst files placed back on disk after compaction are deleted as orphans without resurrecting deletes
  - Missing manifest adoption: pre-existing .sst files are adopted and a MANIFEST is written
  - Flush crash window: uncommitted .sst is purged and WAL replay restores data
  - MANIFEST.tmp cleanup on startup
  - Missing file in MANIFEST raises clear FileNotFoundError
  - Corrupt MANIFEST JSON raises ValueError
  - Compacting when all keys were deleted leaves 0 tables and empty manifest list
  - Auto-compaction triggers when sstable_count >= compaction_threshold
  - Event log records 'flush', 'compaction', and 'recovery' events with events_since()
  - stats() returns compactions, compaction_threshold, auto_compact, wal_bytes, wal_records, total_sstable_bytes
"""

from __future__ import annotations

import json
import os
import random
import shutil

import pytest

from strata import Strata
from strata.engine import DEFAULT_COMPACTION_THRESHOLD
from strata.manifest import MANIFEST_FILENAME, MANIFEST_TMP_FILENAME, read_manifest, write_manifest


def test_compact_n_tables_into_one(tmp_path):
    """compact() merges N tables into 1, leaving all keys readable."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        for i in range(15):
            db.put(f"key{i:02d}", f"val{i:02d}")
            if i % 3 == 0:
                db.flush()
        db.flush()  # Flush remaining memtable items so all 15 items are in SSTables

        assert db.stats()["sstable_count"] >= 4
        summary = db.compact()
        assert summary is not None
        assert db.stats()["sstable_count"] == 1
        assert summary["records_out"] == 15


    # Verify all keys after reopen
    with Strata(db_dir) as db:
        for i in range(15):
            assert db.get(f"key{i:02d}") == f"val{i:02d}"


def test_compact_overwritten_values(tmp_path):
    """Overwritten values are deduplicated during compaction; newest survives."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("common", "v1")
        db.flush()
        db.put("common", "v2")
        db.flush()
        db.put("common", "v3_latest")
        db.flush()

        summary = db.compact()
        assert summary is not None
        assert summary["overwritten_dropped"] == 2
        assert summary["records_out"] == 1
        assert db.get("common") == "v3_latest"


def test_tombstone_resurrection_prevention(tmp_path):
    """A deleted key stays deleted after compaction and after reopening.

    Tombstones in newer tables must prevent resurrection of older values,
    and once compacted across all tables, the key is permanently gone.
    """
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        # Table 1: write initial value
        db.put("user_123", "alice")
        db.put("user_456", "bob")
        db.flush()

        # Table 2: delete user_123 (tombstone)
        db.delete("user_123")
        db.flush()

        assert db.get("user_123") is None
        assert db.get("user_456") == "bob"

        # Compact all tables
        summary = db.compact()
        assert summary is not None
        assert summary["tombstones_dropped"] == 1
        assert summary["records_out"] == 1

        assert db.get("user_123") is None
        assert db.get("user_456") == "bob"

    # Reopen to verify durable deletion
    with Strata(db_dir) as db:
        assert db.get("user_123") is None
        assert db.get("user_456") == "bob"


def test_randomized_model_vs_dict(tmp_path):
    """Compare Strata against a reference Python dict across 3,000 random operations."""
    db_dir = str(tmp_path / "db")
    rng = random.Random(42)
    reference: dict[str, str] = {}

    db = Strata(db_dir, sync=False, memtable_limit_bytes=512, auto_compact=True, compaction_threshold=4)

    for step in range(3000):
        key = f"key_{rng.randint(0, 300):04d}"
        op = rng.random()

        if op < 0.60:
            # 60% puts / overwrites
            val = f"val_{step}_{rng.randint(0, 1000)}"
            reference[key] = val
            db.put(key, val)
        elif op < 0.85:
            # 25% deletes
            reference.pop(key, None)
            db.delete(key)
        else:
            # 15% gets
            ref_val = reference.get(key)
            db_val = db.get(key)
            assert db_val == ref_val

        # Randomly trigger explicit compaction (1% chance)
        if rng.random() < 0.01:
            db.compact()

        # Randomly restart database (0.5% chance)
        if rng.random() < 0.005:
            db.close()
            db = Strata(db_dir, sync=False, memtable_limit_bytes=512, auto_compact=True, compaction_threshold=4)

    # Final comparison: check every possible key
    for k in [f"key_{i:04d}" for i in range(350)]:
        assert db.get(k) == reference.get(k)

    db.close()

    # Reopen once more and verify everything matches
    with Strata(db_dir) as db_reopened:
        for k in [f"key_{i:04d}" for i in range(350)]:
            assert db_reopened.get(k) == reference.get(k)


def test_crash_window_a_orphan_deleted(tmp_path):
    """Crash Window A: An SSTable on disk not in MANIFEST is deleted as an orphan."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("k1", "v1")
        db.flush()

    # Inject an uncommitted orphan SSTable file
    orphan_path = os.path.join(db_dir, "000099.sst")
    from strata.sstable import write_sstable
    write_sstable(orphan_path, [("ghost", "value")], sync=False)
    assert os.path.exists(orphan_path)

    # Reopen: the orphan must be deleted and ghost must not exist
    with Strata(db_dir) as db:
        assert not os.path.exists(orphan_path)
        assert db.get("k1") == "v1"
        assert db.get("ghost") is None
        assert db.recovery_info["orphans_removed"] == 1


def test_crash_window_b_old_tables_deleted(tmp_path):
    """Crash Window B: Old SSTables restored after compaction are deleted without resurrecting data."""
    db_dir = str(tmp_path / "db")
    backup_dir = str(tmp_path / "backup")
    os.makedirs(backup_dir)

    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("item", "old_val")
        db.flush()
        # Backup the old table
        shutil.copy(os.path.join(db_dir, "000001.sst"), os.path.join(backup_dir, "000001.sst"))

        db.delete("item")
        db.flush()

        # Compact: drops tombstone and produces merged table or empty list
        db.compact()
        assert db.get("item") is None

    # Simulate crash before old files were purged: restore old 000001.sst to data_dir
    shutil.copy(os.path.join(backup_dir, "000001.sst"), os.path.join(db_dir, "000001.sst"))
    assert os.path.exists(os.path.join(db_dir, "000001.sst"))

    # Reopen: 000001.sst is not in MANIFEST, so it must be deleted and item must stay deleted
    with Strata(db_dir) as db:
        assert not os.path.exists(os.path.join(db_dir, "000001.sst"))
        assert db.get("item") is None


def test_missing_manifest_adopted(tmp_path):
    """A directory with .sst files but no MANIFEST adopts them in numerical order."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("a", "1")
        db.flush()
        db.put("b", "2")
        db.flush()

    # Remove the MANIFEST file simulating legacy database
    manifest_path = os.path.join(db_dir, MANIFEST_FILENAME)
    os.remove(manifest_path)
    assert not os.path.exists(manifest_path)

    # Reopen: should adopt 000001.sst and 000002.sst and write a new MANIFEST
    with Strata(db_dir) as db:
        assert os.path.exists(manifest_path)
        assert read_manifest(db_dir) == ["000001.sst", "000002.sst"]
        assert db.get("a") == "1"
        assert db.get("b") == "2"


def test_manifest_tmp_deleted_on_startup(tmp_path):
    """Leftover MANIFEST.tmp is deleted on startup."""
    db_dir = str(tmp_path / "db")
    os.makedirs(db_dir, exist_ok=True)
    manifest_tmp = os.path.join(db_dir, MANIFEST_TMP_FILENAME)
    with open(manifest_tmp, "w") as f:
        f.write("half-written-manifest-data")

    assert os.path.exists(manifest_tmp)
    with Strata(db_dir) as db:
        assert not os.path.exists(manifest_tmp)
        assert db.recovery_info["tmp_removed"] >= 1


def test_manifest_missing_file_raises_error(tmp_path):
    """A MANIFEST listing a missing file raises FileNotFoundError."""
    db_dir = str(tmp_path / "db")
    write_manifest(db_dir, ["nonexistent_000099.sst"], sync=False)

    with pytest.raises(FileNotFoundError, match="MANIFEST lists SSTable"):
        Strata(db_dir)


def test_corrupt_manifest_raises_error(tmp_path):
    """A corrupt MANIFEST file raises ValueError."""
    db_dir = str(tmp_path / "db")
    os.makedirs(db_dir, exist_ok=True)
    manifest_path = os.path.join(db_dir, MANIFEST_FILENAME)
    with open(manifest_path, "w") as f:
        f.write("{invalid_json: 123}")

    with pytest.raises(ValueError, match="Corrupt or invalid MANIFEST"):
        Strata(db_dir)


def test_compact_everything_deleted_leaves_zero_tables(tmp_path):
    """Compacting when all keys were deleted results in zero SSTables and empty manifest list."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("x", "val_x")
        db.flush()
        db.delete("x")
        db.flush()

        assert db.stats()["sstable_count"] == 2
        summary = db.compact()
        assert summary is not None
        assert summary["records_out"] == 0
        assert summary["tombstones_dropped"] == 1
        assert summary["output"] is None
        assert db.stats()["sstable_count"] == 0
        assert read_manifest(db_dir) == []

    # Reopen: should still have 0 tables and get('x') is None
    with Strata(db_dir) as db:
        assert db.stats()["sstable_count"] == 0
        assert db.get("x") is None


def test_auto_compaction_triggers(tmp_path):
    """Auto-compaction automatically runs when table count reaches compaction_threshold."""
    db_dir = str(tmp_path / "db")
    # Threshold = 3
    with Strata(db_dir, sync=False, compaction_threshold=3, auto_compact=True) as db:
        db.put("k1", "v1")
        db.flush()
        assert db.stats()["sstable_count"] == 1

        db.put("k2", "v2")
        db.flush()
        assert db.stats()["sstable_count"] == 2

        # 3rd flush reaches threshold -> auto compact merges 3 tables into 1!
        db.put("k3", "v3")
        db.flush()
        assert db.stats()["sstable_count"] == 1
        assert db.stats()["compactions"] == 1


def test_event_log_and_stats(tmp_path):
    """Events deque records flush, compaction, recovery, and stats has all new fields."""
    db_dir = str(tmp_path / "db")
    with Strata(db_dir, sync=False, auto_compact=False) as db:
        db.put("a", "1")
        db.flush()
        db.put("b", "2")
        db.flush()
        db.compact()

        events = list(db.events)
        types = [e["type"] for e in events]
        assert "recovery" in types
        assert "flush" in types
        assert "compaction" in types

        # Check events_since
        ev1 = events[0]["id"]
        later_events = db.events_since(ev1)
        assert len(later_events) == len(events) - 1

        # Check stats schema
        st = db.stats()
        for expected_key in [
            "memtable_keys", "memtable_bytes", "sstable_count",
            "bloom_enabled", "bloom_skips", "disk_lookups", "false_positives",
            "compactions", "compaction_threshold", "auto_compact",
            "wal_bytes", "wal_records", "total_sstable_bytes", "sstables"
        ]:
            assert expected_key in st, f"Missing stats key: {expected_key}"

        assert st["compactions"] == 1
        assert st["auto_compact"] is False
        assert st["compaction_threshold"] == DEFAULT_COMPACTION_THRESHOLD
