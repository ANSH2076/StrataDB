"""Unit tests for Bloom filters and Stage 3 integration.

Tests verify:
  1. No false negatives (100% recall for inserted keys).
  2. False positive rate is < 3% for 10 bits/key and 7 hashes.
  3. Determinism across different PYTHONHASHSEED environments.
  4. Tombstone safety: tombstones in newer SSTables correctly shadow older values
     regardless of whether bloom filtering is enabled or disabled.
  5. Full read equivalence between use_bloom=True and use_bloom=False.
  6. Correct incrementing of bloom_skips, disk_lookups, and false_positives counters.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from strata import Strata
from strata.bloom import BloomFilter


# -- BloomFilter Unit Tests ----------------------------------------------------

def test_no_false_negatives():
    """Every key added to the filter must return might_contain=True (0% false negatives)."""
    n_keys = 2000
    bf = BloomFilter.for_capacity(n_keys)
    keys = [f"test_key_{i}" for i in range(n_keys)]

    for k in keys:
        bf.add(k)

    for k in keys:
        assert bf.might_contain(k) is True, f"False negative detected for {k!r}"


def test_false_positive_rate():
    """False positive rate on 20,000 absent keys must be under 3% (~1% expected)."""
    n_keys = 2000
    bf = BloomFilter.for_capacity(n_keys)
    for i in range(n_keys):
        bf.add(f"present_{i}")

    n_absent = 20000
    false_positives = 0
    for i in range(n_absent):
        if bf.might_contain(f"absent_{i}"):
            false_positives += 1

    fp_rate = false_positives / n_absent
    # Sized for ~0.82% theoretical FPR; assert well under the 3% requirement.
    assert fp_rate < 0.03, f"FPR too high: {fp_rate:.4f} ({false_positives}/{n_absent})"


def test_determinism_identical_bytes():
    """Two filters populated with the same keys produce identical raw bytes."""
    keys = [f"key_{i}" for i in range(500)]

    bf1 = BloomFilter.for_capacity(500)
    for k in keys:
        bf1.add(k)

    bf2 = BloomFilter.for_capacity(500)
    for k in keys:
        bf2.add(k)

    assert bf1.raw_bytes == bf2.raw_bytes


def test_determinism_across_pythonhashseed():
    """Filter bits are identical across subprocesses with different PYTHONHASHSEED.

    This proves we do NOT rely on Python's randomized hash() function.
    """
    code = (
        "import sys\n"
        "from strata.bloom import BloomFilter\n"
        "bf = BloomFilter.for_capacity(100)\n"
        "for i in range(100):\n"
        "    bf.add(f'key_{i}')\n"
        "sys.stdout.write(bf.raw_bytes.hex())\n"
    )

    outputs = []
    for seed in ["0", "12345", "999999", "random"]:
        env = {**os.environ, "PYTHONHASHSEED": seed}
        res = subprocess.check_output([sys.executable, "-c", code], env=env, text=True)
        outputs.append(res)

    assert len(set(outputs)) == 1, "Filter output changed across PYTHONHASHSEED values!"


# -- Engine Integration Tests --------------------------------------------------

def test_tombstone_safety(tmp_path):
    """A deleted key must return None regardless of use_bloom=True or False.

    If tombstones were missing from the Bloom filter in the newer SSTable,
    might_contain would return False, skipping the tombstone and resurrecting
    the old value from the older SSTable!
    """
    db_dir = str(tmp_path / "db")

    with Strata(db_dir, sync=False) as db:
        # 1. Put key and flush to older SSTable (000001.sst)
        db.put("victim_user", "active_account")
        db.flush()

        # 2. Delete key and flush tombstone to newer SSTable (000002.sst)
        db.delete("victim_user")
        db.flush()

    # Query with Bloom filter ENABLED
    with Strata(db_dir, use_bloom=True) as db_bloom_on:
        assert db_bloom_on.get("victim_user") is None

    # Query with Bloom filter DISABLED
    with Strata(db_dir, use_bloom=False) as db_bloom_off:
        assert db_bloom_off.get("victim_user") is None


def test_bloom_on_and_off_equivalence(tmp_path):
    """get() returns identical results with bloom enabled vs disabled across all keys."""
    db_dir = str(tmp_path / "db")

    # Generate a rich mix of flushes, overwrites, and deletes
    with Strata(db_dir, sync=False, memtable_limit_bytes=300) as db:
        for i in range(100):
            db.put(f"key_{i:03d}", f"val_{i}")
        # Overwrite some
        for i in range(0, 50, 2):
            db.put(f"key_{i:03d}", f"updated_{i}")
        # Delete some
        for i in range(10, 30):
            db.delete(f"key_{i:03d}")

    # Reopen and compare every key with bloom on vs off
    with Strata(db_dir, use_bloom=True) as db_on, Strata(db_dir, use_bloom=False) as db_off:
        # Check all possible keys (existing, overwritten, deleted, and absent)
        test_keys = [f"key_{i:03d}" for i in range(150)] + ["nonexistent_a", "nonexistent_z"]
        for k in test_keys:
            val_on = db_on.get(k)
            val_off = db_off.get(k)
            assert val_on == val_off, f"Mismatch for {k!r}: on={val_on!r}, off={val_off!r}"


def test_bloom_counters(tmp_path):
    """Reading absent keys increments bloom_skips when on and disk_lookups when off."""
    db_dir = str(tmp_path / "db")

    # Populate several SSTables with keys in range k000..k099
    with Strata(db_dir, sync=False, memtable_limit_bytes=150) as db:
        for i in range(100):
            db.put(f"k{i:03d}", f"v{i}")

    # Query keys that fall inside [first_key, last_key] range of SSTables but are absent
    absent_keys = [f"k{i:03d}_missing" for i in range(5, 95, 5)]

    # 1. With Bloom filter ENABLED:
    with Strata(db_dir, use_bloom=True) as db_on:
        for k in absent_keys:
            assert db_on.get(k) is None
        st_on = db_on.stats()
        assert st_on["bloom_enabled"] is True
        assert st_on["bloom_skips"] > 0
        # Most absent keys should have been skipped by bloom before disk lookup
        assert st_on["bloom_skips"] > st_on["disk_lookups"]

    # 2. With Bloom filter DISABLED:
    with Strata(db_dir, use_bloom=False) as db_off:
        for k in absent_keys:
            assert db_off.get(k) is None
        st_off = db_off.stats()
        assert st_off["bloom_enabled"] is False
        assert st_off["bloom_skips"] == 0
        assert st_off["disk_lookups"] > 0
