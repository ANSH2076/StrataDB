"""Benchmark script: Read performance across Bloom filter and Compaction.

Tests:
  1. Part 1: Bloom filter ON vs OFF across 46 uncompacted SSTables (5,000 absent & present lookups).
  2. Part 2: Compaction benchmark: compact all SSTables into 1 table, timing lookups before and after.

Usage:
    python3 tests/bench_reads.py
"""

from __future__ import annotations

import os
import random
import shutil
import sys
import tempfile
import time

# Allow importing strata from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strata import Strata

NUM_TOTAL_KEYS = 20_000
NUM_LOOKUPS = 5_000
MEMTABLE_LIMIT_BYTES = 16 * 1024  # 16 KB to produce ~35-46 SSTables


def setup_benchmark_db(data_dir: str) -> int:
    """Populate database with NUM_TOTAL_KEYS with auto_compact=False."""
    print(f"Setting up database with {NUM_TOTAL_KEYS:,} keys...")
    with Strata(data_dir, sync=False, memtable_limit_bytes=MEMTABLE_LIMIT_BYTES, auto_compact=False) as db:
        for i in range(NUM_TOTAL_KEYS):
            # Introduce ~10% overwrites to test compaction deduplication
            db.put(f"user_{i:06d}", f"profile_data_payload_{i:06d}")
        # Overwrite 2,000 keys
        for i in range(0, 2000):
            db.put(f"user_{i:06d}", f"updated_payload_{i:06d}")
        # Delete 500 keys
        for i in range(2000, 2500):
            db.delete(f"user_{i:06d}")

    sst_count = len([f for f in os.listdir(data_dir) if f.endswith(".sst")])
    print(f"Dataset ready: {sst_count} SSTables generated on disk.\n")
    return sst_count


def run_lookups(db: Strata, keys: list[str]) -> tuple[float, dict]:
    """Run lookups and return (elapsed_seconds, stats)."""
    t0 = time.perf_counter()
    for k in keys:
        _ = db.get(k)
    elapsed = time.perf_counter() - t0
    return elapsed, db.stats()


def main() -> None:
    rng = random.Random(42)
    temp_dir = tempfile.mkdtemp(prefix="strata_bench_")

    try:
        sst_count_before = setup_benchmark_db(temp_dir)

        # 1. Prepare 5,000 PRESENT keys sampled randomly from existing active keys
        present_candidates = [i for i in range(NUM_TOTAL_KEYS) if not (2000 <= i < 2500)]
        present_indices = rng.sample(present_candidates, NUM_LOOKUPS)
        present_keys = [f"user_{i:06d}" for i in present_indices]

        # 2. Prepare 5,000 ABSENT keys interleaved within key range
        absent_keys = [f"user_{i:06d}_missing" for i in range(NUM_LOOKUPS)]

        # --- Benchmark 1: Uncompacted (Bloom OFF) ---
        with Strata(temp_dir, sync=False, use_bloom=False, auto_compact=False) as db:
            t_absent_off, _ = run_lookups(db, absent_keys)
            t_present_off, _ = run_lookups(db, present_keys)

        # --- Benchmark 2: Uncompacted (Bloom ON) ---
        with Strata(temp_dir, sync=False, use_bloom=True, auto_compact=False) as db:
            t_absent_on, stats_absent_on = run_lookups(db, absent_keys)
            t_present_on, stats_present_on = run_lookups(db, present_keys)

        # --- Benchmark 3: Compaction ---
        print("Compacting database...")
        with Strata(temp_dir, sync=False, use_bloom=True, auto_compact=False) as db:
            compaction_summary = db.compact()
            sst_count_after = db.stats()["sstable_count"]
            assert compaction_summary is not None

            # --- Benchmark 4: Compacted (Bloom ON) ---
            t_absent_compacted, stats_absent_compacted = run_lookups(db, absent_keys)
            t_present_compacted, stats_present_compacted = run_lookups(db, present_keys)

        speedup_bloom_absent = t_absent_off / t_absent_on if t_absent_on > 0 else float("inf")
        speedup_compact_present = t_present_on / t_present_compacted if t_present_compacted > 0 else float("inf")
        speedup_compact_absent = t_absent_on / t_absent_compacted if t_absent_compacted > 0 else float("inf")

        print("### Stage 4 Benchmark Results")
        print(f"- **SSTables before compaction:** {sst_count_before}")
        print(f"- **SSTables after compaction:** {sst_count_after}")
        print(f"- **Compaction duration:** {compaction_summary['duration_ms']:.2f} ms")
        print(f"- **Records in:** {compaction_summary['records_in']:,} | **Records out:** {compaction_summary['records_out']:,}")
        print(f"- **Overwrites dropped:** {compaction_summary['overwritten_dropped']:,} | **Tombstones dropped:** {compaction_summary['tombstones_dropped']:,}\n")

        print("#### Read Latency Comparison (5,000 lookups each)\n")
        print("| Configuration | Absent Keys Time | Present Keys Time | Notes |")
        print("|:---|:---:|:---:|:---|")
        print(f"| **Uncompacted (Bloom OFF)** | {t_absent_off:.4f} s | {t_present_off:.4f} s | Searches across {sst_count_before} SSTables on disk |")
        print(f"| **Uncompacted (Bloom ON)** | {t_absent_on:.4f} s | {t_present_on:.4f} s | {speedup_bloom_absent:.2f}x faster absent lookups |")
        print(f"| **Compacted (Bloom ON)** | {t_absent_compacted:.4f} s | {t_present_compacted:.4f} s | **{speedup_compact_present:.2f}x** faster present lookups vs uncompacted |")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
