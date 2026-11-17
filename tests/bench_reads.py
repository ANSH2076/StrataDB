"""Benchmark script: Read performance with Bloom filter ON vs OFF.

Tests 5,000 lookups of ABSENT keys and 5,000 lookups of PRESENT keys
across 20,000 keys distributed across many SSTables.

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
MEMTABLE_LIMIT_BYTES = 16 * 1024  # 16 KB to produce ~35-40 SSTables


def setup_benchmark_db(data_dir: str) -> int:
    """Populate database with NUM_TOTAL_KEYS and return number of SSTables created."""
    print(f"Setting up database with {NUM_TOTAL_KEYS:,} keys...")
    with Strata(data_dir, sync=False, memtable_limit_bytes=MEMTABLE_LIMIT_BYTES) as db:
        for i in range(NUM_TOTAL_KEYS):
            # Pad keys to ensure natural lexicographical alignment
            db.put(f"user_{i:06d}", f"profile_data_payload_{i:06d}")

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
        sst_count = setup_benchmark_db(temp_dir)

        # 1. Prepare 5,000 PRESENT keys sampled randomly from existing keys
        present_indices = rng.sample(range(NUM_TOTAL_KEYS), NUM_LOOKUPS)
        present_keys = [f"user_{i:06d}" for i in present_indices]

        # 2. Prepare 5,000 ABSENT keys interleaved within the key range so
        # range checks [first_key, last_key] match, exercising the Bloom filter.
        absent_keys = [f"user_{i:06d}_missing" for i in range(NUM_LOOKUPS)]

        # --- Benchmark 1: Absent Keys (Bloom OFF) ---
        with Strata(temp_dir, sync=False, use_bloom=False) as db:
            t_absent_off, stats_absent_off = run_lookups(db, absent_keys)

        # --- Benchmark 2: Absent Keys (Bloom ON) ---
        with Strata(temp_dir, sync=False, use_bloom=True) as db:
            t_absent_on, stats_absent_on = run_lookups(db, absent_keys)

        # --- Benchmark 3: Present Keys (Bloom OFF) ---
        with Strata(temp_dir, sync=False, use_bloom=False) as db:
            t_present_off, stats_present_off = run_lookups(db, present_keys)

        # --- Benchmark 4: Present Keys (Bloom ON) ---
        with Strata(temp_dir, sync=False, use_bloom=True) as db:
            t_present_on, stats_present_on = run_lookups(db, present_keys)

        # Compute speedups
        speedup_absent = t_absent_off / t_absent_on if t_absent_on > 0 else float("inf")
        speedup_present = t_present_off / t_present_on if t_present_on > 0 else float("inf")

        # Output Results
        print("### Benchmark Results (5,000 lookups across 20,000 keys)")
        print(f"Total SSTables: {sst_count}\n")

        header = (
            "| Lookup Type | Bloom OFF | Bloom ON | Speedup | "
            "Bloom Skips (ON) | Disk Lookups (ON) | False Positives (ON) |"
        )
        separator = (
            "|:---|:---:|:---:|:---:|:---:|:---:|:---:|"
        )
        row_absent = (
            f"| Absent Keys (5,000) | {t_absent_off:.4f} s | {t_absent_on:.4f} s | "
            f"**{speedup_absent:.2f}x** | {stats_absent_on['bloom_skips']:,} | "
            f"{stats_absent_on['disk_lookups']:,} | {stats_absent_on['false_positives']:,} |"
        )
        row_present = (
            f"| Present Keys (5,000) | {t_present_off:.4f} s | {t_present_on:.4f} s | "
            f"**{speedup_present:.2f}x** | {stats_present_on['bloom_skips']:,} | "
            f"{stats_present_on['disk_lookups']:,} | {stats_present_on['false_positives']:,} |"
        )

        print(header)
        print(separator)
        print(row_absent)
        print(row_present)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
