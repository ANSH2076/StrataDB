"""crash_test.py — Kill-the-process durability test for Strata.

This test proves that Strata does not lose acknowledged writes even
when the process is killed mid-flight.

How it works (each round):
  1. Start crash_writer.py as a subprocess.
  2. Read its stdout to collect acknowledged write indices.
  3. After a random number of acks (500–2000), kill the process with
     proc.kill() (SIGKILL on Unix, TerminateProcess on Windows).
  4. Reopen the database and verify that EVERY acknowledged key exists
     with the correct value.
  5. Keys that were written but not yet acknowledged are allowed to be
     present or absent — we do not check them.

Usage:  python3 tests/crash_test.py
"""

import os
import random
import shutil
import subprocess
import sys
import time

# Allow importing strata from the project root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strata import Strata

ROUNDS = 10
MIN_ACKS = 500
MAX_ACKS = 2000

# Path to crash_writer.py, next to this file.
WRITER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_writer.py")


def run_one_round(round_num: int, base_dir: str) -> bool:
    """Run one crash-and-recover round.  Returns True on PASS."""
    data_dir = os.path.join(base_dir, f"round{round_num}")
    os.makedirs(data_dir, exist_ok=True)

    target_acks = random.randint(MIN_ACKS, MAX_ACKS)

    # Start the writer subprocess.
    proc = subprocess.Popen(
        [sys.executable, WRITER_SCRIPT, data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Read acknowledged indices from stdout.
    acked_indices: list[int] = []
    for line in proc.stdout:
        idx = int(line.strip())
        acked_indices.append(idx)
        if len(acked_indices) >= target_acks:
            break

    # Kill the process — simulates a sudden crash.
    proc.kill()
    proc.wait()

    # Count how many .sst files existed when the process was killed.
    sst_count_at_kill = len([f for f in os.listdir(data_dir) if f.endswith(".sst")])

    # Reopen and verify.
    start = time.time()
    with Strata(data_dir, auto_compact=False) as db:
        recovery_ms = (time.time() - start) * 1000

        # Check sst file count after reopening (orphans should have been cleaned up)
        disk_ssts_after = sorted([f for f in os.listdir(data_dir) if f.endswith(".sst")])
        from strata.manifest import read_manifest
        manifest_ssts = sorted(read_manifest(data_dir) or [])
        manifest_matches_disk = (disk_ssts_after == manifest_ssts)

        missing = []
        wrong = []
        for idx in acked_indices:
            val = db.get(f"key{idx}")
            expected = f"value{idx}"
            if val is None:
                missing.append(idx)
            elif val != expected:
                wrong.append((idx, val, expected))

    passed = len(missing) == 0 and len(wrong) == 0 and manifest_matches_disk

    status = "PASS" if passed else "FAIL"
    print(f"  Round {round_num}: {status} — "
          f"{len(acked_indices)} acked writes, "
          f"{sst_count_at_kill} .sst at kill -> {len(disk_ssts_after)} after reopen, "
          f"recovery took {recovery_ms:.1f} ms")

    if not manifest_matches_disk:
        print(f"    MANIFEST MISMATCH: disk={disk_ssts_after}, manifest={manifest_ssts}")
    if missing:
        print(f"    MISSING keys: {missing[:10]}{'...' if len(missing) > 10 else ''}")
    if wrong:
        print(f"    WRONG values: {wrong[:10]}{'...' if len(wrong) > 10 else ''}")

    return passed


def main() -> None:
    print(f"Crash test: {ROUNDS} rounds, {MIN_ACKS}–{MAX_ACKS} acks each\n")

    base_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "crash_test_data"
    )
    # Start fresh each run.
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    all_passed = True
    for r in range(1, ROUNDS + 1):
        if not run_one_round(r, base_dir):
            all_passed = False

    print()
    if all_passed:
        print("ALL ROUNDS PASSED ✓")
    else:
        print("SOME ROUNDS FAILED ✗")
        sys.exit(1)

    # Clean up test data.
    shutil.rmtree(base_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
