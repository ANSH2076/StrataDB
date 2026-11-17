"""crash_writer.py — Helper for the crash test.

This script opens a Strata database and writes keys forever:
    key0 -> value0, key1 -> value1, ...

After EACH put returns (meaning the WAL has fsync'd), it prints the
index to stdout and flushes.  The crash test reads these printed
indices to know which writes were acknowledged before it kills us.

Usage:  python3 tests/crash_writer.py <data_directory>
"""

import sys
import os

# Allow importing strata from the project root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strata import Strata


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <data_dir>", file=sys.stderr)
        sys.exit(1)

    data_dir = sys.argv[1]

    # sync=True ensures every put is fsync'd before we print the index.
    # memtable_limit_bytes=2048 triggers frequent flushes to SSTables.
    # auto_compact=True with compaction_threshold=4 triggers background/inline compactions.
    db = Strata(
        data_dir,
        sync=True,
        memtable_limit_bytes=2048,
        auto_compact=True,
        compaction_threshold=4,
    )

    i = 0
    while True:
        db.put(f"key{i}", f"value{i}")
        # Print AFTER put returns — this means the WAL has the record.
        print(i, flush=True)
        i += 1


if __name__ == "__main__":
    main()
