"""Compaction logic for Strata.

Compaction merges multiple SSTables into a single sorted SSTable,
dropping obsolete (overwritten) versions of keys and dropping tombstones
(delete markers).

Why dropping tombstones is safe ONLY in a full merge:
A tombstone says: "This key was deleted; ignore older versions in older tables."
If we were doing a partial compaction (merging only some tables), an older
table below the merged set might still contain an old value. Dropping the
tombstone would resurrect that deleted value!
In Strata Stage 4, compaction ALWAYS merges ALL SSTables. Because nothing
older exists below the merged set, and the MemTable is strictly NEWER than
every SSTable, no older version can possibly exist. Therefore, tombstones
can be safely discarded forever.
"""

from __future__ import annotations

import heapq
from typing import Any, Dict, Generator, List, Optional, Tuple

from strata.sstable import SSTable


def merge_tables(
    tables_newest_first: List[SSTable],
    stats: Dict[str, Any],
) -> Generator[Tuple[str, Optional[str]], None, None]:
    """K-way merge of all SSTables yielding (key, value) in ascending key order.

    Args:
        tables_newest_first: List of SSTables sorted newest-to-oldest.
        stats: Mutable dict updated in-place with compaction statistics:
               - records_in: total records read across all input tables
               - records_out: surviving records yielded
               - overwritten_dropped: older versions of keys shadowed and dropped
               - tombstones_dropped: delete markers safely eliminated

    Algorithm:
      1. For each table at index `age_rank` (0 = newest, 1 = 2nd newest, ...),
         wrap its sorted iterator `items()` into tagged tuples:
         `(key, age_rank, value)`.
      2. Use `heapq.merge` to merge these pre-sorted streams in O(N log K) time,
         where K is the number of tables and N is total records.
      3. For any duplicated key, tuples sort first by `key` (ascending), then
         by `age_rank` (ascending). Because `age_rank` 0 is the newest table,
         the NEWEST version of every key appears first in the merged stream!
      4. As we iterate through the merged stream, we remember `last_key`:
         - If `key == last_key`, this is an older, shadowed version of the key.
           Increment `overwritten_dropped` and skip it.
         - If `key != last_key`, this is the latest version.
           Update `last_key = key`.
           - If `value is None` (tombstone), it has successfully shadowed any
             older versions. Because all tables are merged, drop it and
             increment `tombstones_dropped`.
           - If `value is not None`, yield `(key, value)` and increment `records_out`.
    """
    stats["records_in"] = 0
    stats["records_out"] = 0
    stats["overwritten_dropped"] = 0
    stats["tombstones_dropped"] = 0

    if not tables_newest_first:
        return

    # Tag each table's stream with its age_rank (0 = newest).
    def tagged_stream(
        table: SSTable,
        age_rank: int,
    ) -> Generator[Tuple[str, int, Optional[str]], None, None]:
        for k, v in table.items():
            yield (k, age_rank, v)

    iterators = [
        tagged_stream(tbl, rank)
        for rank, tbl in enumerate(tables_newest_first)
    ]

    # heapq.merge requires the inputs to already be sorted. Since each SSTable
    # is sorted by key ascending, each tagged stream is sorted by (key, age_rank).
    merged_stream = heapq.merge(*iterators, key=lambda item: (item[0], item[1]))

    last_key: Optional[str] = None
    has_seen_any_key = False

    for key, _age_rank, value in merged_stream:
        stats["records_in"] += 1

        if has_seen_any_key and key == last_key:
            # An older version of the same key appeared. It is shadowed by
            # the newer version we already processed.
            stats["overwritten_dropped"] += 1
            continue

        # This is the newest version of this key across all tables.
        last_key = key
        has_seen_any_key = True

        if value is None:
            # Tombstone. Safe to drop because we are merging ALL tables!
            stats["tombstones_dropped"] += 1
        else:
            stats["records_out"] += 1
            yield (key, value)
