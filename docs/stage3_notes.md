# Stage 3 — Learning Notes: Bloom Filters

These notes explain Stage 3 of Strata: implementing an in-memory
**Bloom filter** for each SSTable to prevent unnecessary disk seeks when
querying absent keys.

---

## 1. What is a Bloom Filter?

A Bloom filter is a space-efficient probabilistic data structure that tests
set membership.  It answers one question:
> **"Is this key in the set?"**

The possible answers are:
- **"Definitely NOT in the set"** (100% certainty; no false negatives).
- **"PROBABLY in the set"** (with a small, controlled probability of a false positive).

It consists of:
1. A **bit array** of size $m$ bits, initially all set to 0.
2. $k$ independent hash functions that each map an input key to an index in $[0, m - 1]$.

### Tiny Worked Example ($m = 16$ bits, $k = 3$ hashes)

Imagine a 16-bit array, initially all zeroes:
```
Bit Index: 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
Bits:     [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```

#### Step 1: Add `"cat"`
Suppose our 3 hash functions produce positions 2, 7, and 13:
- Set bit 2 = 1
- Set bit 7 = 1
- Set bit 13 = 1

```
Bits:     [0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0]
                 ^              ^                    ^
```

#### Step 2: Add `"dog"`
Suppose `"dog"` hashes to positions 7, 11, and 15:
- Bit 7 is already 1 (stays 1)
- Set bit 11 = 1
- Set bit 15 = 1

```
Bits:     [0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1]
                                ^           ^        ^     ^
```

#### Step 3: Query `"cat"`
Hash positions: 2, 7, 13.
- Bit 2 is 1
- Bit 7 is 1
- Bit 13 is 1
All bits are 1 $\to$ `"cat"` is **PROBABLY in the set**.

#### Step 4: Query `"fish"`
Suppose `"fish"` hashes to positions 1, 7, 13:
- Bit 1 is 0!
Since bit 1 is 0, `"fish"` **could never have been added**!
Result: **DEFINITELY NOT in the set** $\to$ skip disk entirely!

#### Step 5: False Positive Example (`"bird"`)
Suppose an absent key `"bird"` happens to hash to positions 2, 11, and 15:
- Bit 2 was set by `"cat"`
- Bit 11 was set by `"dog"`
- Bit 15 was set by `"dog"`
All three bits happen to be 1, so the filter says: `"bird"` MIGHT be here.
We do the disk read and discover `"bird"` is missing.  This is a **false positive**.

---

## 2. Why False Positives Happen, but False Negatives CANNOT

- **No False Negatives**: When key $K$ is added, all $k$ of its bit positions are set to 1.  We never clear or decrement bits.  Therefore, if $K$ was added, its $k$ bits will *always* be 1.  It is physically impossible for `might_contain(K)` to return `False`.
- **False Positives**: Other keys previously added to the filter set bits across the array.  By coincidence, an absent key might have all of its $k$ probe positions already set to 1 by a combination of other keys.

---

## 3. Why `hashlib.blake2b` and NEVER Python's `hash()`

Python's built-in `hash()` function is **randomized per process**.
On startup, Python picks a random seed (`PYTHONHASHSEED`) to randomize string hashing (protecting against hash collision Denial-of-Service attacks).

If we used `hash()`:
1. Process A writes an SSTable and computes filter bits with seed $S_1$.
2. The database reboots. Process B starts with random seed $S_2$.
3. When Process B queries an existing key, `hash()` produces completely different bit positions.
4. The probe sees 0-bits and returns `might_contain = False`!
5. The existing key is skipped on disk and becomes **permanently unreadable**!

Instead, we use `hashlib.blake2b(digest_size=16)`.  It is part of Python's standard library, fast, cryptographic, and $100\%$ deterministic across all Python versions, OSes, and restarts.

### The Double-Hashing Trick
Computing 7 separate cryptographic hashes per key would be slow.
Instead, we compute a single 16-byte `blake2b` digest and split it into two 64-bit integers ($h_1$ and $h_2$).
The $i$-th bit position is derived as:
$$\text{pos}_i = (h_1 + i \cdot h_2) \pmod{\text{num\_bits}}$$

In plain English: $h_1$ is our starting index, and $h_2$ is a constant step size.
Kirsch & Mitzenmacher (2006) proved that this gives the exact same false-positive rate as $k$ independent hash functions without extra hashing overhead.

---

## 4. Why Tombstones MUST Be in the Filter

In an LSM-tree:
- Keys deleted via `db.delete("user")` write a tombstone (`{"k": "user", "v": null}`).
- If `"user"` existed in `000001.sst` and was deleted in `000002.sst`, `000002.sst` holds the tombstone.

**What happens if tombstones are skipped from the Bloom filter?**
1. User queries `db.get("user")`.
2. Engine checks `000002.sst`.  Because the tombstone wasn't added to the filter, `might_contain("user")` returns `False`!
3. The engine assumes `000002.sst` has no relevant information and skips it.
4. The engine checks `000001.sst`.  Its Bloom filter says `"user"` might be here.
5. The engine reads `000001.sst` and returns the old value!
6. **Result: The deleted key has been resurrected!**

By adding tombstones to the filter, the engine visits `000002.sst`, finds `(True, None)`, stops immediately, and correctly returns `None`.

---

## 5. Why 10 Bits Per Key and 7 Hashes?

In Bloom filter theory:
$$k_{\text{optimal}} = \frac{m}{n} \ln(2) \approx \frac{m}{n} \times 0.693$$
For $m/n = 10$ bits per key:
$$k = 10 \times 0.693 \approx 6.93 \to 7 \text{ hashes}$$

The false positive rate is:
$$p \approx \left(1 - e^{-k \cdot n / m}\right)^k \approx \left(1 - e^{-7/10}\right)^7 \approx 0.0082 \approx 0.82\% \text{ (~1\%)}$$

10 bits per key is the industry standard balance point (used in RocksDB, Cassandra, and LevelDB):
- 1 million keys takes only $1.19\text{ MB}$ of RAM.
- 99 out of 100 absent lookups avoid touching the disk.

---

## 6. Engine Read Path & Ordering

When `db.get(key)` is called:
1. **MemTable Check**: $O(1)$ dictionary lookup. If found (value or tombstone), return immediately.
2. **SSTable Loop (Newest to Oldest)**:
   - **Step A: Range Check (`[first_key, last_key]`)**:
     Cheap comparison of two strings in memory. If outside, skip without touching disk or computing hashes.
   - **Step B: Bloom Filter Check (`table.might_contain(key)`)**:
     If `use_bloom=True` and the filter returns `False`, increment `bloom_skips` and skip disk reading entirely.
   - **Step C: Disk Lookup (`table.get(key)`)**:
     Increment `disk_lookups`. Read sparse block from disk. If absent, increment `false_positives`.

---

## 7. Real Benchmark Results

Running `tests/bench_reads.py` on 20,000 keys across 46 SSTables ($5,000$ absent lookups and $5,000$ present lookups):

| Lookup Type | Bloom OFF | Bloom ON | Speedup | Bloom Skips (ON) | Disk Lookups (ON) | False Positives (ON) |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| Absent Keys (5,000) | 0.3099 s | 0.0795 s | **3.90x** | 4,939 | 50 | 50 |
| Present Keys (5,000) | 0.2704 s | 0.2905 s | **0.93x** | 0 | 4,970 | 0 |

- **Absent keys**: Bloom filter skipped 4,939 out of 5,000 lookups, delivering a **3.90x speedup** by avoiding disk reads.
- **False positives**: Exactly 50 out of 5,000 ($1.0\%$), matching the theoretical ~1% target.
- **Present keys**: Slight $7\%$ hashing overhead when the key is known to exist and disk must be read anyway.

---

## 8. Known Limitations in Stage 3

1. **In-Memory Only (Rebuilt on Startup)**: Filters are not serialized to disk. On database startup, the engine scans each SSTable to rebuild the filter.
2. **Memory Overhead**: Each SSTable holds its own Bloom filter in RAM. While compact ($1.2\text{ MB}$ per $1\text{M}$ keys), total memory scales with the number of on-disk keys until compaction is introduced in Stage 4.

---

## 9. Decisions Not Specified in Prompt

1. **Double Hashing Stride Step**: Enforced `if h2 == 0: h2 = 1` so the stride is always non-zero.
2. **Bit Array Storage**: Stored as `bytearray(num_bits // 8 + 1)` with bit manipulation `byte_idx = pos // 8` and `bit_offset = pos % 8`.
3. **Minimum Filter Size**: Enforced `num_bits = max(64, n_keys * bits_per_key)` to avoid invalid bit lengths for empty or tiny tables.
4. **SSTable Scan Counting**: Added a fast line count pass `sum(1 for _ in f)` during open to size the filter accurately before populating it.
5. **Stats Schema Additions**: Added `bloom_enabled` (bool), `bloom_skips` (int), `disk_lookups` (int), and `false_positives` (int) without modifying any existing keys.
6. **Range Check Priority**: Executed the range check `[first_key, last_key]` prior to the Bloom filter check to skip obviously out-of-range keys without computing hashes.
7. **Benchmark Setup**: Chose $16\text{ KB}$ memtable limit yielding 46 SSTables across 20,000 keys, and interleaved absent test keys (`user_{i}_missing`) so they fall within SSTable bounds to properly test the Bloom filter.

---

## 10. Five Interview Questions & Answers

**Q1: What is a Bloom filter and why is it used in LSM-tree databases?**
A Bloom filter is a space-efficient probabilistic data structure used to test set membership. In an LSM-tree, reads must search backwards through multiple SSTables on disk. A Bloom filter in RAM allows the engine to skip checking SSTables that definitely do not contain the key, eliminating costly disk seeks.

**Q2: Can a Bloom filter return a false negative? Why or why not?**
No. When a key is inserted, all $k$ of its hash bit positions are set to 1. Because bits are never cleared, all $k$ bits for an inserted key will always evaluate to 1. If any probe bit is 0, the key was never inserted.

**Q3: Why can't we use Python's built-in `hash()` function for database filters?**
Python randomizes string hash seeds on every process start (`PYTHONHASHSEED`) to prevent collision attacks. A filter built with one seed would produce different bit positions in another process, resulting in catastrophic false negatives where existing keys are deemed absent. We use `hashlib.blake2b` for deterministic hashing.

**Q4: Why must deleted keys (tombstones) be inserted into the Bloom filter?**
In an LSM-tree, deletions are recorded as tombstones in newer SSTables to shadow older records. If tombstones were excluded from the Bloom filter, queries for deleted keys would skip the newer SSTable and fall through to older tables containing the old value, resurrecting deleted data.

**Q5: How does double hashing work and why is it useful?**
Double hashing generates $k$ hash probe positions from two base hash values using $\text{pos}_i = (h_1 + i \cdot h_2) \pmod m$. This provides the same statistical randomness and low false-positive rate as $k$ independent hash functions while requiring only a single 16-byte cryptographic digest.
