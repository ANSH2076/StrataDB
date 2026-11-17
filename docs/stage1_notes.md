# Stage 1 — Learning Notes

These are my notes on how the Stage 1 database works.  Everything here
is about the WAL (Write-Ahead Log), the MemTable, and crash recovery.

---

## What each file does

| File | Job |
|------|-----|
| `strata/wal.py` | The Write-Ahead Log.  An append-only file on disk.  Every put or delete is written here first, with a checksum, so we can recover after a crash. |
| `strata/memtable.py` | The MemTable.  A Python dict that holds the latest version of every key in memory.  Fast reads come from here. |
| `strata/engine.py` | The `Strata` class — the public API.  Ties the WAL and MemTable together.  On startup, replays the WAL to rebuild the MemTable. |
| `strata/__init__.py` | Just re-exports `Strata` so you can write `from strata import Strata`. |
| `tests/test_engine.py` | pytest tests covering basic operations, recovery, corruption, and special characters. |
| `tests/crash_writer.py` | Helper script that writes keys forever and prints which ones it finished. |
| `tests/crash_test.py` | Kills the writer mid-flight and checks that every acknowledged key survived. |

### How the pieces connect

```
  User code
     │
     ▼
  Strata (engine.py)      ← public API: put / get / delete
     │          │
     ▼          ▼
   WAL        MemTable     ← WAL writes to disk, MemTable lives in memory
 (wal.py)   (memtable.py)
```

On **write**: the engine writes to the WAL first, then updates the MemTable.

On **read**: the engine just asks the MemTable (fast, O(1) dict lookup).

On **startup**: the engine replays the WAL line by line to rebuild the MemTable.

---

## The WAL record format

Each line in `wal.log` looks like this:

```
a1b2c3d4 {"op":"put","k":"name","v":"Alice"}
```

Breaking it down:

- `a1b2c3d4` — CRC32 checksum of the JSON text, as 8 lowercase hex digits.
- ` ` — a single space separator.
- `{"op":"put","k":"name","v":"Alice"}` — the operation as JSON.
- `\n` — a newline marks the end of the record.

For deletes, the JSON is `{"op":"del","k":"name"}` (no `"v"` field).

The checksum is computed over the JSON bytes using `zlib.crc32`.

---

## Why we write to the WAL before the MemTable

Imagine the opposite order:

1. Update the MemTable (in memory).
2. Write to the WAL (on disk).
3. **Crash happens before step 2 finishes.**

The user thinks the write succeeded (step 1 returned), but the WAL has
no record of it.  On restart, replay rebuilds the MemTable *without*
that write.  The data silently disappears.

By writing to the WAL first:

1. Write to the WAL (on disk) — fsync'd.
2. Update the MemTable (in memory).
3. Return to the user.

If we crash between steps 1 and 2, the write is in the WAL.  On
restart, replay will apply it.  If we crash before step 1 finishes,
the incomplete WAL record fails the CRC check and gets discarded — the
user never saw it complete, so no data is lost.

**Rule: The WAL is the source of truth.  The MemTable is a cache.**

---

## flush() vs fsync()

These are two different things:

| Call | What it does |
|------|-------------|
| `file.flush()` | Pushes Python's internal buffer into the **OS kernel buffer**.  After flush, the data has left your process — but it might still be sitting in the kernel's RAM, not on the physical disk yet. |
| `os.fsync(fd)` | Asks the OS kernel to push its buffer to the **physical disk** (or at least to the disk's own write cache).  This is the expensive one. |

**What each protects against:**

- `flush()` alone protects against your Python process crashing (the OS
  still has the data).
- `fsync()` protects against the whole machine losing power (the data
  made it to the disk hardware).

That is why we do both: `flush()` then `fsync()`.  Without fsync, a
power failure could lose the last few writes even though Python already
"flushed" them.

The `sync=True` flag in Strata controls whether we call `fsync()`.
Setting it to `False` is faster but less safe — fine for tests, risky
for real data.

---

## What is a torn write and how the checksum handles it

A **torn write** happens when the process crashes *in the middle of
writing a WAL record*.  For example:

```
a1b2c3d4 {"op":"put","k":"name","v":"Al       ← process killed here
```

The last line is incomplete — it has no closing `"}` and no trailing
newline.  The file now has garbage at the end.

**How we detect it:**

During replay, we read line by line.  An incomplete line either:

1. Has no trailing `\n` (we detect this explicitly), or
2. Has a `\n` but the JSON is truncated, so `json.loads()` fails, or
3. The CRC does not match because the data is partial.

In all three cases, replay stops at that line.  We then **truncate** the
file to the byte offset of the last good record.  This removes the
garbage so that the next append starts cleanly.

**Key insight:** We only ever lose the *unfinished* write.  Every
write that completed and returned to the user has a full valid line in
the WAL.

---

## What is a tombstone and why delete does not remove the key

When you call `db.delete("name")`, we do NOT remove `"name"` from the
dict.  Instead, we set its value to `None`.  This `None` value is called
a **tombstone**.

**Why not just delete it?**

In Stage 2, old data lives in SSTables on disk.  If we just removed the
key from the MemTable, a later read would not find it in memory, then
look at the SSTables and find the old value — returning stale data as if
the delete never happened.

The tombstone says: "This key was explicitly deleted.  Stop looking."

The `get()` method returns a tuple `(found, value)`:

| Result | Meaning |
|--------|---------|
| `(True, "Alice")` | Key exists with this value. |
| `(True, None)` | Key was deleted (tombstone).  Stop looking in older tables. |
| `(False, None)` | MemTable knows nothing.  Check older tables (Stage 2). |

The public `Strata.get()` hides this complexity — it returns the string
value or `None`.

---

## Five likely interview questions

**Q1: Why use a Write-Ahead Log instead of just writing to the dict?**

A dict lives in memory and disappears when the process ends.  The WAL
is a file on disk.  By writing every change to the WAL before updating
the dict, we can replay the WAL after a crash and get the dict back.
The WAL is append-only (no random seeks), which makes it fast.

**Q2: What happens if the power goes out in the middle of a write?**

The last WAL record may be incomplete (a "torn write").  On startup, the
replay function reads line by line and checks each CRC32 checksum.  The
incomplete record fails the check and gets truncated.  All earlier writes
are safe because they have valid checksums and trailing newlines.

**Q3: Why fsync and not just flush?**

`flush()` only moves data from Python's buffer to the OS kernel's
buffer.  The kernel may keep it in RAM for a while.  `fsync()` forces
the kernel to write it to the physical disk.  Without fsync, a power
failure could lose data that Python already "flushed".

**Q4: Why use tombstones instead of deleting the key from the dict?**

In an LSM-tree, old versions of data live in older sorted files
(SSTables).  If you just remove the key from the in-memory table, a
read would fall through to those older files and find the old value.  A
tombstone marks the key as deleted so the read path knows to stop
looking.

**Q5: What is the time complexity of your operations?**

- `put()`: O(1) for the dict update, plus the cost of one disk write (the WAL append).
- `get()`: O(1) — a dict lookup.
- `delete()`: Same as put — O(1) dict update plus one disk write.
- Recovery: O(n) where n is the number of records in the WAL, because we replay them all.
- `sorted_items()`: O(n log n) for the sort — only used when flushing to an SSTable (Stage 2).
