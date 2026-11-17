# Stage 5 — Learning Notes: Live Dashboard, Server Concurrency, and Glass-Box Tracing

These notes cover Stage 5 of Strata: building the live HTTP server, the single-file
vanilla "glass box" dashboard, read tracing, and simulated crash diagnostics.

---

## 1. Why HTTP Polling Over WebSockets or SSE

For a local database dashboard, we chose **short HTTP polling** (every 400ms) with `?since=<event_id>`:

1. **Simplicity and Standard Library Only**:
   - WebSockets require framing, masking, and persistent full-duplex protocol state, none of which exist in Python's standard library (`http.server`). Supporting WebSockets without third-party dependencies would require writing hundreds of lines of custom RFC 6455 framing code.
   - Server-Sent Events (SSE) keep HTTP connections open indefinitely. When combined with synchronous worker threads in `ThreadingHTTPServer`, open connections tie up worker threads and complicate browser reload and teardown cycles.
2. **Crash Resilience and Automatic Reconnect**:
   - When the user triggers `/api/crash` or the server is restarted, persistent WebSocket connections break and require reconnect loops, backoff timers, and error handlers.
   - HTTP polling naturally recovers: if a request fails during a reboot, the next poll 400ms later simply succeeds without any connection lifecycle ceremony.
3. **Differential Updates with `?since=<id>`**:
   - Each poll passes `since=last_event_id`. The server returns only newly generated events since that ID. If nothing happened, the response payload is tiny (~200 bytes of stats and `events: []`), yielding negligible CPU and network overhead on `localhost`.

---

## 2. Why One Lock: Concurrency and Thread Safety

In `strata/server.py`, a single global `threading.Lock` protects every database interaction:

```python
with self.state.lock:
    result = self.state.engine.get(key)
```

### Why not fine-grained locks or lock-free designs?
1. **LSM Lifecycle Invariants**:
   - In Strata, operations are not independent memory updates. A write (`put`) might trigger an automated flush or compaction. A flush updates the WAL, writes an SSTable, updates the MANIFEST, and replaces the in-memory MemTable.
   - A read (`get` or `trace_get`) reads the MemTable and iterates across active SSTable readers.
   - If a background compaction replaces 4 SSTables with 1 new table while a concurrent reader is iterating through the old tables, fine-grained locking requires complex reader-writer locks, reference-counting on table handles, and lock hierarchies to prevent deadlocks.
2. **Explainability and Correctness**:
   - As a first-year student project, correct durability and crash safety take precedence over high concurrent write throughput.
   - A single coarse lock makes the system provably race-free: exactly one thread accesses or mutates engine state at a time.
3. **Atomic Crash Simulation**:
   - When `/api/crash` is called, the server holds the lock, samples up to 200 keys, calls `_abandon()`, reopens the engine, and verifies integrity. With a single lock, no concurrent write can slip in between the crash and the audit.

---

## 3. Localhost Binding and Security

The HTTP server binds strictly to `127.0.0.1` by default:

```python
server_address = (args.host, args.port)  # default host="127.0.0.1"
```

1. **Why `127.0.0.1` instead of `0.0.0.0`?**
   - Binding to `0.0.0.0` exposes the database server to all network interfaces (local WiFi, LAN, Docker bridges).
   - Because Strata is an unauthenticated development database with administrative endpoints (`/api/crash`, `/api/reset`), exposing it to the network would allow anyone on the same local network to wipe data or inspect storage.
2. **Payload Protection**:
   - Every POST endpoint checks `Content-Length` and rejects any payload exceeding 1MB (`1,048,576` bytes) with HTTP 400. This prevents memory exhaustion from unbounded requests.
3. **Safe Reset Boundary**:
   - The `/api/reset` endpoint validates that `data_dir` is neither empty nor root (`/`). It strictly deletes files matching `*.sst`, `*.tmp`, `wal.log`, and `MANIFEST`, refusing to touch any other system files.

---

## 4. The Glass-Box Trace Mechanism

To visualize how reads traverse an LSM-tree without distorting benchmark metrics, Strata implements `trace_get(key)` alongside `get(key)`.

### Shared Lookup Core
Both methods share an internal method: `_lookup(key, trace=None)`:
- When called from `get(key)`, `trace is None`. The lookup increments performance counters:
  - `bloom_skips`: incremented when a bloom filter rejects an SSTable.
  - `disk_lookups`: incremented when an SSTable file is physically opened and searched.
  - `false_positives`: incremented when the bloom filter said "maybe" but the key was not found.
- When called from `trace_get(key)`, `trace is not None`. **None of the performance counters are incremented!** This ensures monitoring or dashboard UI lookups do not pollute production operational stats.

### Step-by-Step Step Recording
`trace_get(key)` returns a structured sequence of diagnostic steps:
1. **MemTable Search**:
   - `action: "hit"` (found active value or tombstone).
   - `action: "miss"` (not in memory).
2. **SSTable Inspection (Newest to Oldest)**:
   - **Range Check**: If key is outside `[first_key, last_key]`, action is `"range_skip"`.
   - **Bloom Filter Check**: If bloom filter rejects key, action is `"bloom_skip"`.
   - **Sparse Index & File Search**: If bloom filter matches, action is `"sparse_search"`. Binary search narrows the offset to at most 16 records.
     - Found active value: `action: "hit"`.
     - Found tombstone: `action: "tombstone"`. (Search stops immediately; prevents resurrection).
     - Not found: `action: "disk_miss"` (false positive).
3. **Not Found**:
   - If all tables are checked without a hit: `action: "not_found"`.

---

## 5. XSS Prevention via `textContent`

The Strata dashboard (`dashboard/index.html`) is completely self-contained with zero external dependencies.

### Security Requirement: Never Use `innerHTML`
User keys and values can contain arbitrary characters, including HTML tags and JavaScript payloads:
```json
{"key": "<script>alert(1)</script>", "value": "<img src=x onerror=alert('pwned')>"}
```

If the dashboard used `innerHTML` to display keys, values, or event logs, the browser would parse and execute the script tag or image handler.

### The Fix: Strictly DOM APIs and `textContent`
In `dashboard/index.html`:
- Elements are created using `document.createElement('div')`, `document.createElement('span')`, etc.
- Text is inserted strictly via `element.textContent = data`.
- `textContent` tells the browser to treat the data purely as raw text, automatically escaping `<>&"'` without running the HTML parser.

---

## 6. Interview Questions and Answers

### Q1: Why does an LSM-tree use tombstones for deletion instead of deleting in-place?
**Answer:** In an LSM-tree, SSTables on disk are immutable. In-place deletion would require finding the key in an existing sorted file, rewriting the entire file, and invalidating sparse indexes. That turns an $O(1)$ append into an expensive $O(N)$ random rewrite. Instead, LSM-trees append a tombstone record (`value=None`) to the WAL and MemTable. The tombstone shadows older records during reads. Obsolete data and the tombstone itself are only purged later during compaction when older files are merged.

### Q2: Why must compaction drop tombstones only when merging the oldest layer?
**Answer:** If a tombstone is dropped while older SSTables still contain the key, the next lookup will skip the missing tombstone and find the obsolete value in the older SSTable! This is called **tombstone resurrection**. Strata currently implements full-database compaction (merging all SSTables into one), which guarantees no older SSTables exist, so surviving tombstones can be safely discarded. In multi-level leveled compaction, a tombstone can only be dropped if it is proven that no lower level contains that key.

### Q3: What is the purpose of the MANIFEST file and how does it guarantee atomicity?
**Answer:** The MANIFEST records the definitive list of active SSTables. When a flush or compaction produces new SSTables, deleting old files and adding new ones cannot be done simultaneously across multiple file deletions and creations. A crash mid-way would leave the database in an inconsistent state. Strata writes the new file list to `MANIFEST.tmp`, `fsync`s the file, and performs an atomic POSIX rename to `MANIFEST`. On restart, the engine only loads tables listed in `MANIFEST`, deleting any unreferenced `.sst` files as orphans.

### Q4: Why can a Bloom filter have false positives but never false negatives?
**Answer:** A Bloom filter hashes a key with $k$ independent hash functions and sets the corresponding bits in a bit array.
- If a key was added, all $k$ bits were set to 1. When querying, if even one bit is 0, the key **definitely was never added** (zero false negatives).
- However, other distinct keys may have collectively set those same $k$ bits to 1. When querying a key that was never added, all $k$ bits might already be 1, leading to a "maybe present" answer (false positive).

### Q5: What is the write amplification vs. read amplification tradeoff in LSM-trees?
**Answer:**
- **Write Amplification:** Bytes written to disk divided by bytes written by the user. In Strata, a write is written to the WAL, flushed to an SSTable, and rewritten during compaction. Compacting more frequently increases write amplification.
- **Read Amplification:** Number of disk reads required to answer a single `get(key)`. Without compaction, SSTables accumulate, and reads must check every file (high read amplification). Compaction reduces the number of SSTables, keeping read amplification low.
- LSM-trees trade background write amplification (compaction) to keep read amplification low while maintaining $O(1)$ sequential write performance.

---

## 7. Unspecified Decisions and Implementation Rationale

1. **Dashboard Refresh Interval (400ms)**:
   - *Decision*: Set polling to 400ms with differential `since=<id>` event fetching.
   - *Rationale*: 400ms feels snappy and near-instant to human observation while keeping local loopback CPU usage below 0.1%.
2. **Ephemeral Port 0 in Unit Tests**:
   - *Decision*: In `tests/test_server.py`, the test server binds to `("127.0.0.1", 0)`.
   - *Rationale*: Asking the OS kernel for an ephemeral port prevents test flakiness from port collisions if port 8000 or 8099 is already occupied on the test machine.
3. **Simulated Crash Scope**:
   - *Decision*: `/api/crash` samples up to 200 random keys, calls `_abandon()`, reloads the engine from disk, and compares the values.
   - *Rationale*: Provides immediate in-browser verification that uncorrupted committed data survives an unexpected shutdown, while including clear disclaimer badges indicating that it is a process-level simulation rather than an OS-level power loss.
4. **Max Payload Limit (1MB)**:
   - *Decision*: Strictly enforce 1MB maximum payload on POST requests in `strata/server.py`.
   - *Rationale*: Prevents denial-of-service from large malicious inputs or runaway batch loads.
