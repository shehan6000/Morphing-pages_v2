# Morphing Pages

**Fine-grained, recency-driven encoding transitions for columnar storage.**

Morphing Pages is a page-level encoding scheme in which each page independently tracks a decayed *temperature* signal from observed reads and writes, and autonomously migrates through a three-state encoding spectrum (HOT → WARM → COLD) using a copy-on-write transition mechanism with a hysteresis guard against thrashing.

This repository contains the policy simulation, a standalone C++20 concurrency-safe copy-on-write component, and a from-scratch disk-backed storage engine (real `pwrite`/`pread`, real zlib) that validates the mechanism end-to-end.

> Companion code for the paper *"Morphing Pages: Fine-Grained, Recency-Driven Encoding Transitions for Columnar Storage"* (Shehan Liyanaarachchi).
> The same encoding-transition idea is extended to vector indexing in the sister project, [Morphing Vectors](../morphing_vectors).

---

## Motivation

Time-series and append-heavy analytical workloads have a predictable lifecycle: data is mutable and hot right after ingestion, then cools within hours to days into a read-mostly, scan-heavy pattern. Existing systems (TimescaleDB, InfluxDB TSM, Delta Lake, Iceberg) handle this at **coarse granularity** and with a **schedule-driven trigger**:

| Limitation | Consequence |
|---|---|
| **Granularity:** the unit of migration is a chunk, shard, or file | Cold rows and hot rows in the same chunk are forced into the same encoding |
| **Trigger:** compression fires on age or size thresholds | A page that is still read heavily gets compressed anyway and pays decode cost on every read |

Morphing Pages makes the encoding decision **per page**, driven directly by a decayed measure of recent read/write activity.

## How It Works

### Temperature signal

Each page keeps a scalar temperature `τ`, decayed once per tick and bumped on access:

```
τ ← τ · δ                 (every tick, δ ∈ (0,1))
τ ← τ + w_write           (on write)
τ ← τ + w_read            (on read),   w_write > w_read > 0
```

### Three encoding states

| State | Encoding | Optimized for |
|---|---|---|
| **HOT** | Row-major / uncompressed | Writes, point updates |
| **WARM** | Columnar, dictionary + bit-packing (light compression) | Cheap decode |
| **COLD** | Delta transform + heavy compression | Storage efficiency, scan-heavy reads |

### Transition rules

- **Demotion:** HOT → WARM → COLD when `τ < θ_demote` for `h` consecutive ticks. The hysteresis window `h` prevents flapping near the threshold.
- **Promotion:** *asymmetric and immediate.* Any write to a WARM or COLD page re-encodes it directly to HOT, bypassing the hysteresis window.

### Copy-on-write transcoding (transactional safety)

Concurrent readers must never observe a half-transcoded page:

1. Allocate a new buffer and encode into it (old page untouched).
2. Atomically swap the page pointer to publish the new version.
3. Retain the old version for in-flight readers.
4. Reclaim the old version once no readers remain (reference counting).

Implemented with `std::atomic<std::shared_ptr<const EncodedPage>>`, so the **read path is lock-free**. This is a well-established MVCC-style pattern; the contribution is applying it at page-encoding granularity.

---

## Results

### 1. Policy simulation (Python, 200 pages, 200 ticks)

Config: `δ=0.90`, `w_write=3.0`, `w_read=1.0`, `θ_demote=0.5`, `h=3`, fixed-schedule trigger = 20 idle ticks.

| Policy | Read latency (µs) | Storage (% raw) | Morph CPU (s) |
|---|---|---|---|
| Adaptive | 0.23 | 94.42 | 0.135 |
| Static-uncompressed | 0.07 | 100.00 | 0.000 |
| Fixed-schedule | 5.46 | 94.42 | 0.079 |

- Matches fixed-schedule storage exactly, with ~**24×** lower simulated read latency at 8 KB pages.
- Exactly **2 transitions per page** (HOT→WARM→COLD), **zero thrashing**.
- **Sensitivity sweep** (27 configs over `δ`, `h`, `θ_demote`): zero thrashing everywhere; speedup ranged **2.10× to 65.92×**. Adaptive never lost to fixed-schedule on read latency or storage.
- **Scaling** (200 to 20,000 pages): morph CPU scales linearly, stays at roughly 31–40% of simulated work, no thrashing.
- At DuckDB's real 256 KB block size the *simulated* speedup grows to ~1236×, because heavy-compression decode cost scales with payload size.

> The simulation models no I/O cost, so its absolute speedups **overstate** the real effect. It is used for cheap sensitivity and scaling checks, not as the headline number. See below.

### 2. Real disk-backed engine (headline result)

A from-scratch engine using real `pwrite`/`pread` syscalls and real zlib compression. 5 independent runs, 50 pages, 200 ticks, ~8 KB/page.

| Policy | Avg. read latency (µs) | Logical storage (% raw) |
|---|---|---|
| **Adaptive** | 1.5–1.9 (mean ~1.6) | 94.43% |
| Fixed-schedule | 7.0–8.2 (mean ~7.5) | 94.43% |
| Static-uncompressed | ~1.1 | 100.00% |

**Measured speedup: ~4.0–5.0×** (adaptive vs. fixed-schedule), stable across runs, with storage parity replicated exactly.

**Why smaller than simulation?** Every real read pays a constant `pread` + syscall overhead regardless of policy, which dilutes the compression-driven advantage that a pure simulation cannot see. The real figure is the one to cite.

### 3. Concurrency correctness (standalone C++20)

- 8 reader threads, 1 writer forcing HOT promotions, 1 background thread driving state-machine ticks, all on a single page for 2 s per run.
- Every read checks a canary value framed around the payload to detect torn reads.
- **~15–19 million reads per run, zero torn pages.**
- Rebuilt under **ThreadSanitizer**: it caught a real bug (reader threads mutating temperature bookkeeping without synchronization), fixed with a mutex scoped to the bookkeeping only. After the fix, **zero data races** across ~450K instrumented reads.

### 4. Cold-read re-promotion (negative-ish result)

A decayed cold-read-pressure counter promoting COLD → WARM worked as designed but reduced average read latency during cold bursts by only **~3.4%** (20.01 µs → 19.33 µs). The first read of any burst always pays cold-decode cost, and at 1,024-value pages the WARM/COLD decode gap is small. Probably not worth the added complexity at this scale.

---

## What's Real in the Disk-Backed Engine

| File | What it does |
|---|---|
| `src/include/block_file.hpp` | Actual file on disk with offset-based POSIX `pwrite`/`pread`; safe for concurrent access with no locking on the read path |
| `src/include/real_encoding.hpp` | Real zlib compression (no placeholders), same HOT/WARM/COLD pipeline as the prototype, with a delta transform before COLD |
| `src/include/persistent_morphing_page.hpp` | The same `atomic<shared_ptr<const T>>` COW discipline validated under TSan, applied to disk-resident pages |
| `benchmark/engine_benchmark.cpp` | Replays the write-then-decay workload; every latency is a real `std::chrono` measurement around a real `pread` + `zlib::uncompress` |

## What This Does *Not* Establish

- It is **not DuckDB**, and not a production engine: no checkpointing, MVCC, query planner, or crash recovery.
- `block_file.hpp` is **append-only with no free list or compaction**. Physical file size grows with every transcode; *logical* size (sum of live region lengths) is reported separately and is the number used for storage efficiency.
- The concurrency test is a unit-level correctness test, not a systems evaluation.
- The DuckDB integration (`morphing_pages_ext/`) is **designed but unbuilt**.
- Results use a **synthetic** write-then-decay trace modeled loosely on the TSBS ingest-then-analyze pattern; no real TimescaleDB/InfluxDB comparison has been run yet.

---

## Build and Run

No submodule, no Docker, no multi-GB dependency. Builds in seconds.

```bash
# Requires g++ with C++20 support, pthreads, and zlib (libz)
mkdir -p build
g++ -std=c++20 -O2 -pthread benchmark/engine_benchmark.cpp -o build/engine_benchmark -lz
./build/engine_benchmark
```

Run the Python policy simulation (sweep, scaling, bursty trace):

```bash
python3 benchmark.py
```

Run the concurrency test under ThreadSanitizer:

```bash
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread <concurrency_test>.cpp -o build/tsan_test
./build/tsan_test
```

> Replace `<concurrency_test>.cpp` and check script names against your actual file names.

## Repository Layout

```
morphing_pages/
├── src/include/
│   ├── morphing_page.hpp              # standalone COW + temperature state machine (C++20)
│   ├── block_file.hpp                 # real file I/O (pwrite/pread, append-only)
│   ├── real_encoding.hpp              # real zlib HOT/WARM/COLD pipeline
│   └── persistent_morphing_page.hpp   # COW applied to disk-resident pages
├── benchmark/
│   └── engine_benchmark.cpp           # real-I/O benchmark
├── benchmark.py                       # Python policy simulation, sweep, scaling
├── morphing_pages_ext/                # DuckDB extension skeleton (structural tests only)
│   └── morphing_block_manager_test.cpp
├── DUCKDB_INTEGRATION_DESIGN.md       # integration design and rejected paths
```

> Adjust this tree to match the actual repository.

## DuckDB Integration Design

Three paths were evaluated against DuckDB's `RowGroup → ColumnData → ColumnSegment` architecture:

| Path | Verdict | Reason |
|---|---|---|
| **A: Custom `CompressionFunction`** | Rejected | The checkpointer only re-processes *dirty* segments; a cold, read-only segment is never dirty, so there is no way to force re-evaluation as a page cools |
| **B: Custom `BlockManager` subclass** | **Recommended** | Owns raw block I/O directly and sidesteps checkpoint timing |
| **C: SQL-level maintenance job** | Fallback | Public API only, but operates at partition granularity, closer to the coarse-grained baselines this work argues against |

`MorphingBlockManager` is a decorator around DuckDB's `BlockManager`, routing reads/writes for registered segments through the concurrency-tested page mechanism. It currently compiles only against **stub types**, and its method signatures are largely unverified against real DuckDB headers. A structural test covers registration, routing, and concurrent access under release and TSan builds (this caught a real bug: a `std::mutex`-holding page being non-movable, fixed with `unique_ptr` indirection).

See `DUCKDB_INTEGRATION_DESIGN.md` for the recommended build order.

## Limitations and Future Work

- [ ] Build the real `BlockManager` integration in DuckDB and re-run the concurrency test against it
- [ ] TSBS evaluation against real TimescaleDB and InfluxDB, with a DuckDB fixed-schedule control condition
- [ ] Validate on bursty, multi-modal workloads (the policy was swept only on write-then-decay)
- [ ] Measure real background-thread interference with foreground queries (current scaling cost is single-threaded simulation)
- [ ] Add compaction / free-list management to `block_file.hpp`
- [ ] Integrate crash recovery with a real engine's WAL/checkpoint protocol (a crash before the pointer swap leaves the old page authoritative; a crash after leaks the orphaned old page but does not corrupt data)

## Related Work (brief)

Closest prior work is **AdaCom** (Windheuser et al., EDBT 2024), which does access-driven, batch-periodic segment compression in DuckDB without addressing in-place re-encoding safety, and the adaptive segment encoding of **Fehér et al.** in Hyrise. Broader lineage includes database cracking, self-driving databases, and hybrid row/column layouts. See the paper for the full discussion.



## Author

**Shehan Liyanaarachchi**, Independent Researcher

## License

Add your license here (e.g., MIT or Apache-2.0) and include a `LICENSE` file in the repo root.
