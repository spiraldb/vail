# Matrix layout: local before/after diagnostics

These results are **local diagnostics, not a new Hail/cloud comparison**. The
host has 14 logical CPUs and 96 GiB RAM. Both Vail builds use 32 runtime workers
and 14 source partitions. Queries run serially; each capture has three warmups
and at least three measured seconds, sampled at 10 Hz. The slowest old filter
capture contains only two measured queries. Both Python environments use 3.11.13.
Compilers and tests finished before measurement. Version order alternates by size.

The baseline release wheel has the same Matrix Python source as commit
`d72a0869d5de78286af02f57f925dd87315de4f1` (runtime from `76d6a51`). Native binary
SHA-256 values are in `comparison.csv` and each capture's `worker.json`; full
commands are in `provenance.json`, with the collector scripts under `harness/`.
The new build uses DataFusion masks, positional zips, and Arrow batched gather.

## Median query time and peak process-tree RSS

| Rows | Samples | Query | Before ms | After ms | Speedup | Before RSS MiB | After RSS MiB |
|---:|---:|---|---:|---:|---:|---:|---:|
| 100,000 | 10 | Entry sum | 22.19 | 16.62 | 1.34x | 664 | 337 |
| 100,000 | 10 | Row + column filter (count rows) | 24.92 | 16.49 | 1.51x | 688 | 272 |
| 100,000 | 20 | Entry sum | 37.48 | 22.71 | 1.65x | 911 | 450 |
| 100,000 | 20 | Row + column filter (count rows) | 72.49 | 16.02 | 4.52x | 1664 | 291 |
| 100,000 | 40 | Entry sum | 100.05 | 31.82 | 3.14x | 2519 | 559 |
| 100,000 | 40 | Row + column filter (count rows) | 337.21 | 16.85 | 20.01x | 7067 | 318 |
| 100,000 | 80 | Entry sum | 333.85 | 56.05 | 5.96x | 7817 | 822 |
| 100,000 | 80 | Row + column filter (count rows) | 2684.50 | 19.82 | 135.45x | 49375 | 405 |
| 200,000 | 10 | Entry sum | 38.66 | 26.46 | 1.46x | 729 | 406 |
| 200,000 | 10 | Row + column filter (count rows) | 36.45 | 16.22 | 2.25x | 760 | 290 |
| 400,000 | 10 | Entry sum | 71.86 | 47.83 | 1.50x | 1025 | 551 |
| 400,000 | 10 | Row + column filter (count rows) | 63.72 | 18.63 | 3.42x | 1261 | 338 |

Entry sum now grows approximately linearly with sample count. At equal four
million-entry shapes, 100k rows × 40 samples takes 31.8 ms, while 400k rows × 10
samples takes 47.8 ms; the previous wide-row penalty has disappeared in this probe.

The filter benchmark forces row execution and counts columns; it does not return
entries. The new plan can avoid constructing unused entry arrays, whereas the old
copy-and-search path materialized them. Correctness tests separately consume
filtered entries and verify their values, presence bits, positional identities,
and storage/localization round trips.

Estimated instructions/query are median time multiplied by average observed
instruction throughput, not an exact counter measured at each query boundary.
At 80 samples, entry sum falls from 10.9 billion to 4.08 billion estimated
instructions/query; filtering falls from 77.1 billion to 0.552 billion. Repeated
queries in the faster build can have a higher instructions/second rate while
requiring fewer instructions per query.

The SSD counters in the 10 Hz plots are **system-wide**, including unrelated
background traffic. Process I/O is also recorded in CSV. Cache misses are not
available through macOS proc_pid_rusage. These three-second captures are useful
for diagnosis, not final comparative performance claims.

## Artifacts

- [Sample scaling](samples-scaling.png), [row scaling](rows-scaling.png).
- [Entry sum: 10 Hz](matrix_entry_sum-10hz.png).
- [Column filtering: 10 Hz](matrix_filter_rows_columns-10hz.png).
- [All sizes and counter estimates](comparison.csv).
- Per-capture `metrics.csv`, `counters.json`, `worker.json`, and `summary.json`.

## Verification

- Full Python suite: **1,043 passed, 1 skipped, 19 subtests passed**.
- Rust execution unit tests: **65 passed**.
- Rust Table unit/integration tests: **58 passed**.
- Twelve new Python cases cover layout invariants and malformed layouts; four new
  Rust cases cover batched gathers and validation. Existing indexed-read tests
  still verify pruning of unrelated files.
- A pre-existing join test assumed the ordering adapter was the direct child of
  a merge join. DataFusion's distribution optimizer can insert an order-preserving
  repartition above it; the assertion now checks for the ordering adapter in the
  input subtree, retaining its no-hash-join and row-result checks.

## Cloud comparison pending

A normal m7i.8xlarge VM (32 vCPU, 128 GiB, no GPU) was created, but automatic
approval review rejected uploading the private source archive because explicit
source-transfer authorization was missing. No archive was transferred. The VM
was terminated and its temporary security group and imported key removed; its
root volume was configured for deletion on termination. A new stock PyPI Hail
comparison at 10/20/40/80 samples and increased row counts remains pending that
approval. `run_cloud_benchmarks.py --samples-8x` includes the 80-sample captures.
