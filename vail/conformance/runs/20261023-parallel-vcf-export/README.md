# Parallel single-file VCF export

Candidate production source: `23bfea7a`; final public tests: `13aa528c`.
Baseline: current main `6b6eb3b6125ff843cd7c0309dedb4bbcb257c209`.

The exporter keeps the existing ordered, concurrently prefetched Arrow input.
It slices batches into at most 32,768 entries or one wide row (at most 4,096
rows), and runs up to the available CPU count capped at 32 blocking jobs.
Each job binds Arrow views once, reuses its record buffer, and formats directly
into BGZF compression. The final sink consumes compressed members in input
order. One header and one EOF are emitted. No flattened entry table is built.
Outstanding jobs and outputs are bounded. Errors stop input and join outstanding
jobs before the atomic temporary destination is removed. Cancellation leaves
workers owning only private buffers, with no destination handles.

Plain and ordinary gzip output also benefit from parallel record formatting;
ordinary gzip compression still belongs to its final stream writer. Existing
parallel-mode shard conventions are unchanged.

This is a generic terminal codec/sink for all supported VCF schemas and sizes.
Relational execution remains in DataFusion, including existing projection,
partitioning, ordering, and concurrent producer optimizations. No relational
operator, benchmark-specific shortcut, shuffle, or schema special case is added.
The bounded FuturesOrdered queue schedules only row formatting/compression;
DataFusion does not provide an existing VCF/BGZF codec for that terminal work.

## Validation protocol

The public exporter tests cover sample-slot order, missing and filtered entries,
INFO/FORMAT formatting, sliced rows, multiple input partitions, gzip/BGZF
roundtrips, late failure atomicity, header-only output, and real tabix queries
across BGZF member boundaries for records wider than 64 KiB.

Exact upstream `profile25_mt` input is regenerated with official Hail 0.2.139:
public `https://storage.googleapis.com/hail-common/benchmark/profile.vcf.bgz`,
`import_vcf(..., min_partitions=16)`, then a Hail matrix write. It has 24,885 rows
and 2,535 samples. Native conversion is untimed and uses the frozen original
Vail environment. Both candidate and baseline subsequently read identical files.

Normal kache/maturin release builds install distribution wheels into isolated
baseline/candidate environments, using the completed cloud Cargo target
sequentially. No compiled shared objects are copied as a cache workaround.
The first baseline/candidate restoration attempt preserved older tar source
mtimes and Cargo skipped recompilation. Identical native hashes caught this
before accepting measurements. That attempt was stopped and excluded; the
candidate is rebuilt normally after refreshing source mtimes. The installed
candidate native hash must equal its wheel member hash and differ from baseline.

Measurement initializes each engine outside the query timer, then times the
upstream read-plus-export operation into one `.vcf.bgz`. Three fresh processes
per engine/storage combination are run serially on a 32-vCPU cloud VM. Existing
10 Hz process-tree telemetry records CPU and peak RSS. Decompression and full
content hashing occur outside the query timer. Compilation belongs to query
execution where the engine performs it. OS cache state is uncontrolled; neither
fsync nor durable-storage latency is measured.

## Matched results

Median of three fresh-worker samples; peak RSS is the maximum sampled RSS.

| Engine / input | Wall seconds | CPU-seconds | Peak RSS GiB |
|---|---:|---:|---:|
| Hail / Hail legacy | 6.568 | 64.54 | 5.084 |
| Main baseline / Hail legacy | 49.756 | 63.16 | 7.001 |
| Candidate / Hail legacy | 12.235 | 78.55 | 5.667 |
| Main baseline / Vail native | 42.421 | 55.04 | 1.313 |
| Candidate / Vail native | 5.643 | 60.25 | 1.473 |

The change improves wall time 4.07x on legacy input and 7.52x on native input.
Native candidate takes 0.859x Hail's wall time; legacy remains 1.863x Hail's time.
Average CPU utilization rises from about 1.3 cores to 10.7 native / 6.4 legacy.
This is parallel throughput improvement, not a claim of reduced total work:
CPU-seconds rise about 9.5% native / 24.4% legacy. Native peak RSS rises about 164 MiB;
legacy peak RSS falls about 1.33 GiB. Compression settings are unchanged.
Native compressed output grows only 0.048%; legacy 0.064%, from independent
member boundaries. All 12 Vail outputs have identical decompressed SHA-256
`8036eaf6ea3fbe05e9b1837aaf361d582fb0ac698610eed6c68dddbcbd35c924`.
Every file contains all 24,885 records. This verifies exact baseline/candidate
text equality, not merely equal counts. Hail text differs in existing header and
formatting conventions, so its byte hash is not asserted equal to Vail's.

Final candidate native SHA-256:
`b3bd96af3826794611d5401bb0a2c90c76be3539e5d98b878d588b499e2373a6`.
Matched main baseline native SHA-256:
`920e32c992ede0d1d66e2c9715410adf3b2e0030bdfc1b0312b96ae593e7bbb9`.
Final candidate public tests: **15 passed in 3.18 seconds**, including external
tabix; no skipped tests. The candidate hash was checked against the installed
wheel payload and was required to differ from baseline before accepting tests
or timing results.

The authoritative CPU-clock profiles are `query-native` and `query-legacy`.
They capture public read/export plus Python startup, without subsequent output
decompression/hashing. Preliminary process-wide profiles included validation
and are not used for query hotspot percentages. Guest hardware performance
counters were unavailable; software CPU-clock sampling was available.

## CPU profiles and remaining opportunities

Software CPU-clock sampling at 99 Hz, with call stacks.
Native self CPU: 37.68% `longest_match`, 9.89% `deflate_medium` (both zlib-rs),
6.97% `Values::append`, and substantial integer formatting. Legacy self CPU:
31.00% `longest_match`, 8.80% `deflate_medium`, 6.54% `Values::append`,
2.20% `BufferedScalars::probe_values`, and 1.77% Arrow list interleaving.
These are self percentages, not inclusive sums over overlapping call stacks.
Formatting/compression stacks execute on Tokio workers; schema/Arrow-view
preparation is outside the row/entry loops and absent from the >=0.5% hotspots.

The remaining native CPU is mostly actual compression and formatting. Reducing
that work would require codec/number-format improvements, not another relational
operator or benchmark-specific planner rule. Legacy input remains materially
slower and uses more CPU than Hail; decoder and Arrow construction remain
separate investigation targets. This change does not claim to solve them.

The preliminary profiler's thread report used an unsupported `tid` sort key;
then a report of user-owned data required perf's explicit force option. Those
reporting attempts are excluded. The final query-only profile script uses
`--sort comm,pid` and `-f` on the known owned data. Raw final `query-*.perf.data`
and both text reports are retained in the evidence archive.

## Evidence

The downloaded raw archive SHA-256 is
`55d977f117fd4ae2b7307a407bb80ded8365697517d6af2b0258626763cf9057`,
verified against the cloud archive before extraction. Local archive:
`/private/tmp/vail-parallel-vcf-export-evidence.tar.gz`.
Extracted final CPU-clock data is retained in `evidence/results/query-*.perf.data`
(the binary profiles are not committed to Git); text profiles, all 15 measured
results, 10 Hz telemetry, build/test logs, source/native fingerprints and full
content hashes are committed. Candidate and baseline source fingerprints were
independently checked against the exact local Git revisions. Both final CPU
profiles report zero lost samples.
