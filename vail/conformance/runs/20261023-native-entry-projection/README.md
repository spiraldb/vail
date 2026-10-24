# Native entry projections: retain source key bounds and parallelism

Source candidate: `57935256` atop current-main `ec909af` (no main update by this agent).
Only key-bound lineage analysis and public tests change. There is no benchmark,
field-name, reduction or data-size matcher, and no new execution operator.

## Problem and fix

Selecting entry or row fields rebuilds the matrix core through identity row
projections, an explicit empty `KeyBy`, and metadata-only key attachment. The
source values and physical partitions remain unchanged. The old bound analysis
requires declared keys at every intermediate node and stops at the empty
`KeyBy`. Ordered entry expansion consequently combines all source partitions
into one run even though their original key bounds remain valid.

The fix traces the requested key columns through identity projections, explicit
struct-field forwarding, type-identical casts, empty key removal and metadata
wrappers back to certified source bounds. Nonempty key changes/sorts and opaque
operators remain barriers. Actual input partition counts must still agree with
the bound vector; overlapping/equal boundary keys remain grouped to preserve
duplicate-row-key/column-key ordering. Input evaluation and error behavior are
not skipped. No setup or configuration is added to row/entry loops.

The existing DataFusion partial/final aggregations can then consume the existing
independently executable entry partitions. This repairs available partition/key
information; it introduces no special aggregate rewrite.

## Public plan confirmation

The source has 67 partitions in every variant. The public `entries().n_partitions()`
action reports:

| Matrix projection | Main | Candidate |
|---|---:|---:|
| None | 67 | 67 |
| Select entry DP | 1 | 67 |
| Select row fields | 1 | 67 |
| Select entries and rows | 1 | 67 |

These are diagnostic observations, not tests coupled to plan topology. Public
tests assert output values, multiplicity, missingness and observable ordering.

## Matched warmed measurements

Exclusive `vail-bench-20261021`, n2d-standard-32, AMD EPYC, 32 vCPUs. Native Vail
files are the same genuine Hail-origin profile matrix converted previously to
native storage. Each case runs in a fresh Python process with one warmup then
three measured samples. The table uses median wall/CPU seconds and maximum
process high-water RSS among measured samples. Source construction is inside
each timer; Python process startup is outside. OS cache is uncontrolled. Builds,
profiles, correctness tests and timing runs never overlap. No new Hail ratios.

| Query | Main wall s | Candidate wall s | Wall speedup | CPU s main / candidate | RSS GiB main / candidate |
|---|---:|---:|---:|---:|---:|
| `direct_sum` | 0.293 | 0.283 | 1.04× | 7.76 / 7.33 | 0.93 / 0.92 |
| `entry_sum` | 1.165 | 1.129 | 1.03× | 25.85 / 24.62 | 3.05 / 3.08 |
| `entry_sum_entryonly` | 10.022 | 1.018 | 9.84× | 14.92 / 21.90 | 1.84 / 1.28 |
| `entry_sum_rowonly` | 6.374 | 0.864 | 7.38× | 10.98 / 15.34 | 3.15 / 3.00 |
| `entry_sum_colonly` | 1.120 | 1.175 | 0.95× | 23.16 / 25.10 | 3.15 / 3.10 |
| `entry_sum_narrow` | 5.404 | 0.767 | 7.05× | 9.82 / 13.90 | 1.79 / 1.19 |
| `gq_summary` | 5.242 | 0.789 | 6.64× | 9.94 / 14.48 | 1.82 / 1.18 |
| `sample_depth` | 6.503 | 0.931 | 6.99× | 10.12 / 16.59 | 1.57 / 1.18 |

Every DP-sum variant returns `353709503`. Independent GQ summary returns
`Struct(total=1246497536, present=57513340, high=24865771)` on both revisions.
The grouped per-sample depth pipeline returns
`Struct(total=353709503, samples=2535, entries=63083475)` on both revisions.
All measured iterations agree exactly.

This is a latency/parallel-utilization fix, not a computation reduction. Affected
queries use approximately 40–64% more CPU seconds while finishing 6.6–9.8 times
faster, and generally use less peak memory. Unaffected controls show small
variation, including the column-only case being about 5% slower. These short
measurements do not establish a statistical regression or a CPU-efficiency win.

## Profiles and remaining work

Software `cpu-clock` sampling at 97 Hz, DWARF stacks, separate two-query fresh
processes, captured for main and candidate after timed runs. Both profiles still
show Arrow list gathering, MutableArrayData, offset conversion and entry batch
construction. Candidate self samples include ~8.94% MutableArrayData extend and
~5.61% list take; these are not disjoint cost totals. Profiles include startup,
OpenBLAS background thread samples (~7–9%) and sampling/unwinding limitations.
The available CPU parallelism improves; expensive entry construction remains.

A trial DataFusion `EmbeddedProjection` implementation on MatrixEntriesExec did
not help ordinary entry-table sum: frozen ConsumerScope wrappers prevent the
projection reaching it. It was removed from the candidate. Blindly changing
those wrappers would violate their shared consumer/schema identity contract.
Automatic nested demand should be propagated before consumer freezing, with
shared DAG demands handled consistently. No guard was weakened. Native direct
matrix DP sum remains ~4 times faster than the unprojected entry-table version,
so projection and unnecessary gathering remain important follow-up work.

## Correctness and provenance

146 public tests pass: new duplicate row/column keys, filtered entries versus
missing entry fields, string/list/struct projections, row/entry selection,
zero-column cardinality, changed-key sorting, head/collect, count/sum, plus
existing matrix layout, table/interval joins, interval reads, partition actions,
count projections and broadcast projection tests. There are no mocks, private
manifest edits, private-helper assertions or plan-topology tests.

Normal kache/maturin release builds, same target directory used serially; separate
normal installed wheel environments. No compiled extension was copied as cache.

- Main native SHA256: `83c0cccea5d713bf651822b08cee029937b7d05e12930a998fdaaf7c09884227`
- Candidate native SHA256: `ad715d12bedfeb315989af13db59a96b9b8a9c58c7056dd9844f68c930c489b1`
- `join_interval.rs`: `548f13594624a1820975be9d8907c34ceda25445690f0632558ed83bd21231ba`
- Unchanged `matrix_entries.rs`: `b364fe2182714f6a997c0ff2c93646c9ac05fad9080758c1da2c30e623751636`

An attempted paired build restored old tar timestamps, causing Cargo to reuse
the baseline artifact for the purported candidate. Identical loaded native
hashes exposed this. Those results are preserved under
`stale-mtime-attempt-results` and excluded. Changed source mtimes were refreshed,
then the candidate was normally rebuilt; the final harness rejects identical
baseline/candidate native hashes and records source/test fingerprints. Earlier
ineffective and exploratory runs are likewise preserved separately and excluded.

[Summary CSV](summary.csv), [all measured samples](summary.json),
[final provenance](evidence/native-entry-projection/results/provenance.json),
[public test log](evidence/native-entry-projection/results/public-tests.log).
The full raw archive, including both perf.data files, is retained in
`raw-artifacts/native-entry-projection-evidence.tar.gz` (ignored by Git), SHA256
`21736f0a52c4c69b8b43305934e9bc03bbd2e5de631337296b84fcbb37db6a6b`.
