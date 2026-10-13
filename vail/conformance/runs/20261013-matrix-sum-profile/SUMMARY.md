# Matrix entry-sum profile and fix — 2026-10-13

Workload: range MatrixTable of 100,000 rows × 10 columns, annotate each entry with row_idx + col_idx, then sum its int64 cast. Every iteration builds a fresh lazy query and asserts the exact expected result (50,004,000,000). Source partition request: 14.

## Diagnosis

85.69% of baseline sampled CPU was below NullableCanonicalKeyExpr::evaluate. Matrix column binding filters column records by their internal integer ID inside collection lambdas. The physical expression lowerer encoded both primitive operands as allocated canonical binary keys before comparing them. The logical lowerer already used DataFusion directly for these primitive types, but collection lambdas take the physical lowering path. The dominant costs were encoding, malloc/free, and binary-key buffers, rather than integer addition or summation itself. This explains the high confidence in a small, targeted fix.

## Changes

- Physical comparisons on bool/int32/int64/string now use ordinary DataFusion expressions after type promotion, matching the logical lowering path. Missing values still propagate. Floats and complex types retain their existing total-order comparison encoding.
- Integer sum uses Arrow's typed int64 batch reduction, with promotion before reduction and wrapping state merging. It no longer boxes each input as ScalarValue. Existing aggregate state layouts are unchanged.
- Regression tests exercise all six primitive comparisons inside map/filter lambdas, int64 precision beyond 2^53, nulls, string ordering, retained float NaN/signed-zero total ordering, sum overflow, int32 promotion, empty/all-missing sums, and partial-state merging.

## Benchmark

The installed optimized release wheels were tested sequentially with the same workload and assertion. Baseline is revision 173aac1f4be07c163cd7f6b675cfeb9619b9c7be; updated binary/source hashes are captured alongside the profiles. No compiler processes were active to pause in this run. The peer agent deferred heavy jobs; ordinary desktop background activity and one lightweight fixture query could still occur.

| Engine | Warmups | Measurements | Median seconds |
|---|---:|---:|---:|
| Vail before | 3 | 5 | 1.999539 |
| Vail fixed | 3 | 5 | 0.248174 |
| Hail initial targeted run | 3 | 5 | 0.162397 |
| Vail longer confirmation | 10 | 10 | 0.261466 |
| Hail longer confirmation | 10 | 10 | 0.093280 |

The matched before/after run improved 8.06×. Hail's fresh-JVM timing was sensitive to warmup, so both engines received a longer confirmation. In that confirmation Vail remains 2.80× slower than Hail. All correctness assertions passed. Stock Hail is PyPI 0.2.139, Spark local[14], verified 48-GiB maximum heap; it was not built from source.

## Profile and 10-Hz resource capture

Both profiles sampled native stacks at 500 Hz and process counters at 10 Hz. The baseline uses five measured iterations; the faster fixed version uses 30 to obtain sufficient samples. CPU weights exclude warmups and idle samples. Neither engine's benchmark medians above include profiling.

| Profile metric | Before | Fixed |
|---|---:|---:|
| Profiled median seconds | 2.059603 | 0.253955 |
| Canonical nullable key encoder CPU share | 85.69% | 0 sampled frames |
| Median retired instructions per iteration | 41,858,525,645 | 4,738,213,849.5 |
| Median cycles per iteration | 7,668,634,251 | 929,748,999.5 |
| Mean utilized cores | 0.990 | 0.987 |
| Peak sampled RSS, MiB | 574.83 | 337.11 |
| Peak sampled physical footprint, MiB | 85.97 | 56.64 |

RSS includes mapped code and is not a count of live query buffers. Instructions/cycles come from proc_pid_rusage V4; cache-miss and branch-misprediction counters are unavailable through this API.

The remaining profile is dominated by nested-array expansion, copying and gathering: Arrow MutableArrayData::try_extend, memmove, struct extension, take, and list/lambda processing. It still uses about one core. Those are the next likely performance opportunities; this change does not eliminate all scalarized aggregate code or all Matrix overhead.

[Before flamegraph](before/flamegraph.svg) · [After flamegraph](after/flamegraph.svg)

[Before telemetry](before/telemetry.svg) · [After telemetry](after/telemetry.svg)

## Validation and reproduction

Full Python suite: 881 passed, 1 skipped, 19 passing subtests (56.85 s). The int32 promotion regression was extended afterward and rerun successfully. Rust production code was built in the optimized release wheel; a separate Rust test build was not run.

Use benchmarks/profile_annotate.py --workload matrix-entry-sum --profile with the recorded installed wheel. The before profile uses --warmups 3 --iterations 5; the after profile uses --warmups 3 --iterations 30. Render with benchmarks/render_annotate_profile.py. The ordinary benchmark uses benchmarks/compare_hail.py --case matrix_entry_sum with the warmup/measurement counts above. CPU 100% means one core; all query data travels through Arrow, with JSON used only for profiling/control metadata.
