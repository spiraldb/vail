# Full supported benchmark comparison, 2026-11-06

**Final results independently accepted.** All three timing repetitions, complete input hashes, artifact identities and provider shutdown receipts were independently reviewed. See `FINAL-INTEGRITY-ACCEPTED.json`.

Measured Vail source: `c82d0305603df7e0b9da8979a74c3cd9b210b073`, fresh normal Linux native `e6b3539a`. Baseline: the last full-suite source `517c8753`, normal native `4761b8ec`. Hail: `0.2.139-5e5eedb87ff8`. Current-versus-baseline measures cumulative changes since that full suite; it does not isolate the primitive-flatmap commit.

Three fresh processes per case/version, fixed cyclic order, original upstream benchmark actions and assertions. All 1,410 scheduled observations passed: 1,401 eligible executed-query observations and nine intentionally planning-only observations. There are zero nonpasses or execution-withheld observations; three planning groups are excluded from query ratios.

## Comparison with Hail

Ratios are the median across per-case speedups; each case speedup is comparator median divided by current median. The native pool includes format-independent cases. The Hail-input pool contains only the 67 separately qualified storage cases, so those two rows have different denominators.

| Vail input | Paired execution cases | Median wall speedup | Median core-second speedup | Slower wall cases | Higher core-second cases |
|---|---:|---:|---:|---:|---:|
| Native / format-independent | 111 | 4.495× | 5.079× | 4 | 10 |
| Hail-format storage | 67 | 3.349× | 4.086× | 9 | 8 |

37 native cases and 17 Hail-input cases are at least 10× faster than Hail in wall time. These are descriptive results, not significance tests.

## Changes since the previous full-suite build

| Input | Sum of per-case median wall times | Sum of per-case median core-seconds | Median per-case wall speedup | Wall-slower cases | CPU-higher cases |
|---|---:|---:|---:|---:|---:|
| Native | 343.76 → 255.58 s | 1697.59 → 1571.03 | 1.0045× | 49 | 40 |
| Hail-format | 270.51 → 186.17 s | 1065.41 → 959.57 | 1.0162× | 18 | 18 |

The sum of medians decreases 25.7% wall / 7.5% CPU for native and 31.2% wall / 9.9% CPU for Hail-format. The median individual benchmark changes little: the gains are concentrated. Every adverse observation, including small ones, remains in the report. The largest native wall regression versus the baseline is `table_scan_prev_non_null`: 2.12 → 2.72 s (+28.3%); all three paired repetition directions are adverse. No cause or noise attribution is established here.

## split_multi_hts

| Engine/input | Wall median | Observed core-seconds median | Wall speedup vs Hail | CPU speedup vs Hail |
|---|---:|---:|---:|---:|
| Hail | 6.105 s | 52.44 | 1.00× | 1.00× |
| Vail on Hail | 2.949 s | 42.45 | 2.07× | 1.24× |
| Vail native | 2.313 s | 28.84 | 2.64× | 1.82× |

Native split_multi_hts improved from 71.25 to 2.31 seconds versus the previous full-suite Vail build (30.81× wall, 3.49× CPU). This includes the earlier split/fusion/count work and must not be attributed solely to the primitive-output flatmap addition.

## Every wall-time slowdown versus Hail

| Benchmark | Input | Vail median | Hail median | Vail/Hail wall |
|---|---|---:|---:|---:|
| benchmark_table::test_group_by_collect_per_row | vail-hail | 12.133 s | 3.088 s | 3.929× |
| benchmark_matrix_table::test_gnomad_coverage_stats_optimized | vail-hail | 17.733 s | 5.352 s | 3.313× |
| benchmark_methods::test_variant_qc | vail-hail | 13.574 s | 4.148 s | 3.272× |
| benchmark_matrix_table::test_matrix_table_many_aggs_row_wise | vail-hail | 11.311 s | 4.603 s | 2.458× |
| benchmark_methods::test_export_vcf | vail-hail | 12.259 s | 6.412 s | 1.912× |
| benchmark_table::test_group_by_take_rekey | vail-hail | 9.481 s | 5.039 s | 1.882× |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | vail-hail | 11.183 s | 5.990 s | 1.867× |
| benchmark_matrix_table::test_kyle_sex_specific_qc | vail-hail | 6.354 s | 4.593 s | 1.384× |
| benchmark_matrix_table::test_mt_localize_and_collect | vail-hail | 12.027 s | 9.594 s | 1.254× |
| benchmark_matrix_table::test_matrix_table_entries_show | vail-vail | 8.893 s | 5.124 s | 1.736× |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | vail-vail | 8.269 s | 5.990 s | 1.381× |
| benchmark_matrix_table::test_write_profile_mt | vail-vail | 5.126 s | 4.448 s | 1.152× |
| benchmark_sentinel::test_sentinel_read_gunzip | vail-vail | 11.062 s | 10.685 s | 1.035× |

## CPU-heavy controls discussed in chat

| Benchmark/input | Vail wall | Hail wall | Vail core-sec | Hail core-sec |
|---|---:|---:|---:|---:|
| test_matrix_table_filter_entries_unfilter, vail-hail | 3.074 | 3.282 | 35.21 | 19.52 |
| test_matrix_table_filter_entries_unfilter, vail-vail | 1.932 | 3.282 | 39.93 | 19.52 |
| test_table_aggregate_downsample_dense, vail-hail | 8.566 | 16.787 | 169.82 | 56.04 |
| test_table_aggregate_downsample_dense, vail-vail | 7.961 | 16.787 | 108.36 | 56.04 |

## Evidence and limits

- [All cases, medians, ranges and samples](analysis/all-cases.json); [CSV](analysis/all-cases.csv); [every adverse wall/CPU metric](analysis/all-regressions.json).
- [Machine-readable summary](analysis/summary.json), [integrity acceptance](FINAL-INTEGRITY-ACCEPTED.json), and [artifact lock](artifact-lock.json).
- Raw source protocol and stage receipts are compressed in `protocol.tar.gz`, `prepare.tar.gz`, `preflight.tar.gz`, `rep1.tar.gz`, `rep2.tar.gz`, `rep3.tar.gz`, `final.tar.gz`; six host receipts are in `host/`. No shared libraries are included.
- Input enrollment: 17,568 files, 8,240,691,997 bytes. Independently sealed initial and final complete SHA-256 / canonical-set checks passed for this rerun. Provider STOP is verified separately for every stage; both benchmark VMs are stopped.
- 112 native/format-independent cases were enrolled; one intentionally planning-only case is excluded from executed-query ratios. The 67 Hail-format cases are not an additional 67 format-independent workloads. All 39 policy exclusions and 85 supplemental omissions remain explicit.
- The unused `test.hail.helpers` import is omitted from the one supported gVCF benchmark in both engines; only decorators of other, excluded cases referenced it, and the adapter already removes test decorators. This fixes the previous Hail setup failure without changing the timed body. The gVCF case now has three actual Hail observations.
- Hardware: the existing n2d-standard-32 benchmark VM, with original 32-core settings and BLAS threads limited to one. Build/correctness/input hashing are outside timing and do not overlap timed work.
- CPU is observed worker/descendant accounting at 10 Hz, a lower bound for unseen short-lived children. Summed sampled RSS can double-count shared memory and is not allocation/PSS or initialization peak.
- Original upstream execution/assertion evidence only; no cross-engine value-equivalence claim. Fixed cyclic order and uncontrolled OS cache; no causal or significance claims.

## Reproduce numerical analysis only

Extract `protocol.tar.gz` and all six stage archives into one fresh temporary directory. Copy `tools/analyze.py` to that directory, symlink `results/artifact-lock.json` to `../artifact-lock.json`, then run:

```sh
python3 /absolute/reproduction/analyze.py --root /absolute/reproduction/results --output /absolute/reproduction/analysis
```

This executes no query/build/VM action. It reproduces the same numbers with a PROVISIONAL integrity banner; the separately accepted original receipt hashes and review are retained in `FINAL-INTEGRITY-ACCEPTED.json`.
