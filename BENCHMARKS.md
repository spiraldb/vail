# Hail and Vail benchmark results

Fresh full supported native-format run, started **2026-11-12**. Vail source: `8ed78f4fb2363fe83295363438cdf143f6a4b8a1`; Hail: `0.2.139-5e5eedb87ff8`.

This compares **Vail reading Vail-native datasets with Hail reading Hail-native datasets**, plus format-independent operations on common inputs. It measures end-to-end first actions in fresh processes, rather than warmed kernel throughput. It does not measure Vail reading Hail-format storage.

## Overview

Of 112 scheduled cases, **111 have complete three-process execution measurements for both engines**. One case is intentionally planning-only; all other unpaired results, if any, remain listed below. Among complete execution pairs, the median per-case Hail/Vail ratio is **8.116× for wall time** and **8.209× for observed core-seconds**. Vail takes longer in **2** cases and uses more core-seconds in **8**. These are descriptive measurements, not significance tests or a claim about all Hail workloads.

| Workload group | Complete execution pairs | Median wall H/V | Median CPU H/V | Vail wall slower | Vail CPU higher |
|---|---:|---:|---:|---:|---:|
| All native / format-independent | 111 | 8.116× | 8.209× | 2 | 8 |
| Persisted native storage | 68 | 7.626× | 5.832× | 0 | 5 |
| Format-independent | 43 | 10.257× | 12.712× | 2 | 3 |

Each case contributes equally to the median above. A ratio greater than 1 favors Vail; a ratio below 1 favors Hail. Each case ratio divides the Hail median of three fresh processes by the corresponding Vail median. Wall and CPU are summarized separately: parallel execution can improve wall time while increasing CPU use.

The equal-case geometric means are 9.276× wall and 9.587× CPU. Sums of case medians are Vail 195.5 versus Hail 762.8 wall-seconds, and Vail 1417 versus Hail 5382 core-seconds. These sums are neither elapsed suite duration nor a representative workload weighting. 70 paired cases reach 5× wall and 49 reach 10×; the complete distribution and adverse cases matter more than those thresholds.

## Every case where Hail has lower wall time

Values are seconds; excess is `(Vail / Hail − 1) × 100%`. No small adverse result is silently classified as noise. This run does not identify causes.

| Case | Vail median | Hail median | Vail excess |
|---|---:|---:|---:|
| `benchmark_sentinel::test_sentinel_read_gunzip` | 10.97 | 10.53 | 4.2% |
| `benchmark_sentinel::test_sentinel_cpu_hash_1` | 13.7 | 13.68 | 0.1% |

## Every case where Hail uses fewer core-seconds

These CPU measurements include the worker and observed descendants, including the JVM. They are a lower bound for very short-lived children between samples.

| Case | Vail median | Hail median | Vail excess |
|---|---:|---:|---:|
| `benchmark_table::test_table_range_join[1000000000,1000000000]` | 93.13 | 64.53 | 44.3% |
| `benchmark_matrix_table::test_matrix_table_filter_entries_unfilter` | 30.1 | 21.06 | 42.9% |
| `benchmark_table::test_table_import_ints_impute` | 92.67 | 69.65 | 33.1% |
| `benchmark_matrix_table::test_write_profile_mt` | 39.11 | 33.27 | 17.6% |
| `benchmark_matrix_table::test_matrix_table_array_arithmetic` | 27.98 | 24.24 | 15.4% |
| `benchmark_table::test_table_aggregate_counter` | 39.9 | 34.86 | 14.5% |
| `benchmark_table::test_table_aggregate_downsample_dense` | 64.89 | 58 | 11.9% |
| `benchmark_sentinel::test_sentinel_read_gunzip` | 12 | 11.6 | 3.4% |

## What was measured

- **Machine:** one Google Cloud `n2d-standard-32`, AMD EPYC 7B13, 32 vCPUs (16 physical cores with SMT), 128 GiB RAM, 300 GB balanced persistent disk, Ubuntu 22.04, `us-east1-b`. One benchmark process at a time; no overlapping build or profiling workload during timings.
- **Runtime:** separate Python 3.12 environments; Hail Spark `local[32]`, 16 GiB driver/executor settings; Vail default mimalloc and Rayon 32; BLAS/OMP 1 for both. These settings do not cap every runtime thread pool identically. Exact packages, Java/glibc versions and hashes are retained in the evidence.
- **Vail artifact:** normal release wheel built from the frozen source with Rust 1.98.0, kache 2.0.0, maturin 1.15.0 and `-C target-cpu=native`. Native SHA256: `d1773071fd6e3e1ee2c778de9c258175346840c31948e8d92f492877f777df9c`. No held inner-scan prototype or dirty workspace changes are included.
- **Lifecycle:** one original upstream action per fresh process, three processes per engine/case, alternating engine order across cases/repetitions. Import and `Hail.init` are outside the action timer; query construction, planning, compilation and the original action are inside. No extra warmup or forcing action was inserted. Fresh processes do **not** imply cold filesystem caches: fixture preparation and hashing read the files beforehand, and OS cache state was not controlled.
- **Inputs:** freshly generated original-size public upstream fixtures, seeded 20261112 during Hail fixture generation, then converted once through Vail's public legacy-reader/write APIs before timing. Vail-native inputs are derived from the same Hail fixtures, with count/prefix checks; full conversion equality was not tested. Storage layout, compression and partitioning may differ. Public raw import files are common. Random values generated inside timed bodies follow engine defaults; the fixture seed does not establish identical timed random streams. Major datasets include profile25 (24,885 variants ×2,535 samples), simulated coverage (250,000×1,000), random doubles (60,000×4,000), 7.5 million integer rows, 5 million string rows and 10 million-row partition variants. Input count/prefix checks cover all 14 persisted datasets; these are not full output-equivalence tests.
- **Input integrity:** 17,593 files, 8,242,363,754 bytes enrolled with complete SHA256 and exact canonical file sets, including required BGEN indexes and present VCF tabix companions. Each phase checks set/stat identity; a final complete rehash checks content. Fresh generated data and the loader configuration mean these ratios should not be treated as isolated code-change speedups against older reports.
- **Loader configuration:** the normal wheel initially failed import with insufficient static TLS space. Without rebuilding it, both engines and descendants were then run with `GLIBC_TUNABLES=glibc.rtld.optional_static_tls=16384`. This reserves additional static TLS per thread and may affect TLS access; it is a recorded, nonneutral runtime configuration. The initial failed import and aborted 65,536-byte TLS attempt (Hail JVM startup failed) are retained separately. The final 16,384-byte setting passed full Hail initialization/shutdown and repeated Vail preflight before timing; no observations from the failed setting are pooled. See the [GNU libc documentation](https://sourceware.org/glibc/manual/latest/html_node/Dynamic-Linking-Tunables.html).
- **Measurements:** elapsed action time and 10 Hz process-tree CPU/RSS counters. CPU includes the measurement thread and observed JVM/other descendants; children born and exited between samples can be missed. RSS is a sampled sum, so it can miss peaks and double-count shared pages. The 96 GiB RSS guard covers fixture/action intervals after engine initialization, not total process lifetime or setup. It is not a memory-ownership or hardware-bandwidth measurement.

## Coverage and correctness limits

The frozen pool contains 68 storage and 44 format-independent cases from the upstream Hail benchmark bodies. It retains the previous 39 policy exclusions and omits 85 separately recorded supplemental cases. “Full” means the full frozen supported pool, not all Hail functionality or all benchmarks ever recorded. The pinned original bodies, parameters and assertions are preserved; the established import/decorator adapter is documented in the protocol.

The evidence tiers are 110 execution-only cases, one case with an existing assertion that its count equals 15, and one planning-only case. Most original benchmark actions discard their return value. Successful completion therefore establishes execution of that action, **not** complete cross-engine value, ordering or schema equivalence. Setup compares full counts and small logical prefixes; no new timed output validation or conformance workload was added.

Current-Vail preflight gates matching Hail timings. A nonpass is retained and reused as a non-sample in later repetitions rather than retried or removed. Ratios require three eligible, finite-positive execution samples for **both** engines. Planning measurements remain separate.

### Planning and other unpaired cases

| Case | Evidence tier | Current preflight | Vail timed statuses | Hail timed statuses |
|---|---|---|---|---|
| `benchmark_table::test_table_python_construction` | planning | passed | passed, passed, passed | passed, passed, passed |

## Complete case table

All values below are medians of three independent processes. `—` means no complete paired execution comparison; consult the status table and raw observations rather than inferring a speedup. Units are seconds and observed core-seconds.

| Case | Vail wall (s) | Hail wall (s) | Wall H/V | Vail CPU (core-s) | Hail CPU (core-s) | CPU H/V |
|---|---:|---:|---:|---:|---:|---:|
| `benchmark_combiner::test_import_gvcf_force_count` | 1.258 | 5.478 | 4.356× | 2.07 | 21.68 | 10.473× |
| `benchmark_matrix_table::test_export_range_matrix_table_entry_field_p100` | 1.041 | 4.052 | 3.891× | 7.03 | 40.26 | 5.727× |
| `benchmark_matrix_table::test_gnomad_coverage_stats_optimized` | 1.079 | 5.184 | 4.804× | 28.58 | 68.4 | 2.393× |
| `benchmark_matrix_table::test_import_bgen_filter_count` | 13.62 | 21.59 | 1.585× | 57.14 | 164.8 | 2.884× |
| `benchmark_matrix_table::test_import_bgen_force_count_all` | 13.77 | 21.15 | 1.536× | 58.3 | 162.9 | 2.795× |
| `benchmark_matrix_table::test_import_bgen_force_count_just_gp` | 11.43 | 21.08 | 1.845× | 37.74 | 163.3 | 4.328× |
| `benchmark_matrix_table::test_import_bgen_info_score` | 14.07 | 26.05 | 1.851× | 78.33 | 200.3 | 2.557× |
| `benchmark_matrix_table::test_kyle_sex_specific_qc` | 0.7813 | 4.389 | 5.618× | 11.41 | 28.14 | 2.466× |
| `benchmark_matrix_table::test_large_range_matrix_table_sum` | 3.736 | 20.75 | 5.554× | 50.31 | 417.7 | 8.303× |
| `benchmark_matrix_table::test_matrix_table_aggregate_entries` | 0.5419 | 3.694 | 6.817× | 7.69 | 20.61 | 2.680× |
| `benchmark_matrix_table::test_matrix_table_array_arithmetic` | 1.108 | 3.839 | 3.466× | 27.98 | 24.24 | 0.866× |
| `benchmark_matrix_table::test_matrix_table_call_stats_star_star` | 0.6074 | 3.515 | 5.787× | 15.57 | 23.07 | 1.482× |
| `benchmark_matrix_table::test_matrix_table_cols_show` | 0.2471 | 3.581 | 14.488× | 0.29 | 14.27 | 49.207× |
| `benchmark_matrix_table::test_matrix_table_decode_and_count` | 0.2847 | 2.993 | 10.510× | 7.51 | 16.86 | 2.245× |
| `benchmark_matrix_table::test_matrix_table_decode_and_count_just_gt` | 0.2794 | 2.905 | 10.397× | 7.34 | 16.42 | 2.237× |
| `benchmark_matrix_table::test_matrix_table_entries_show` | 0.389 | 5.163 | 13.275× | 0.6 | 25.18 | 41.967× |
| `benchmark_matrix_table::test_matrix_table_entries_table` | 0.2898 | 5.042 | 17.396× | 6.92 | 40.5 | 5.853× |
| `benchmark_matrix_table::test_matrix_table_entries_table_no_key` | 0.3161 | 3.676 | 11.631× | 7.77 | 24.33 | 3.131× |
| `benchmark_matrix_table::test_matrix_table_filter_entries` | 0.6271 | 3.361 | 5.359× | 16.54 | 20.57 | 1.244× |
| `benchmark_matrix_table::test_matrix_table_filter_entries_unfilter` | 1.144 | 3.492 | 3.053× | 30.1 | 21.06 | 0.700× |
| `benchmark_matrix_table::test_matrix_table_many_aggs_col_wise` | 0.9592 | 7.785 | 8.116× | 17.09 | 71.57 | 4.188× |
| `benchmark_matrix_table::test_matrix_table_many_aggs_row_wise` | 0.6144 | 4.93 | 8.024× | 13.99 | 34.64 | 2.476× |
| `benchmark_matrix_table::test_matrix_table_nested_annotate_rows_annotate_entries` | 1.096 | 10.19 | 9.297× | 16.67 | 102.4 | 6.140× |
| `benchmark_matrix_table::test_matrix_table_rows_force_count` | 0.04763 | 2.74 | 57.530× | 0.62 | 13.37 | 21.565× |
| `benchmark_matrix_table::test_matrix_table_rows_is_transition` | 0.04564 | 3.055 | 66.932× | 0.49 | 15.58 | 31.796× |
| `benchmark_matrix_table::test_matrix_table_rows_show` | 0.3412 | 3.378 | 9.899× | 0.38 | 13.92 | 36.632× |
| `benchmark_matrix_table::test_matrix_table_scan_count_cols_2` | 0.01683 | 2.371 | 140.844× | 0.04 | 9.35 | 233.750× |
| `benchmark_matrix_table::test_matrix_table_scan_count_rows_2` | 1.719 | 3.362 | 1.956× | 17.1 | 21.73 | 1.271× |
| `benchmark_matrix_table::test_matrix_table_show` | 0.67 | 3.399 | 5.074× | 0.73 | 13.68 | 18.740× |
| `benchmark_matrix_table::test_matrix_table_take_col` | 0.2458 | 1.833 | 7.458× | 0.29 | 5.75 | 19.828× |
| `benchmark_matrix_table::test_matrix_table_take_entry` | 0.3631 | 3.593 | 9.897× | 0.42 | 14.03 | 33.405× |
| `benchmark_matrix_table::test_matrix_table_take_row` | 0.3805 | 3.353 | 8.812× | 0.43 | 12.67 | 29.465× |
| `benchmark_matrix_table::test_mt_group_by_memory_usage` | 0.3131 | 6.827 | 21.804× | 7.9 | 42.01 | 5.318× |
| `benchmark_matrix_table::test_mt_localize_and_collect` | 4.534 | 9.481 | 2.091× | 4.6 | 18.4 | 4.000× |
| `benchmark_matrix_table::test_per_row_stats_star_star` | 0.3275 | 3.503 | 10.696× | 7.84 | 27.25 | 3.476× |
| `benchmark_matrix_table::test_read_decode_gnomad_coverage` | 0.04685 | 2.799 | 59.744× | 0.78 | 18.86 | 24.179× |
| `benchmark_matrix_table::test_write_profile_mt` | 4.052 | 4.658 | 1.150× | 39.11 | 33.27 | 0.851× |
| `benchmark_matrix_table::test_write_range_matrix_table_p100` | 0.2727 | 4.158 | 15.247× | 6.67 | 32.38 | 4.855× |
| `benchmark_methods::test_export_vcf` | 2.951 | 6.596 | 2.236× | 62.81 | 66.01 | 1.051× |
| `benchmark_methods::test_import_vcf_count_rows` | 1.909 | 7.944 | 4.160× | 2.08 | 27.56 | 13.250× |
| `benchmark_methods::test_import_vcf_write` | 20.93 | 24.62 | 1.176× | 48.2 | 64.18 | 1.332× |
| `benchmark_methods::test_split_multi_hts` | 1.509 | 6.302 | 4.176× | 29.62 | 54.74 | 1.848× |
| `benchmark_methods::test_variant_qc` | 1.096 | 4.28 | 3.905× | 21.44 | 29.89 | 1.394× |
| `benchmark_sentinel::test_sentinel_cpu_hash_1` | 13.7 | 13.68 | 0.999× | 13.76 | 13.86 | 1.007× |
| `benchmark_sentinel::test_sentinel_read_gunzip` | 10.97 | 10.53 | 0.960× | 12 | 11.6 | 0.967× |
| `benchmark_shuffle::test_shuffle_key_by_aggregate_bad_locality` | 0.1004 | 14 | 139.507× | 1.88 | 88.06 | 46.840× |
| `benchmark_shuffle::test_shuffle_key_by_aggregate_good_locality` | 0.1038 | 6.955 | 66.977× | 2.06 | 33.59 | 16.306× |
| `benchmark_shuffle::test_shuffle_key_rows_by_4096_byte_rows` | 0.515 | 5.266 | 10.224× | 0.73 | 70.58 | 96.685× |
| `benchmark_shuffle::test_shuffle_key_rows_by_65k_byte_rows` | 0.7393 | 5.051 | 6.832× | 0.82 | 69.71 | 85.012× |
| `benchmark_shuffle::test_shuffle_key_rows_by_mt` | 0.7906 | 6.161 | 7.793× | 21.25 | 65.02 | 3.060× |
| `benchmark_shuffle::test_shuffle_order_by_10m_int` | 0.03942 | 8.978 | 227.786× | 0.26 | 183.4 | 705.231× |
| `benchmark_table::test_group_by_collect_per_row` | 0.1973 | 3.333 | 16.892× | 4.66 | 27.08 | 5.811× |
| `benchmark_table::test_group_by_take_rekey` | 0.3349 | 5.352 | 15.983× | 3.42 | 66.59 | 19.471× |
| `benchmark_table::test_join_partitions_table[10,1000]` | 1.795 | 5.919 | 3.297× | 23.66 | 63 | 2.663× |
| `benchmark_table::test_join_partitions_table[10,100]` | 0.2452 | 3.723 | 15.182× | 4.15 | 39.32 | 9.475× |
| `benchmark_table::test_join_partitions_table[10,10]` | 0.09792 | 2.658 | 27.144× | 2.01 | 11.33 | 5.637× |
| `benchmark_table::test_join_partitions_table[100,1000]` | 1.354 | 4.817 | 3.559× | 9.19 | 52.89 | 5.755× |
| `benchmark_table::test_join_partitions_table[100,100]` | 0.2041 | 3.175 | 15.554× | 3.29 | 29.03 | 8.824× |
| `benchmark_table::test_join_partitions_table[100,10]` | 0.2513 | 3.907 | 15.550× | 4.4 | 43.63 | 9.916× |
| `benchmark_table::test_join_partitions_table[1000,1000]` | 1.325 | 3.996 | 3.016× | 7.16 | 32.24 | 4.503× |
| `benchmark_table::test_join_partitions_table[1000,100]` | 1.351 | 4.739 | 3.507× | 9.26 | 49.35 | 5.329× |
| `benchmark_table::test_join_partitions_table[1000,10]` | 1.883 | 6.223 | 3.305× | 24.11 | 67.99 | 2.820× |
| `benchmark_table::test_read_force_count_partitions[1000]` | 0.1073 | 3.784 | 35.256× | 0.78 | 31.59 | 40.500× |
| `benchmark_table::test_read_force_count_partitions[100]` | 0.02756 | 2.765 | 100.318× | 0.34 | 20.21 | 59.441× |
| `benchmark_table::test_read_force_count_partitions[10]` | 0.01837 | 2.376 | 129.311× | 0.11 | 9.36 | 85.091× |
| `benchmark_table::test_read_with_index[1000]` | 0.8372 | 4.931 | 5.890× | 2.42 | 46.77 | 19.326× |
| `benchmark_table::test_table_aggregate_array_sum` | 0.3483 | 2.913 | 8.363× | 2.53 | 25.77 | 10.186× |
| `benchmark_table::test_table_aggregate_counter` | 1.655 | 8.615 | 5.205× | 39.9 | 34.86 | 0.874× |
| `benchmark_table::test_table_aggregate_downsample_dense` | 3.388 | 16.84 | 4.971× | 64.89 | 58 | 0.894× |
| `benchmark_table::test_table_aggregate_downsample_worst_case` | 0.9427 | 4.094 | 4.343× | 20.08 | 20.65 | 1.028× |
| `benchmark_table::test_table_aggregate_int_stats` | 0.9026 | 5.724 | 6.341× | 16.11 | 29.07 | 1.804× |
| `benchmark_table::test_table_aggregate_linreg` | 2.269 | 11.56 | 5.097× | 46.43 | 53.61 | 1.155× |
| `benchmark_table::test_table_aggregate_take_by_strings` | 3.251 | 5.244 | 1.613× | 7.73 | 24.24 | 3.136× |
| `benchmark_table::test_table_annotate_many_flat` | 0.03197 | 2.677 | 83.723× | 0.23 | 14.56 | 63.304× |
| `benchmark_table::test_table_big_aggregate_compilation` | 2.256 | 48.54 | 21.515× | 2.44 | 118.8 | 48.676× |
| `benchmark_table::test_table_big_aggregate_compile_and_execute` | 2.423 | 49.87 | 20.586× | 6.65 | 152.6 | 22.953× |
| `benchmark_table::test_table_expr_take` | 0.2541 | 2.448 | 9.636× | 0.3 | 8.63 | 28.767× |
| `benchmark_table::test_table_foreign_key_join[1000000,1000000]` | 3.189 | 6.936 | 2.175× | 3.56 | 115 | 32.292× |
| `benchmark_table::test_table_foreign_key_join[1000000,1000]` | 0.2853 | 8.011 | 28.083× | 0.63 | 140.7 | 223.365× |
| `benchmark_table::test_table_group_by_aggregate_sorted` | 0.08853 | 4.54 | 51.278× | 0.1 | 58.72 | 587.200× |
| `benchmark_table::test_table_group_by_aggregate_unsorted` | 0.08881 | 5.225 | 58.835× | 0.1 | 75.18 | 751.800× |
| `benchmark_table::test_table_import_ints` | 1.116 | 17.87 | 16.016× | 22.63 | 81.17 | 3.587× |
| `benchmark_table::test_table_import_ints_impute` | 3.434 | 15.49 | 4.511× | 92.67 | 69.65 | 0.752× |
| `benchmark_table::test_table_import_strings` | 0.8698 | 8.921 | 10.257× | 15.68 | 30.74 | 1.960× |
| `benchmark_table::test_table_key_by_shuffle` | 0.01169 | 5.005 | 428.061× | 0.03 | 74.12 | 2470.667× |
| `benchmark_table::test_table_python_construction` | — | — | — | — | — | — |
| `benchmark_table::test_table_range_force_count` | 0.04606 | 2.317 | 50.299× | 0.06 | 15 | 250.000× |
| `benchmark_table::test_table_range_join[1000000000,1000000000]` | 3.244 | 4.006 | 1.235× | 93.13 | 64.53 | 0.693× |
| `benchmark_table::test_table_range_join[1000000000,1000]` | 0.009405 | 2.4 | 255.181× | 0.02 | 11.49 | 574.500× |
| `benchmark_table::test_table_range_means` | 0.7464 | 2.337 | 3.131× | 10.36 | 11.99 | 1.157× |
| `benchmark_table::test_table_read_force_count_ints` | 0.08505 | 3.458 | 40.661× | 1.75 | 14.02 | 8.011× |
| `benchmark_table::test_table_read_force_count_strings` | 0.2245 | 3.489 | 15.542× | 4.79 | 12.08 | 2.522× |
| `benchmark_table::test_table_scan_prev_non_null` | 1.326 | 4.355 | 3.283× | 5.23 | 28.05 | 5.363× |
| `benchmark_table::test_table_scan_sum_1k_partitions` | 0.1716 | 4.281 | 24.943× | 0.95 | 38.32 | 40.337× |
| `benchmark_table::test_table_show` | 0.271 | 4.1 | 15.125× | 0.31 | 15.24 | 49.161× |
| `benchmark_table::test_table_take` | 0.2432 | 2.484 | 10.211× | 0.28 | 9.02 | 32.214× |
| `benchmark_table::test_test_head_and_tail_region_memory` | 0.188 | 2.542 | 13.523× | 0.22 | 10.71 | 48.682× |
| `benchmark_table::test_test_inner_join_region_memory` | 0.1771 | 3.023 | 17.067× | 0.21 | 7.1 | 33.810× |
| `benchmark_table::test_test_left_join_region_memory` | 0.1967 | 3.051 | 15.512× | 0.59 | 7.5 | 12.712× |
| `benchmark_table::test_test_map_filter_region_memory` | 0.191 | 2.935 | 15.366× | 0.21 | 7.04 | 33.524× |
| `benchmark_table::test_union_partitions_table[10,1000]` | 1.504 | 5.979 | 3.975× | 4.28 | 65.64 | 15.336× |
| `benchmark_table::test_union_partitions_table[10,100]` | 1.367 | 4.073 | 2.978× | 3.95 | 50.41 | 12.762× |
| `benchmark_table::test_union_partitions_table[10,10]` | 1.343 | 2.757 | 2.052× | 3.7 | 12.75 | 3.446× |
| `benchmark_table::test_union_partitions_table[100,1000]` | 1.51 | 4.85 | 3.212× | 4.43 | 59.46 | 13.422× |
| `benchmark_table::test_union_partitions_table[100,100]` | 1.357 | 3.212 | 2.367× | 3.87 | 29.38 | 7.592× |
| `benchmark_table::test_union_partitions_table[100,10]` | 1.36 | 3.79 | 2.788× | 3.82 | 43.33 | 11.343× |
| `benchmark_table::test_union_partitions_table[1000,1000]` | 1.6 | 4.216 | 2.635× | 4.93 | 40.47 | 8.209× |
| `benchmark_table::test_union_partitions_table[1000,100]` | 1.516 | 4.907 | 3.236× | 4.34 | 59.39 | 13.684× |
| `benchmark_table::test_union_partitions_table[1000,10]` | 1.505 | 6.014 | 3.996× | 4.43 | 67.99 | 15.348× |
| `benchmark_table::test_write_range_table[10000000,1000]` | 0.3639 | 6.115 | 16.803× | 5.88 | 71.94 | 12.235× |
| `benchmark_table::test_write_range_table[10000000,100]` | 0.07155 | 3.776 | 52.782× | 1.17 | 40.87 | 34.932× |
| `benchmark_table::test_write_range_table[10000000,10]` | 0.06136 | 3.327 | 54.228× | 0.76 | 18.47 | 24.303× |

## Reproduction and evidence

The [run directory](vail/conformance/runs/20261112-full-native-benchmark/) contains the frozen protocol, source manifest, exact engine/runtime/input identities, raw phase receipts, per-repetition telemetry, complete-input rehash, shutdown receipts and review record. The [sample CSV](vail/conformance/runs/20261112-full-native-benchmark/samples.csv) preserves each scheduled timed observation; [summary JSON](vail/conformance/runs/20261112-full-native-benchmark/summary.json) includes all sample values, min/max ranges and derivation. Original failures and superseded setup inventories remain part of the record. Native binary hashes come from guest measurements of the normally installed artifacts; the evidence packet does not retain those binaries for an independent local binary rehash.

To repeat this run, build the stated source normally with its Cargo lockfile, recreate the recorded Python/Java environments, prepare the exact fixtures with the saved protocol, enroll their hashes, run the current preflight followed by all three saved schedules, then rehash inputs. The protocol uses absolute paths for its isolated VM workspace; adapt those paths explicitly and record any environment differences. Timing results alone do not demonstrate broad Hail semantic compatibility.
