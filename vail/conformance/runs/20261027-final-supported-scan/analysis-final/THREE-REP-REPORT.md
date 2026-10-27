# Three-repetition benchmark results

**Integrity status: FINAL_ACCEPTED**. Frozen benchmark native4761 / production517c8753, BEFORE pruning73201cc4c. These are not results for latest main.

Three eligible observations per version/case; ratio of medians. All cross-engine result comparisons remain false.

| Comparison | Cases | Median wall speedup | Geometric mean wall speedup | Summed-median wall speedup | Median observed-CPU speedup | Wall slower | CPU higher |
|---|---:|---:|---:|---:|---:|---:|---:|
| Frozen native vs old | 111 | 1.087× | 1.356× | 1.198× | 1.063× | 14 | 26 |
| Frozen native vs hail | 110 | 4.765× | 5.907× | 2.276× | 4.977× | 6 | 11 |
| Frozen legacy vs old | 67 | 1.123× | 1.943× | 2.845× | 1.130× | 8 | 5 |
| Frozen legacy vs hail | 67 | 3.256× | 3.843× | 1.248× | 3.503× | 10 | 10 |

## Hail = 1.0 normalization

Median of per-case normalized ratios; lower is better. This is not the inverse of the arithmetic mean speedup and not a ratio of aggregate totals.

| Input mode | Cases | Hail wall | Old wall/Hail | Frozen wall/Hail | Hail observed CPU | Old CPU/Hail | Frozen CPU/Hail |
|---|---:|---:|---:|---:|---:|---:|---:|
| native | 110 | 1.0 | 0.272 | 0.210 | 1.0 | 0.235 | 0.201 |
| legacy | 67 | 1.0 | 0.408 | 0.307 | 1.0 | 0.349 | 0.285 |

Final full-input integrity was independently accepted at evidence revision `5c5675749699a387d536955c6c7efaa02189cab6`:17,568 files, canonical set and full SHA256 matched; provider terminated. The immutable stage summary originally recorded review pending; this report references the subsequent explicit root/reviewer acceptance.

Raw action receipt SHA256s:
- Rep1: `1f4f147780d0d07f29c4f68b6437cc0716a62e1c82eb172ffa517ba6910f9e99`
- Rep2: `4fdfb3d65c8ea14e96680dc0880548d94043b79cbb3859435492a4c3ee0b5152`
- Rep3: `bd94bea289d9d6ebe5ee3fd938f9f48bce8a7a8b492942a5c8abadea0b7217b5`

A descriptive highlight means median slowdown>=10% AND all3 individual repetition ratios>1 (within1e-12 of1 is an arithmetic tie, not a statistical threshold). It is not a significance test, confidence interval or proof that smaller differences are noise. Every adverse ratio is retained.

## Native: every adverse comparison

| Case | Comparator | Metric | Current/comparator | Repetition ratios | >=10%, all3 directions |
|---|---|---|---:|---|---|
| benchmark_table::test_table_aggregate_downsample_dense | hail | observed_cpu_seconds | 2.031 | 2.031, 2.195, 1.736 | yes |
| benchmark_matrix_table::test_matrix_table_filter_entries_unfilter | hail | observed_cpu_seconds | 1.923 | 1.967, 1.852, 2.131 | yes |
| benchmark_methods::test_split_multi_hts | hail | observed_cpu_seconds | 1.888 | 1.994, 1.829, 1.848 | yes |
| benchmark_table::test_table_range_join[1000000000,1000000000] | hail | observed_cpu_seconds | 1.456 | 1.413, 1.463, 1.474 | yes |
| benchmark_table::test_table_import_ints_impute | hail | observed_cpu_seconds | 1.325 | 1.273, 1.329, 1.335 | yes |
| benchmark_matrix_table::test_matrix_table_filter_entries | hail | observed_cpu_seconds | 1.179 | 1.272, 1.153, 1.331 | yes |
| benchmark_matrix_table::test_write_profile_mt | hail | observed_cpu_seconds | 1.143 | 1.065, 1.143, 1.111 | yes |
| benchmark_matrix_table::test_matrix_table_array_arithmetic | hail | observed_cpu_seconds | 1.125 | 1.130, 1.111, 1.139 | yes |
| benchmark_sentinel::test_sentinel_read_gunzip | hail | observed_cpu_seconds | 1.026 | 0.986, 1.026, 1.090 | no |
| benchmark_methods::test_export_vcf | hail | observed_cpu_seconds | 1.020 | 0.991, 1.027, 0.946 | no |
| benchmark_table::test_table_aggregate_downsample_worst_case | hail | observed_cpu_seconds | 1.017 | 1.029, 0.996, 1.028 | no |
| benchmark_methods::test_split_multi_hts | hail | wall_seconds | 11.589 | 12.046, 11.176, 10.853 | yes |
| benchmark_matrix_table::test_matrix_table_entries_show | hail | wall_seconds | 1.693 | 1.704, 1.646, 1.704 | yes |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | hail | wall_seconds | 1.389 | 1.389, 1.348, 1.404 | yes |
| benchmark_matrix_table::test_write_profile_mt | hail | wall_seconds | 1.090 | 1.081, 1.090, 1.107 | no |
| benchmark_methods::test_import_vcf_write | hail | wall_seconds | 1.040 | 0.978, 1.097, 1.040 | no |
| benchmark_sentinel::test_sentinel_read_gunzip | hail | wall_seconds | 1.037 | 0.984, 1.037, 1.122 | no |
| benchmark_table::test_table_range_join[1000000000,1000] | old | observed_cpu_seconds | 1.500 | 1.000, 1.500, 1.500 | no |
| benchmark_matrix_table::test_mt_group_by_memory_usage | old | observed_cpu_seconds | 1.276 | 1.376, 1.305, 0.799 | no |
| benchmark_methods::test_export_vcf | old | observed_cpu_seconds | 1.151 | 1.139, 1.151, 1.167 | yes |
| benchmark_sentinel::test_sentinel_read_gunzip | old | observed_cpu_seconds | 1.061 | 1.060, 1.005, 1.108 | no |
| benchmark_table::test_write_range_table[10000000,1000] | old | observed_cpu_seconds | 1.061 | 1.064, 1.061, 1.025 | no |
| benchmark_table::test_write_range_table[10000000,100] | old | observed_cpu_seconds | 1.055 | 1.055, 0.950, 1.162 | no |
| benchmark_table::test_table_aggregate_downsample_dense | old | observed_cpu_seconds | 1.039 | 1.062, 1.117, 0.911 | no |
| benchmark_table::test_table_aggregate_counter | old | observed_cpu_seconds | 1.037 | 0.871, 1.037, 1.093 | no |
| benchmark_table::test_write_range_table[10000000,10] | old | observed_cpu_seconds | 1.035 | 1.059, 1.060, 0.944 | no |
| benchmark_table::test_table_scan_sum_1k_partitions | old | observed_cpu_seconds | 1.034 | 1.017, 0.992, 1.043 | no |
| benchmark_methods::test_split_multi_hts | old | observed_cpu_seconds | 1.033 | 1.036, 1.001, 1.029 | no |
| benchmark_methods::test_import_vcf_write | old | observed_cpu_seconds | 1.032 | 0.989, 1.032, 0.999 | no |
| benchmark_matrix_table::test_write_profile_mt | old | observed_cpu_seconds | 1.032 | 0.973, 1.032, 0.930 | no |
| benchmark_matrix_table::test_matrix_table_many_aggs_col_wise | old | observed_cpu_seconds | 1.030 | 0.966, 1.030, 1.001 | no |
| benchmark_matrix_table::test_matrix_table_decode_and_count_just_gt | old | observed_cpu_seconds | 1.029 | 1.004, 1.036, 1.020 | no |
| benchmark_table::test_group_by_collect_per_row | old | observed_cpu_seconds | 1.028 | 1.037, 0.979, 0.812 | no |
| benchmark_matrix_table::test_matrix_table_call_stats_star_star | old | observed_cpu_seconds | 1.020 | 0.978, 1.020, 1.009 | no |
| benchmark_table::test_table_aggregate_linreg | old | observed_cpu_seconds | 1.019 | 1.024, 0.952, 1.026 | no |
| benchmark_matrix_table::test_matrix_table_take_row | old | observed_cpu_seconds | 1.011 | 0.895, 0.989, 1.011 | no |
| benchmark_table::test_table_read_force_count_ints | old | observed_cpu_seconds | 1.011 | 0.995, 0.956, 1.011 | no |
| benchmark_matrix_table::test_matrix_table_filter_entries_unfilter | old | observed_cpu_seconds | 1.008 | 1.009, 1.008, 0.997 | no |
| benchmark_methods::test_variant_qc | old | observed_cpu_seconds | 1.008 | 1.008, 1.026, 1.012 | no |
| benchmark_table::test_table_range_means | old | observed_cpu_seconds | 1.007 | 1.127, 1.006, 1.006 | no |
| benchmark_table::test_table_import_strings | old | observed_cpu_seconds | 1.006 | 1.032, 1.273, 0.711 | no |
| benchmark_matrix_table::test_large_range_matrix_table_sum | old | observed_cpu_seconds | 1.004 | 0.976, 1.004, 1.063 | no |
| benchmark_matrix_table::test_import_bgen_info_score | old | observed_cpu_seconds | 1.002 | 1.012, 1.002, 1.025 | no |
| benchmark_sentinel::test_sentinel_read_gunzip | old | wall_seconds | 1.063 | 1.062, 1.005, 1.132 | no |
| benchmark_table::test_write_range_table[10000000,10] | old | wall_seconds | 1.059 | 1.064, 1.039, 0.898 | no |
| benchmark_matrix_table::test_large_range_matrix_table_sum | old | wall_seconds | 1.057 | 0.981, 1.057, 0.994 | no |
| benchmark_table::test_union_partitions_table[100,1000] | old | wall_seconds | 1.051 | 1.079, 0.970, 1.032 | no |
| benchmark_table::test_group_by_collect_per_row | old | wall_seconds | 1.034 | 1.152, 1.075, 0.962 | no |
| benchmark_methods::test_split_multi_hts | old | wall_seconds | 1.033 | 1.038, 0.996, 1.026 | no |
| benchmark_table::test_table_scan_sum_1k_partitions | old | wall_seconds | 1.023 | 1.018, 1.006, 1.053 | no |
| benchmark_methods::test_import_vcf_write | old | wall_seconds | 1.020 | 0.997, 1.074, 0.998 | no |
| benchmark_table::test_write_range_table[10000000,1000] | old | wall_seconds | 1.016 | 1.026, 1.064, 0.999 | no |
| benchmark_matrix_table::test_matrix_table_decode_and_count_just_gt | old | wall_seconds | 1.014 | 0.958, 1.041, 0.955 | no |
| benchmark_table::test_join_partitions_table[1000,100] | old | wall_seconds | 1.009 | 1.009, 0.978, 0.997 | no |
| benchmark_table::test_table_range_join[1000000000,1000] | old | wall_seconds | 1.006 | 1.085, 0.995, 0.999 | no |
| benchmark_table::test_write_range_table[10000000,100] | old | wall_seconds | 1.002 | 1.003, 0.972, 1.023 | no |
| benchmark_table::test_union_partitions_table[1000,10] | old | wall_seconds | 1.001 | 0.954, 0.960, 1.001 | no |

## Native: Hail/current wall speedup >=10× (34 cases)

| Case | Hail/current | Hail median seconds | Frozen Vail median seconds |
|---|---:|---:|---:|
| benchmark_table::test_table_range_join[1000000000,1000] | 264.610× | 2.40505 | 0.00908903 |
| benchmark_table::test_table_key_by_shuffle | 111.769× | 5.21668 | 0.046674 |
| benchmark_table::test_table_annotate_many_flat | 101.662× | 2.77325 | 0.0272792 |
| benchmark_table::test_read_force_count_partitions[10] | 92.331× | 2.42935 | 0.0263115 |
| benchmark_table::test_read_force_count_partitions[100] | 83.716× | 2.64335 | 0.0315753 |
| benchmark_matrix_table::test_matrix_table_rows_is_transition | 73.216× | 3.14782 | 0.0429936 |
| benchmark_matrix_table::test_matrix_table_scan_count_cols_2 | 71.972× | 2.55502 | 0.0355003 |
| benchmark_matrix_table::test_matrix_table_cols_show | 70.375× | 3.49477 | 0.0496592 |
| benchmark_shuffle::test_shuffle_key_by_aggregate_bad_locality | 63.601× | 14.9151 | 0.234511 |
| benchmark_table::test_table_group_by_aggregate_unsorted | 57.333× | 5.24968 | 0.0915644 |
| benchmark_table::test_table_range_force_count | 52.489× | 2.24671 | 0.0428034 |
| benchmark_table::test_table_group_by_aggregate_sorted | 49.212× | 4.35402 | 0.0884747 |
| benchmark_matrix_table::test_matrix_table_rows_force_count | 49.136× | 2.71824 | 0.0553209 |
| benchmark_table::test_write_range_table[10000000,100] | 47.522× | 3.9401 | 0.0829115 |
| benchmark_table::test_write_range_table[10000000,10] | 39.120× | 3.33794 | 0.0853251 |
| benchmark_matrix_table::test_matrix_table_take_col | 38.377× | 1.80672 | 0.0470778 |
| benchmark_shuffle::test_shuffle_key_by_aggregate_good_locality | 28.320× | 6.9437 | 0.245185 |
| benchmark_table::test_read_force_count_partitions[1000] | 25.536× | 3.74096 | 0.146497 |
| benchmark_table::test_table_foreign_key_join[1000000,1000] | 23.467× | 8.55952 | 0.364743 |
| benchmark_table::test_table_big_aggregate_compilation | 19.982× | 48.3684 | 2.42065 |
| benchmark_table::test_table_aggregate_array_sum | 19.648× | 3.00766 | 0.153081 |
| benchmark_matrix_table::test_matrix_table_rows_show | 19.419× | 3.38775 | 0.174458 |
| benchmark_table::test_table_import_ints | 19.357× | 17.1761 | 0.887313 |
| benchmark_table::test_table_big_aggregate_compile_and_execute | 19.210× | 49.7536 | 2.59004 |
| benchmark_table::test_table_scan_sum_1k_partitions | 17.032× | 4.13984 | 0.24306 |
| benchmark_shuffle::test_shuffle_order_by_10m_int | 16.875× | 8.73565 | 0.517659 |
| benchmark_matrix_table::test_mt_group_by_memory_usage | 16.791× | 14.5435 | 0.866135 |
| benchmark_table::test_write_range_table[10000000,1000] | 14.885× | 6.58854 | 0.442639 |
| benchmark_matrix_table::test_read_decode_gnomad_coverage | 14.548× | 2.80507 | 0.192815 |
| benchmark_table::test_table_import_strings | 13.895× | 9.1624 | 0.659399 |
| benchmark_table::test_table_show | 13.000× | 4.11016 | 0.316158 |
| benchmark_table::test_table_read_force_count_ints | 12.975× | 3.46626 | 0.267141 |
| benchmark_matrix_table::test_write_range_matrix_table_p100 | 12.392× | 4.25829 | 0.343642 |
| benchmark_table::test_join_partitions_table[10,10] | 10.640× | 2.64639 | 0.248718 |
## Legacy: every adverse comparison

| Case | Comparator | Metric | Current/comparator | Repetition ratios | >=10%, all3 directions |
|---|---|---|---:|---|---|
| benchmark_table::test_table_aggregate_downsample_dense | hail | observed_cpu_seconds | 2.853 | 2.915, 2.868, 2.710 | yes |
| benchmark_methods::test_split_multi_hts | hail | observed_cpu_seconds | 1.829 | 1.963, 1.829, 1.747 | yes |
| benchmark_matrix_table::test_matrix_table_filter_entries_unfilter | hail | observed_cpu_seconds | 1.692 | 1.701, 1.629, 1.734 | yes |
| benchmark_methods::test_export_vcf | hail | observed_cpu_seconds | 1.243 | 1.242, 1.252, 1.198 | yes |
| benchmark_table::test_table_aggregate_int_stats | hail | observed_cpu_seconds | 1.219 | 1.254, 1.186, 1.219 | yes |
| benchmark_matrix_table::test_matrix_table_array_arithmetic | hail | observed_cpu_seconds | 1.176 | 1.112, 1.176, 1.151 | yes |
| benchmark_matrix_table::test_write_profile_mt | hail | observed_cpu_seconds | 1.147 | 1.160, 1.117, 1.113 | yes |
| benchmark_matrix_table::test_matrix_table_filter_entries | hail | observed_cpu_seconds | 1.123 | 1.198, 1.098, 1.175 | yes |
| benchmark_matrix_table::test_mt_localize_and_collect | hail | observed_cpu_seconds | 1.033 | 1.020, 1.005, 1.054 | no |
| benchmark_table::test_table_aggregate_counter | hail | observed_cpu_seconds | 1.010 | 1.001, 0.968, 1.018 | no |
| benchmark_methods::test_split_multi_hts | hail | wall_seconds | 12.186 | 12.826, 12.186, 11.367 | yes |
| benchmark_table::test_group_by_collect_per_row | hail | wall_seconds | 3.771 | 3.563, 3.904, 3.939 | yes |
| benchmark_matrix_table::test_gnomad_coverage_stats_optimized | hail | wall_seconds | 3.644 | 3.505, 3.644, 3.628 | yes |
| benchmark_methods::test_variant_qc | hail | wall_seconds | 3.206 | 3.140, 3.247, 3.298 | yes |
| benchmark_matrix_table::test_matrix_table_many_aggs_row_wise | hail | wall_seconds | 2.437 | 2.397, 2.460, 2.297 | yes |
| benchmark_methods::test_export_vcf | hail | wall_seconds | 1.837 | 1.865, 1.837, 1.810 | yes |
| benchmark_table::test_group_by_take_rekey | hail | wall_seconds | 1.815 | 1.787, 1.869, 1.916 | yes |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | hail | wall_seconds | 1.802 | 1.802, 1.737, 1.868 | yes |
| benchmark_matrix_table::test_kyle_sex_specific_qc | hail | wall_seconds | 1.342 | 1.248, 1.366, 1.308 | yes |
| benchmark_matrix_table::test_mt_localize_and_collect | hail | wall_seconds | 1.252 | 1.231, 1.252, 1.249 | yes |
| benchmark_methods::test_export_vcf | old | observed_cpu_seconds | 1.234 | 1.245, 1.234, 1.274 | yes |
| benchmark_methods::test_split_multi_hts | old | observed_cpu_seconds | 1.016 | 1.033, 1.006, 1.007 | no |
| benchmark_table::test_table_read_force_count_strings | old | observed_cpu_seconds | 1.006 | 0.994, 0.981, 1.031 | no |
| benchmark_methods::test_variant_qc | old | observed_cpu_seconds | 1.004 | 1.004, 0.982, 0.986 | no |
| benchmark_matrix_table::test_write_profile_mt | old | observed_cpu_seconds | 1.002 | 1.002, 1.042, 0.915 | no |
| benchmark_table::test_union_partitions_table[100,100] | old | wall_seconds | 1.078 | 1.004, 0.999, 1.139 | no |
| benchmark_matrix_table::test_matrix_table_array_arithmetic | old | wall_seconds | 1.037 | 0.960, 1.037, 0.998 | no |
| benchmark_table::test_table_read_force_count_strings | old | wall_seconds | 1.008 | 1.001, 0.990, 1.030 | no |
| benchmark_table::test_union_partitions_table[10,100] | old | wall_seconds | 1.007 | 0.977, 1.050, 1.007 | no |
| benchmark_matrix_table::test_write_profile_mt | old | wall_seconds | 1.007 | 1.005, 0.995, 1.007 | no |
| benchmark_table::test_union_partitions_table[10,10] | old | wall_seconds | 1.005 | 1.019, 1.054, 0.901 | no |
| benchmark_methods::test_split_multi_hts | old | wall_seconds | 1.003 | 1.015, 0.995, 1.005 | no |
| benchmark_table::test_union_partitions_table[100,10] | old | wall_seconds | 1.001 | 1.064, 0.879, 1.015 | no |

## Legacy: Hail/current wall speedup >=10× (16 cases)

| Case | Hail/current | Hail median seconds | Frozen Vail median seconds |
|---|---:|---:|---:|
| benchmark_matrix_table::test_matrix_table_cols_show | 89.204× | 3.49477 | 0.0391775 |
| benchmark_table::test_table_show | 73.716× | 4.11016 | 0.055757 |
| benchmark_table::test_table_take | 53.527× | 2.50376 | 0.0467759 |
| benchmark_table::test_table_expr_take | 50.273× | 2.5213 | 0.0501518 |
| benchmark_table::test_read_force_count_partitions[100] | 47.506× | 2.64335 | 0.055642 |
| benchmark_matrix_table::test_matrix_table_take_col | 46.416× | 1.80672 | 0.0389247 |
| benchmark_table::test_read_force_count_partitions[10] | 42.451× | 2.42935 | 0.0572271 |
| benchmark_shuffle::test_shuffle_key_by_aggregate_bad_locality | 26.463× | 14.9151 | 0.563615 |
| benchmark_matrix_table::test_matrix_table_take_entry | 24.685× | 3.50656 | 0.142052 |
| benchmark_matrix_table::test_matrix_table_take_row | 24.237× | 3.34211 | 0.137894 |
| benchmark_matrix_table::test_matrix_table_entries_show | 22.577× | 5.21466 | 0.230974 |
| benchmark_table::test_read_force_count_partitions[1000] | 18.432× | 3.74096 | 0.202964 |
| benchmark_matrix_table::test_matrix_table_rows_is_transition | 16.961× | 3.14782 | 0.185593 |
| benchmark_matrix_table::test_matrix_table_rows_force_count | 14.219× | 2.71824 | 0.191169 |
| benchmark_matrix_table::test_matrix_table_rows_show | 13.366× | 3.38775 | 0.25346 |
| benchmark_shuffle::test_shuffle_key_by_aggregate_good_locality | 12.425× | 6.9437 | 0.558845 |

## Provenance and limits

111 native old/current execution pairs;110 Hail/current pairs;67 legacy storage pairs. One intentional planning logical case is separate (9planning observations).1410 scheduled records comprise1408 fresh worker attempts,1407 passed records,1 actual Hail helper failure and2 cached nonpass records.1398 execution observations enter timing ratios. No cached record receives invented CPU/wall.85 supplemental cases omitted.

Old Python3.11 vs frozen Vail Python3.12 is a whole-bundle comparison. Observed CPU is a10Hz worker+observed-descendant lower bound including JVM and telemetry; sampled summed RSS is not PSS/allocatedbytes/uniquephysical memory or initialization peak. Fixed cyclic version order and uncontrolled OS cache: no causal/no-noise/no-regression claim. Existing evidence tiers remain execution-only except one count15 assertion and planning; no cross-engine semantic validation.

All-case CSV files include Hail=1 normalized old/current wall and observedCPU, all3 medians/min/max/relative range/sampleCV. Histogram/ECDF CSVs include110 paired execution cases and exclude planning/missing Hail. Final integrity approval is external to this calculation.
