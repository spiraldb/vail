# First repetition only — descriptive paired observations

These are ONE process per version/mode. Repetitions2/3 and finalinputSHA are incomplete; these are not final multipliers or evidence that a difference is persistent, noise, causal or equivalent. Planning is separate, the failed Hailhelper comparison remains unavailable, all112/67case denominators are preserved in the raw summary. CPU is observed10Hz worker/descendant lowerbound including telemetry; wholebundlePython3.11vs3.12 differs.

| Mode | Paired executed cases | Wall: old→current sum (s) | Observed CPU: old→current sum (s) | Current slower wall / more CPU |
|---|---:|---:|---:|---:|
| vail-vail | 111 | 397.025 → 336.466 | 1859.53 → 1680.13 | 15 / 23 |
| vail-hail | 67 | 771.591 → 269.414 | 1920.75 → 1066.08 | 7 / 8 |

Native Hail/current descriptive subset: 110 executedcase pairs; median observed Hail/current wallratio4.802×; 34 observations ≥10×. Do not combine legacy and native into one distribution or infer weighted end-to-end pipeline gains from these per-case ratios.

All native current-wall-slower observations (complete list, not just largest):

| Case | Old wall s | Current wall s | Change | Old observed CPU s | Current observed CPU s |
|---|---:|---:|---:|---:|---:|
| benchmark_matrix_table::test_kyle_sex_specific_qc | 1.867202 | 3.967775 | +112.50% | 26.80 | 8.84 |
| benchmark_table::test_read_force_count_partitions[10] | 0.026244 | 0.034157 | +30.15% | 0.17 | 0.17 |
| benchmark_table::test_group_by_collect_per_row | 0.311011 | 0.358410 | +15.24% | 4.30 | 4.46 |
| benchmark_table::test_table_aggregate_counter | 2.888675 | 3.304779 | +14.40% | 37.27 | 32.46 |
| benchmark_table::test_table_range_join[1000000000,1000] | 0.008961 | 0.009722 | +8.49% | 0.03 | 0.03 |
| benchmark_table::test_union_partitions_table[100,1000] | 2.455742 | 2.649009 | +7.87% | 11.01 | 6.26 |
| benchmark_table::test_write_range_table[10000000,10] | 0.080169 | 0.085325 | +6.43% | 0.85 | 0.90 |
| benchmark_sentinel::test_sentinel_read_gunzip | 10.569108 | 11.225748 | +6.21% | 11.71 | 12.41 |
| benchmark_methods::test_split_multi_hts | 71.104195 | 73.770752 | +3.75% | 101.07 | 104.66 |
| benchmark_matrix_table::test_read_decode_gnomad_coverage | 0.169136 | 0.174659 | +3.27% | 1.07 | 1.07 |
| benchmark_table::test_write_range_table[10000000,1000] | 0.435655 | 0.447021 | +2.61% | 5.48 | 5.83 |
| benchmark_table::test_table_scan_sum_1k_partitions | 0.237675 | 0.241926 | +1.79% | 1.17 | 1.19 |
| benchmark_table::test_join_partitions_table[1000,100] | 2.327883 | 2.348692 | +0.89% | 22.43 | 17.56 |
| benchmark_matrix_table::test_matrix_table_call_stats_star_star | 1.046210 | 1.054986 | +0.84% | 15.98 | 15.63 |
| benchmark_table::test_write_range_table[10000000,100] | 0.082653 | 0.082911 | +0.31% | 1.09 | 1.15 |

All paired cases, including legacy, faster and adverse observations, are retained in preliminary-pairs.json. No reruns, exclusions or tuning follow from these single observations.
