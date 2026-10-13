# Final 15-case Hail/Vail comparison — 2026-10-13

Both serialized runs completed all 15 cases, and all per-case correctness assertions passed. Each used 3 warmups and 5 measured iterations at 14 cores, scale 1.0. Vail ran first in about 14.25s; stock Hail then ran in 60.23s including Spark startup/shutdown. Hail used Spark 3.5.9 `local[14]`, configured with 48 GiB driver/executor memory and an observed JVM max heap of 48 GiB. Both environments used Python 3.11.13 on arm64. Recorded 1/5/15-minute system load averages across case starts ranged 1.85–8.73.

Vail imported from `vail` version 0.1.0a1 at `/private/tmp/vail-matrix-benchmark/lib/python3.11/site-packages/vail/`; wheel SHA-256 `71bac3f412e444701fac6ea50537a28c404a57a425ebb77dbe01f78b1faf5d33`, native SHA-256 `0ea9e6446a0592417a865faf1876eb849dd65d2e92864c196a0d58c61ce9c44b`. Hail 0.2.139-5e5eedb87ff8 (distribution 0.2.139) imported from `/private/tmp/vail-stock-hail-benchmark/lib/python3.11/site-packages/hail/`, not the outer local Hail namespace. Source and benchmark harness were verified against commit `c76c180d9e666e8c91489a5df1c7e151e9ac62d8`.

The ratio column is Hail median divided by Vail median, so values above 1 mean Vail was faster. The geometric mean speed ratio across all 15 cases was 2.69x. Performance varied by workload; for example, Vail was slower on `table_stats` and `matrix_entry_sum`. No cases failed.

| Case | Vail median (s) | Hail median (s) | Hail/Vail |
|---|---:|---:|---:|
| `table_range_force_count` | 0.0193 | 0.1972 | 10.22x |
| `table_key_by_shuffle` | 0.0668 | 0.7138 | 10.69x |
| `table_group_sorted` | 0.1891 | 0.5385 | 2.85x |
| `table_group_unsorted` | 0.1911 | 0.4669 | 2.44x |
| `table_annotate_100_fields` | 0.0323 | 0.1877 | 5.81x |
| `table_stats` | 0.1956 | 0.1637 | 0.84x |
| `table_inner_join` | 0.0760 | 0.1840 | 2.42x |
| `table_foreign_key_lookup` | 0.1885 | 1.1872 | 6.30x |
| `table_scan_sum` | 0.1961 | 0.1270 | 0.65x |
| `table_write` | 0.3031 | 0.2016 | 0.67x |
| `table_read` | 0.0119 | 0.0920 | 7.72x |
| `matrix_force_count_rows` | 0.0059 | 0.0431 | 7.28x |
| `matrix_entry_sum` | 0.1169 | 0.0737 | 0.63x |
| `matrix_filter_rows_columns` | 0.0696 | 0.0976 | 1.40x |
| `vcf_import_entry_sum` | 0.0551 | 0.1258 | 2.28x |

Raw per-iteration results, settings, and per-case starting load averages are in `vail.json` and `hail.json`; engine stdout/stderr are retained alongside them. Hail emitted Spark native-Hadoop and illegal-reflective-access warnings, but completed successfully.
