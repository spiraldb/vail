# Combined local SUM comparison

Experimental source and limitations are recorded in provenance.json. All 18 paired attempts passed. Results are medians of three serial reps per engine; unrelated laptop activity makes wall time noisy.

| Case | Vail wall s | Hail wall s | Vail core s | Hail core s | H/V wall | H/V core |
|---|---:|---:|---:|---:|---:|---:|
| test_large_range_matrix_table_sum | 6.3431 | 25.9193 | 16.7803 | 171.4450 | 4.086 | 10.217 |
| test_table_aggregate_array_sum | 1.1186 | 0.8415 | 7.2654 | 7.5070 | 0.752 | 1.033 |
| representative_grouped_coordinate_mean | 0.0538 | 2.0162 | 0.2936 | 12.3658 | 37.481 | 42.111 |

The exact 5B-entry matrix case uses the same 2500 partitions. Table array_sum uses upstream defaults. Grouped coordinate mean is a representative 10k×1k range matrix with 250 partitions.

Latest scalar/batch mode medians: exact 5B scalar 4.4658 wall /16.7929 core vs batch 4.9815/27.7362. Width1000 scalar .1955/1.9689 vs batch .1653/1.6160; width10000 scalar .1146/.5945 vs batch .1662/1.1289. Raw 18 attempts are preserved. Batch decodes ~20GB row metadata and copies ~20GB column metadata once; scalar concat writes ~20GB produced Int32 values. No automatic selection heuristic is proposed.

Embedded symbol profiles and caller-based memmove attribution are included. The general sharing fix still requires reviewer approval; these measurements do not imply main contains that fix.
