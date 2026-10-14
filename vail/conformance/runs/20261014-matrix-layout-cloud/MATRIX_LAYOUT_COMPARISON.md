# Matrix layout cloud comparison

This compares the Vail captures at baseline **before SHA `d72a0869d5de78286af02f57f925dd87315de4f1`** and **after SHA `92f3d4f2cf44994a1bd949ab3f89605a778d361c`**. These are the measured revisions; the later audit patch `bfe917a` is not represented by these earlier results. Both captures ran on the same reported Linux AWS m7i.8xlarge class, 32 logical CPUs, with 3 warmups and 10-second measurement windows. Measurements below use per-process-tree peak RSS and mean CPU cores from `summary.json`.

`speedup` is before median divided by after median. CPU and memory are telemetry observations for each workload capture; host load and short-run variance remain relevant, especially where the before capture completed few repetitions.

## Sample scaling

Rows are fixed at 100,000; columns/samples grow from 10 to 80.

| Workload | Samples | Before median (s) | After median (s) | Speedup | Before → after CPU (cores) | Before → after peak RSS (MiB) | Repetitions before/after |
|---|---:|---:|---:|---:|---:|---:|---:|
| `matrix_entry_sum` | 10 | 0.023170 | 0.020353 | 1.14× | 8.59 → 7.07 | 368.8 → 186.1 | 430/490 |
| `matrix_filter_rows_columns` | 10 | 0.030010 | 0.025440 | 1.18× | 5.33 → 1.47 | 434.3 → 123.4 | 332/392 |
| `matrix_entry_sum` | 20 | 0.033225 | 0.024277 | 1.37× | 13.13 → 9.04 | 1024.3 → 236.0 | 300/415 |
| `matrix_filter_rows_columns` | 20 | 0.062009 | 0.025941 | 2.39× | 12.41 → 1.61 | 1803.7 → 133.0 | 159/385 |
| `matrix_entry_sum` | 40 | 0.136349 | 0.029251 | 4.66× | 13.41 → 11.58 | 2362.1 → 327.6 | 73/346 |
| `matrix_filter_rows_columns` | 40 | 0.599112 | 0.025845 | 23.18× | 14.06 → 1.95 | 7619.0 → 154.4 | 17/386 |
| `matrix_entry_sum` | 80 | 0.648694 | 0.045324 | 14.31× | 14.19 → 14.90 | 7788.5 → 498.9 | 16/230 |
| `matrix_filter_rows_columns` | 80 | 4.306373 | 0.029569 | 145.64× | 12.41 → 2.56 | 50749.3 → 189.3 | 3/338 |

## Row scaling

Samples/columns are fixed at 10; Matrix rows grow from 100,000 to 400,000. The 100,000-row baseline is shown in the sample table above.

| Workload | Matrix rows | Before median (s) | After median (s) | Speedup | Before → after CPU (cores) | Before → after peak RSS (MiB) | Repetitions before/after |
|---|---:|---:|---:|---:|---:|---:|---:|
| `matrix_entry_sum` | 200,000 | 0.034249 | 0.025960 | 1.32× | 12.74 → 9.68 | 627.2 → 231.7 | 295/390 |
| `matrix_filter_rows_columns` | 200,000 | 0.039628 | 0.025691 | 1.54× | 8.61 → 1.81 | 600.9 → 138.4 | 252/388 |
| `matrix_entry_sum` | 400,000 | 0.054074 | 0.038942 | 1.39× | 16.55 → 13.14 | 993.3 → 342.2 | 191/271 |
| `matrix_filter_rows_columns` | 400,000 | 0.059262 | 0.027412 | 2.16× | 11.72 → 2.61 | 1393.8 → 161.7 | 168/363 |

## What the measurements support

The entry-sum path improves at every tested size: 1.14× at 10 samples, rising to 14.31× at 80 samples. At fixed 10 samples, it is 1.32× at 200,000 rows and 1.39× at 400,000 rows. The increasingly large sample-scaling gain is consistent with removing repeated work tied to the sample/column dimension; it is benchmark evidence, not a standalone complexity proof.

The filter-and-count path improves from 1.18× at 10 samples to 145.64× at 80. Its before peak RSS grows from 434.3 MiB to 50749.3 MiB, while after RSS is 123.4–189.3 MiB. This benchmark filters rows and columns, then checks row and column counts; it does not read or aggregate entry arrays. The benchmark therefore measures layout/filter/count cost without consuming unused entry payloads. Entry correctness is covered separately by `python/tests/test_matrix_layout.py::test_chained_column_masks_gathers_metadata_and_missing_slots`, whose layout assertions check entry markers, presence bits, column order, and round-trip alignment, and by `python/tests/test_matrix_table.py` projected-entry assertions.

The 40/80-sample filter before captures have only 17/3 repetitions and peak RSS grows sharply at 80 samples, so their large speedups should be read alongside the repetition counts. CPU/RSS capture methodology and caveats are in `after-results/SUMMARY.md`; raw per-case telemetry remains in each Vail `summary.json` and `metrics.csv`.

The machine-readable rows, including both measured SHAs, dimensions, medians, CPU, RSS, and repetition counts, are in [`matrix-layout-comparison.csv`](matrix-layout-comparison.csv).
