# Typed Calls and row-aggregation input projection

Candidate source: `d39caa403e7a06a86c02a126dfc45ca61c874035`, based on main `b57e7c9296da283bac7dcea4cc2d0262d8761bbd`.

Calls use actual Arrow UInt16 allele children, UInt8 ploidy and Boolean phasing. Allele indices above 65535 error, including numeric tokens in partly missing VCF genotypes. There is no packed fallback. Old development packed-Call tables must be rewritten.

General row aggregation projects its expression dependencies before filtering or zipping entry contexts. It omits unused column records and position arrays. Column lists remain positionally aligned, including reordered/duplicated columns and filtered entries. Position-sensitive, random and lookup expressions keep their existing context path. Reductions remain native DataFusion expressions/UDAFs, not Python cell evaluation. Batch Call validation checks child-null containment using Arrow bitmaps and folds canonical-field checks over buffers.

## Correctness

308 Python tests passed across Call construction/accessors, metadata, Arrow/storage, VCF, MatrixTable, genomic/numeric/collection/array/grouped aggregation and lazy initialization. Three Rust tests passed (one ignored benchmark); accessor allocations stayed five per batch at both 64 and 8192 Calls. Successful accessors allocate output buffers, not per-Call objects. Malformed and sliced nullable Calls remain checked.

## Matched laptop measurements

Both fixtures were freshly generated from the same cached VCF, SHA256 `cdacd409124e0f647e08b2eff0dda7705759e971d88d6c9b054dc252c91d4b93`, using `import_vcf(..., min_partitions=16)` followed by write. Each has 24885 rows, 2535 columns, 16 Vortex files, and identical saved row counts per partition. Workloads execute the unchanged upstream benchmark function AST. Two measurements per engine in baseline/candidate/candidate/baseline order, fresh processes, sequential workloads, 10 Hz CPU/RSS sampling. These are descriptive medians, not confidence intervals. Background macOS media analysis was active; our builds, fixture generation and tests had completed before measured runs.

| Case | Before seconds | After seconds | Before/after | Core-seconds before → after | Peak GiB before → after |
|---|---:|---:|---:|---:|---:|
| `test_kyle_sex_specific_qc` | 1.174 | 1.230 | 0.95× | 11.49 → 12.52 | 16.57 → 16.63 |
| `test_matrix_table_call_stats_star_star` | 2.733 | 2.328 | 1.17× | 33.47 → 29.60 | 19.18 → 14.25 |
| `test_matrix_table_filter_entries` | 0.658 | 0.736 | 0.89× | 7.90 → 9.12 | 9.64 → 10.01 |
| `test_matrix_table_many_aggs_row_wise` | 2.013 | 2.026 | 0.99× | 24.19 → 24.71 | 8.85 → 8.88 |

Call statistics improves about 15% in elapsed time and 26% in peak memory. Many row aggregates are approximately unchanged. QC is about 5% slower and entry filtering about 12% slower with the wider typed Call representation. These residual costs are retained and disclosed; the user explicitly requires actual UInt16 fields and forbids a packed fallback. Further profiling is needed to attribute the remaining end-to-end differences precisely.

A separate context-only ABBA control used the same direct-Call native library and identical fixture, changing only the Python context lowering: Call statistics improved from 2.690s / 33.06 core-seconds / 18.89GiB to 2.261s / 28.78 core-seconds / 14.28GiB. This isolates the dependency-projection benefit from Call storage or file count.

An initial comparison accidentally used an old four-file baseline against a new sixteen-file candidate. Those results are preserved under `benchmarks-initial-4-vs-16-files` and excluded above. The first matched comparison before the validation follow-up is under `benchmarks-pre-validation`.

Raw artifacts are in `/Users/danielking/projects/vail/vail/conformance/runs/20261016-direct-allele-calls`: benchmark scripts, per-run summaries/logs/10Hz telemetry, build/test logs, `provenance.json`, and `summary.json`. Final native was built from the exact final Rust revision; the Python source matches the candidate commit. This is an affected-workload laptop comparison, not a full cloud suite or a new Hail baseline run.

## Required review and integration

Reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact source `d39caa403e7a06a86c02a126dfc45ca61c874035` against `b57e7c9296da283bac7dcea4cc2d0262d8761bbd`, permitting a documentation-only record followed by fast-forward integration. The partially missing VCF overflow finding, missing typed metadata coverage, unsigned ordering rejection and explicit-schema import failures were addressed and retested. No source findings remain. Reviewer inspected source without running builds/tests or making edits.

Final native SHA256: `f38558fdd6f4184134d3e64ec62a01cc995d2a69f5f11f649bf177439caf448d`.

Follow-up: profile and reduce the residual QC and entry-filter overhead of the wider typed Call layout while preserving the user's required representation. No claim is made that these workloads improved. Acceptance records these measured tradeoffs and the limited two-repetition, loaded-laptop evidence.
