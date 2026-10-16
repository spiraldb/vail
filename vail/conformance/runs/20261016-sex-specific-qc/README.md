# Sex-specific QC: eliminate per-Call allocation and bulky predicate contexts

The unmodified upstream `test_kyle_sex_specific_qc` now completes in **38.27s**, versus **198.95s** before the fix on this laptop: **5.20× faster**, with **5.24× less CPU time**. Both runs use the same original immutable MatrixTable snapshot and return 24,885 rows. These are single measurements, not confidence intervals.

| Run | Wall seconds | CPU seconds | Average cores | Peak RSS GiB |
|---|---:|---:|---:|---:|
| Before, original layout | 198.95 | 198.58 | 1.00 | 9.10 |
| Final, original layout | 38.27 | 37.90 | 0.99 | 8.19 |
| Final, separate four-file-aligned layout experiment | 15.15 | 42.00 | 2.77 | 18.52 |

The third row changes logical partition metadata and is **not** an engine-only before/after comparison. The original fixture advertises 16 partitions but has `partition_rows=[24885,0,...,0]`. Its four Vortex files contain `[8192,8192,8192,309]` rows. Public `_map_partitions` collection confirmed the original skew and the four-file-aligned counts. Both layout aliases copy only `_vail_matrix.json` and the generation/core `_vail_table.json`, replace `partition_rows`, and retain the latter's absolute original Iceberg metadata URI. They refer to the same original snapshot and files; neither rewrites the original metadata or data. A balanced 16-partition alias exceeded the explicit 32GiB RSS guard after 2.14s (32.84GiB sampled); it did not finish. Overlapping partition slices currently cause repeated whole-file reads.

## Broadly applicable fixes

- Call adapters use the existing checked, stack-backed `DecodedCall` codec. Validating a packed word no longer builds and discards an allele `Vec` per call. Call construction borrows the flat allele buffer and offsets, rather than allocating an Arrow slice wrapper per cell. Allele/phase outputs allocate batch-sized builders once.
- Packed-Call predicates bind their checked word once per expression evaluation before DataFusion arithmetic uses it repeatedly. The predicates remain ordinary DataFusion expressions.
- Plain row `count_where`, `fraction`, `any`, and `all` project only needed entry/column fields before zipping, map directly to Boolean values with entry-presence masking, and use main's shared DataFusion list reducers. They avoid building and filtering full entry-context structs or producing int64 one-hot predicate lists. Position-sensitive, lookup, random, scan, and combinator cases retain the existing path.
- Struct field extraction shares the child array when the parent has no nulls. Nullable parents use Arrow's bitmap/nullif kernels instead of constructing a per-element Boolean vector.

There is still O(entries) arithmetic and storage for output column buffers. The execution adapters no longer perform O(entries) **heap allocation calls**. This is not a claim that every operation throughout Vail, including Python localization/export, has been audited.

## Profiles and remaining cost

The baseline profile showed allocator/free traffic, `CallRows::get` and allocating `unpack`, plus repeated full-context construction. The final profile is dominated by bulk Arrow copying, integer arithmetic, validity/CASE buffer operations and predicate decoding arithmetic. `CallRows::get` is about 1.6% of leaf samples on the busiest thread; it no longer allocates per call. Hotspot summaries are included. Local Samply captures and symbol sidecars remain in the root checkout's identically named run directory; they are deliberately excluded from the commit.

Open performance work:

- Original fixture import produced one nonempty logical partition. Advertising `min_partitions=16` did not create 16 independent nonempty workloads. Fixing source partition layout is separate from the expression changes.
- Reading disjoint row slices from the same Vortex file must avoid decoding that entire file separately per partition. The guarded 16-way experiment demonstrates memory amplification; do not enable such metadata-only splitting indiscriminately.
- Even the original-layout final run peaks at 8.19GiB. Batch sizing, repeated Arrow broadcasts/CASE buffers and projected zipped column data remain important. The four-file experiment trades lower wall time for 18.52GiB; this patch does not claim bounded-memory parallel execution.

## Validation and provenance

- Matching rebuilt native: 70 focused Call/predicate/genomics/function tests pass; 104 broader MatrixTable/layout/shared-reducer/variant-QC/entry-sum/array-aggregation tests pass. JUnit reports included.
- Allocator regression tests execute public expression lowering/evaluation with a real System-backed counting allocator; input construction is excluded. Compare 64 and 8,192 calls, require bounded allocation growth, check nullable sliced inputs, malformed words hidden under missing parents, original shared word buffers, and sliced allele construction. Matching final results are recorded in `rebased-allocation-tests.log`.
- Code candidate: `9f678d9a2e90919200208123d130ca6dca1d148b`, based on main `dcbaac28b5d5377d8999ad112dc2827a206468a8`. Native SHA256: `7ecb7cfbac3eb783db21991097c4dffa0bc89b471d143f77517a17da717d4394`.
- Baseline engine: main `6bce5077d98f61e49f0b7ec9241f855f493b54d3`, plus the 25-line dependency-summary binding (unused by the old lowering). Python includes the identical restored Call predicates from `dc51582fcb2413be6f14372ea8887a4cabc85f88`, required to run this case. Baseline native SHA256 `7d0ee1e202098f797f1f2cb63f99dfc65cf6d8740407bc7ea895d24b204aaa54`. No claim of bit-identical pristine main.
- Intermediate `bound-full` results (40.76s) precede the final rebase/shared Boolean reducers and are retained only as an intermediate measurement. An earlier 279s run overlapped compilation and is excluded from the comparison.
- Apple M3 Max, 14 logical/physical cores, 96GiB RAM. No concurrent build during the final timings. Process CPU/RSS sampled at 10Hz; final profiles recorded with Samply at 500Hz, so their wall times include sampling overhead. Baseline completed timing was unprofiled; the bounded baseline profile is a separate run.
- Exact upstream source SHA256 `9bc92624fd8e2c295d78f39733f5cf3a441eee31ab3bd53185edd2920d9a2125`. Harness extracts the original function AST and times its actions; it does not rewrite the query.
- The existing GCP Hail result is 4.113s wall / 25.89 CPU-seconds on a 32-vCPU n2-standard-32. It is a different host and not a paired comparison with these laptop results. Hail was not rerun.
- Main's vendored DataFusion-common source was resolved through the peer's already built absolute patch path. Its Git tree `945f2e68fcb0dc966ac0beb6e4f595cd2862ad32` matches this candidate's vendored source. This avoids dependency cache duplication, not a source change.

## Reproduce

From the project directory, build/install the candidate with `maturin build --release`, then run:

```sh
samply record --rate 500 --save-only --unstable-presymbolicate \
  -o /private/tmp/qc-profile.json.gz \
  .venv/bin/python benchmarks/profile_sex_specific_qc.py \
  --package "$PWD/python" \
  --upstream ../hail/hail/python/benchmark/hail/benchmark_matrix_table.py \
  --fixture conformance/runs/20261015-nested-matrix-profile/data/profile25_mt.mt \
  --output /private/tmp/qc-results --max-rss-gib 24 --max-seconds 600

cargo test --release -p vail-python -p vail-execution \
  --features pyo3/extension-module --test call_allocations -- --nocapture
```

The cached benchmark fixture is local, not included in the source repository. Telemetry CSVs and summaries contain metrics/control metadata only. No table data is serialized as JSON by these changes.
