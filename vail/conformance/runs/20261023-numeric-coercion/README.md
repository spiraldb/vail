# Numeric coercion and dictionary construction

Code revision `c8ab042418c917f3998c8b273630cd24f0d34347`, based on frozen main `e79cf6ffeb624756cc27769cd6031b0f12858651`.
This branch does not update main or the running cloud benchmark revision.

## Changes

- Promote both array operands before zip. Hail arithmetic promotes int64/float32 to float32; schema unions retain their existing float64 policy. Cast complete arrays with Arrow's batch cast kernel, preserving missing parents and elements.
- Recursively promote numeric leaves of tuple/struct/array comparisons, including Boolean-to-number comparisons. Structural casts preserve tuple positions and struct field names/order. This also permits existing typed array/dictionary builders to promote nested numeric values.
- Infer ordinary literal/expression Python Mapping values as dictionaries; context-specific table record/schema inference still recursively treats row-record mappings as structs; explicit `Struct` remains a struct. Infer dictionary keys and values independently, skipping missing values when other items provide their types. Empty/all-missing sides require explicit types.
- Build row-dependent dictionary values as ordinary native expressions, including nested values. Capture expression leaves inside `literal` once during literal construction; reject row/entry-indexed and free-variable captures.
- Update two older tests which encoded the incorrect Python-dict-as-struct behavior. Empty structs now use explicit `Struct()`.

Execution continues through existing typed DataFusion/Arrow kernels. There are no Python row/entry evaluation loops or new runtime scalar interpreters. This is a correctness/API fix, not a measured performance improvement.

## Validation

- Normal `maturin develop --release --locked`, with kache and two Cargo jobs. Initial cold worktree build: 13m13s; final exact-source Cargo recheck: 0.46s. Own environment/module origins and native SHA256 are in `native-provenance.json`; no saved/copied native artifacts were used.
- All **24 new public regressions passed**. A broader 16-file context/collection suite produced **251 passed, 2 failed**. The two failures are recorded below; neither is suppressed.
- **15 of the 17 previously failing upstream nodes passed**: all five mixed array arithmetic cases, tuple operators, matrix entry-filter statistics, nested literal/HWE/free-variable cases, empty-struct dictionary keys, dictionary methods, collecting columns by key, and ordering by intervals. Per-case results and upstream file hashes are recorded.
- The upstream `test_summarize_variants` now reaches a later missing public API: `hl.summarize_variants`.
- The upstream `test_show__various_types` now reaches its final formatted-string assertion. All construction succeeds; dictionary display order differs (`hello,bar` versus Hail's `bar,hello`). This is a remaining ordering/display compatibility gap. Its module imports PySpark for other tests; the exact selected method body was extracted unchanged, removing only its unused `self` argument and importing Vail. The source/body hashes and adaptation are recorded.
- First upstream attempt lacked `typing_extensions` in this task's environment; installed it and reran. The full table module still requires unrelated PySpark; the exact-body run above covers the requested node without that backend dependency.

The broader suite's remaining failures:

1. `test_collection_functions.py::test_corr_long_arrays_use_shared_constant_size_reducer_state`: correlation of `[1e12, 1e12+1, 1e12+2]` and `[3,2,1]` returns NaN instead of -1. The correlation kernel is unchanged by this candidate. Baseline confirmation is tracked by the coordinating task; this receipt alone does not establish whether it is pre-existing.
2. `test_native_type_policy.py::test_empty_struct_group_key_preserves_rows_and_missingness`: the grouped aggregate dictionary hits an unsupported cast between physically equivalent lists with dictionary metadata. This candidate does not change dictionary collection-cast lowering. Baseline confirmation is tracked by the coordinating task; this receipt alone does not establish whether it is pre-existing.

No main update was performed. Native/Python provenance is tied to the code revision above; subsequent evidence-only commits do not change that code.
