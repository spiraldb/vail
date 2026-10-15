# Full upstream Python API suite after variant QC

Tested Vail commit: `856e1dd482b2329add8de41c4f5515190e1ec042`.
Pinned Hail source: `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.
Native extension SHA-256: `b6a474e2f1529c9e6843e15e008c3e35f03955aa898a495396a32521ed28a27b`.

**The full suite does not pass.** All 62 `test_*.py` modules under `hail/hail/python/test/hail`, including the three backend modules, were attempted. The source census has 1,546 definitions. This scope covers the Hail Python API suite; Scala tests, hailtop infrastructure and doctests are outside this run.

## Authoritative pytest outcomes

- **424 passing source test cases** (parametrization counted separately).
- **794 failing test bodies**, **459 setup failures**, **3 external timeouts**.
- **26 modules with collection errors**. Their uncollected definitions are not counted as pytest passes.
- All collected source cases received an outcome. Continuations ran the cases after timeouts. A further exact-definition selection recovered 14 cases initially excluded because pytest `--deselect` uses prefix matching.
- Twelve incidental imported helper cases are excluded from the above counts (ten pass, two setup failures); the raw event totals remain in `categorized.json`.

## Definition-level evidence

- **407 definitions** pass all their observed pytest parameter cases.
- Of the **476 collection-blocked definitions**, isolated original-body attempts pass **145**, while **331 fail or remain blocked**.
- Thus **994 definitions** still have an observed failure/blocker in either mode. The 145 body-only passes do not repair their modules' pytest collection failures.

Each failing definition receives one category based on its first observed failure. Counts do not mean there is only one issue in the definition, nor that every remaining statement executed.

| First failure category | Definitions | Examples |
| --- | ---: | --- |
| Missing expression/Table/type APIs | 247 | abs, expression show/cache, Table export, coercers, ordering and unsupported aggregators |
| Missing analytics/genomics/plotting APIs | 171 | Regression, PCA/relatedness, sample_qc, Pedigree, experimental, VDS, plotting |
| Hail IR/JVM/backend contracts | 149 | Direct IR construction/typechecking/structural assertions; Spark, service and Py4J-specific tests |
| Missing BlockMatrix/ndarray APIs | 133 | BlockMatrix constructors, from_entry_expr, ndarray indexing and numerical operations |
| Import/export and fixture/storage compatibility | 124 | BGEN/GEN/PLINK, matrix text import, compressed export, cloud paths and legacy Hail .mt fixtures |
| Expression typing/scope/validation | 70 | Mixed numeric array promotion, bind(_ctx), lambda scopes, signatures and error contracts |
| Harness/helper/fixture blockers | 49 | TemporaryFilename/Directory, private comparison helpers, unresolved fixtures/imports |
| Behavior/assertion mismatches | 27 | NaN min/max, ordering/comparisons, fold rounding, partition layout, VCF rendering, phased BN golden |
| Planning/execution/serialization and exception compatibility | 18 | Column aggregation/init, missing matrix row HWE reducer, take coercion, struct pickle and expected error handling |
| Bounded timeouts/native panics | 6 | Three pytest timeouts, one isolated-body timeout, two empty-struct Arrow panic failures |

## Concrete behavior and planning gaps

- Thirteen assertion failures concern exact `n_partitions()` values after key-prefix interval extraction; these are partition-layout/pruning expectations rather than evidence of incorrect collected rows.
- Other behavior failures include NaN min/max (`NaN` versus `0.0`), array/dictionary ordering and comparisons, fold multiplication differing in its last bits, missing scope exceptions, tuple versus list reference metadata, VCF formatting/headers, and diagnostic wording.
- The fixed phased Balding–Nichols genotype list still differs from Hail's golden list. This is an actual compatibility failure, not an IR assertion.
- Matrix column aggregate initialization still rejects group-dependent initializers; another column aggregation is rejected because row fields remain outside an aggregator.
- General MatrixTable row `agg.hardy_weinberg_test` still lacks a per-list reducer. `variant_qc` uses the scalar counts function, so porting QC did not implement that separate matrix aggregate path.
- Two VDS test bodies stop at native `take` argument coercion. Two Struct pickle/dill bodies hit `RecursionError`.
- Several execution-looking failures deliberately trigger errors and then expect Hail's exception class/message. The planning/execution category includes these compatibility failures; it is not a count of broken valid-data query plans.

## Native failures and bounded tests

Both native panic failures involve constructing an empty-field Arrow StructArray without supplying its length:

- `expr/test_expr.py::Tests.test_initop_matrix_aggregate_rows_by_key_rows`.
- `table/test_table.py::test_group_within_partitions_after_import_vcf`.

External pytest limits are 180 seconds per test, with five seconds of supervisor allowance:

- `extract_intervals/test_locus_position.py::test_locus_position.py::test_ht_lt`: External test timeout after 185.0s (limit 180s).
- `methods/test_impex.py::test_impex.py::VCFTests::test_import_vcf`: External test timeout after 185.1s (limit 180s).
- `methods/test_statgen.py::test_statgen.py::Tests::test_split_multi_hts`: External test timeout after 185.2s (limit 180s).
- Isolated-body `table/test_table.py::Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245` exceeded its 180-second worker limit.

These are bounded observations, not proof that the tests would never finish. Later pytest cases were resumed, and none remain unexecuted.

## Randomness and variant QC

- All six definitions in upstream `test_randomness.py` pass through pytest.
- Direct Hail IR/structural assertions remain intact and fail where Vail lacks Hail's IR objects; these are counted as IR/backend contracts.
- Both dedicated upstream `variant_qc` bodies pass. Their module remains collection-blocked by its unrelated `vail.methods` import.
- The combined sample/variant QC call-rate body stops at missing `sample_qc`.
- The phased BN golden mismatch described above remains. Passing the dedicated randomness module does not imply every random-data consumer passes.

## Collection errors

Optional Python packages missing at collection include frozenlist, aiohttp, humanize and rich; pyspark is also absent for Spark-dependent modules. Others are missing Vail modules/types or backend contracts. Supplemental bodies expose further implementation gaps beneath these imports.

| Module | Collection error |
| --- | --- |
| backend/test_py4j_backend.py | ModuleNotFoundError: No module named 'vail.backend.backend'; 'vail.backend' is not a package |
| backend/test_service_backend.py | ModuleNotFoundError: No module named 'vail.backend.service_backend'; 'vail.backend' is not a package |
| backend/test_spark_backend.py | ModuleNotFoundError: No module named 'pyspark' |
| experimental/test_local_whitening.py | ModuleNotFoundError: No module named 'vail.methods' |
| experimental/test_time.py | ModuleNotFoundError: No module named 'vail.experimental' |
| expr/test_freezing.py | ModuleNotFoundError: No module named 'frozenlist' |
| expr/test_types.py | ImportError: cannot import name 'coercer_from_dtype' from 'vail.expr' (/Users/danielking/.codex/worktrees/1611/vail/vail/python/vail/expr/__init__.py) |
| fs/test_worker_driver_fs.py | ModuleNotFoundError: No module named 'aiohttp' |
| genetics/test_pedigree.py | ImportError: cannot import name 'Pedigree' from 'vail.genetics' (/Users/danielking/.codex/worktrees/1611/vail/vail/python/vail/genetics/__init__.py) |
| ggplot/test_ggplot.py | ModuleNotFoundError: No module named 'vail.ggplot' |
| matrixtable/test_file_formats.py | ModuleNotFoundError: No module named 'humanize' |
| methods/relatedness/test_pc_relate.py | ModuleNotFoundError: No module named 'vail.backend.service_backend'; 'vail.backend' is not a package |
| methods/test_pca.py | ModuleNotFoundError: No module named 'vail.methods' |
| methods/test_qc.py | ModuleNotFoundError: No module named 'vail.methods' |
| methods/test_skat.py | AttributeError: module 'vail' has no attribute '_linear_skat' |
| plot/test_plot.py | AttributeError: module 'vail' has no attribute 'plot' |
| table/test_table.py | ModuleNotFoundError: No module named 'pyspark' |
| test_context.py | ModuleNotFoundError: No module named 'vail.backend.backend'; 'vail.backend' is not a package |
| test_hail_in_notebook.py | ModuleNotFoundError: No module named 'rich' |
| typecheck/test_typecheck.py | ModuleNotFoundError: No module named 'vail.typecheck' |
| utils/test_deduplicate.py | ModuleNotFoundError: No module named 'vail.utils.deduplicate' |
| utils/test_hl_hadoop_and_hail_fs.py | ImportError: cannot import name '_get_local_tmpdir' from 'vail.context' (/Users/danielking/.codex/worktrees/1611/vail/vail/python/vail/context.py) |
| utils/test_placement_tree.py | ModuleNotFoundError: No module named 'vail.utils.placement_tree' |
| utils/test_utils.py | ImportError: cannot import name 'frozendict' from 'vail.utils' (/Users/danielking/.codex/worktrees/1611/vail/vail/python/vail/utils/__init__.py) |
| vds/test_combiner.py | ModuleNotFoundError: No module named 'vail.vds' |
| vds/test_vds.py | ModuleNotFoundError: No module named 'vail.vds' |

## Method and reproduction

The existing `full_hail_pytest.py` adapter redirects Hail imports to Vail in a temporary copy, initializes the real native Vail backend, adapts shared fixtures/sample snapshots and backend lifecycle, and leaves body assertions intact. Hail flag decorators are no-ops and JVM health fixtures are omitted. This is an adapted upstream run against Vail, not an unmodified Hail/Spark test run.

`full_hail_bodies.py` supplements only definitions which pytest could not collect. It retains original statements/assertions and parameter values while adapting imports, backend lifecycle and fixtures. Unresolved fixture/helper dependencies are blockers, never passes.

The orchestration uses four isolated pytest module workers, then up to two supplementary body workers; each module runs in its own temporary checkout. Module limits are 1,800 seconds. Raw events, logs, source hashes, source inventory, test-level classifications and error clusters are saved here.

From the Vail project directory:

```sh
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_suite.py
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_bodies.py
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/analyze.py
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_residuals.py
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/analyze.py
.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/write_report.py
```

For a fresh rerun, copy these scripts into a new run directory: existing results are deliberately reused.

## Remaining failures by definition

| Module | Definition | Mode | Category | First error |
| --- | --- | --- | --- | --- |
| backend/test_py4j_backend.py | test_interrupt_propagates | isolated_body | hail_ir_jvm_or_backend_internals | name '_make_interrupt_safe' is not defined |
| backend/test_py4j_backend.py | test_interrupt_closes_the_connection | isolated_body | hail_ir_jvm_or_backend_internals | name '_make_interrupt_safe' is not defined |
| backend/test_py4j_backend.py | test_exceptions_do_not_close_the_connection | isolated_body | hail_ir_jvm_or_backend_internals | name '_make_interrupt_safe' is not defined |
| backend/test_py4j_backend.py | test_successful_commands_pass_through | isolated_body | hail_ir_jvm_or_backend_internals | name '_make_interrupt_safe' is not defined |
| backend/test_py4j_backend.py | test_patching_is_idempotent | isolated_body | hail_ir_jvm_or_backend_internals | name '_make_interrupt_safe' is not defined |
| backend/test_py4j_backend.py | test_py4j_connections_are_interrupt_safe | isolated_body | hail_ir_jvm_or_backend_internals |  |
| backend/test_py4j_backend.py | test_log_suppress_shutdown_exceptions_swallows_exceptions | isolated_body | hail_ir_jvm_or_backend_internals | name '_log_suppress_exceptions' is not defined |
| backend/test_py4j_backend.py | test_log_suppress_shutdown_exceptions_propagates_interrupts | isolated_body | hail_ir_jvm_or_backend_internals | name '_log_suppress_exceptions' is not defined |
| backend/test_py4j_backend.py | test_rpc_failures_cancel_the_operation_and_propagate | isolated_body | hail_ir_jvm_or_backend_internals | 'monkeypatch' |
| backend/test_py4j_backend.py | test_rpc_cancel_failures_do_not_mask_the_original_failure | isolated_body | hail_ir_jvm_or_backend_internals | 'monkeypatch' |
| backend/test_py4j_backend.py | test_rpc_returns_successful_responses | isolated_body | hail_ir_jvm_or_backend_internals | 'monkeypatch' |
| backend/test_py4j_backend.py | test_rpc_java_errors_raise_fatal_errors | isolated_body | hail_ir_jvm_or_backend_internals | 'monkeypatch' |
| backend/test_service_backend.py | test_big_worker_has_big_memory | isolated_body | hail_ir_jvm_or_backend_internals | 'mocker' |
| backend/test_service_backend.py | test_regions | isolated_body | hail_ir_jvm_or_backend_internals | 'mocker' |
| backend/test_service_backend.py | test_driver_and_worker_job_groups | isolated_body | hail_ir_jvm_or_backend_internals | name 'ServiceBackend' is not defined |
| backend/test_service_backend.py | test_attach_to_existing_batch | isolated_body | hail_ir_jvm_or_backend_internals | 'request' |
| backend/test_service_backend.py | test_explicit_regions | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute 'init' |
| backend/test_service_backend.py | test_any_region | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute 'init' |
| backend/test_service_backend.py | test_default_region | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute 'init' |
| backend/test_spark_backend.py | test_copy_spark_log | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute 'init' |
| backend/test_spark_backend.py | test_init_without_existing_spark_context | isolated_body | hail_ir_jvm_or_backend_internals | 'request' |
| backend/test_spark_backend.py | test_init_with_existing_spark_context | isolated_body | hail_ir_jvm_or_backend_internals | 'request' |
| backend/test_spark_backend.py | TestSparkConf.test_ | isolated_body | hail_ir_jvm_or_backend_internals | name 'cases' is not defined |
| backend/test_spark_backend.py | test_min_block_size_pos_int | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute 'init' |
| experimental/test_annotation_db.py | TestAnnotationDB.test_uniqueness | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'genomic_range_table' |
| experimental/test_dsp_necessary_functions_have_not_moved.py | test_dsp_used_functions_have_not_moved | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_ld_score_univariate | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| experimental/test_experimental.py | Tests.test_ld_score_annotated | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| experimental/test_experimental.py | Tests.test_ld_score_regression_1 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_ld_score_regression_2 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_sparse | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_sparse_split_haploid | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_define_function | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_pc_project | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'hwe_normalized_pca' |
| experimental/test_experimental.py | Tests.test_mt_full_outer_join | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_mt_full_outer_join_self | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_block_matrices_tofiles | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| experimental/test_experimental.py | Tests.test_export_block_matrices | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| experimental/test_experimental.py | Tests.test_trivial_loop | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_loop | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_nested_loops | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_loop_errors | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_loop_with_struct_of_strings | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_experimental.py | Tests.test_loop_memory | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| experimental/test_local_whitening.py | test_local_whitening | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| experimental/test_local_whitening.py | test_local_whitening_singleton_final_partition | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| experimental/test_time.py | test_strftime | isolated_body | harness_or_fixture_setup | name 'htime' is not defined |
| experimental/test_time.py | test_strptime | isolated_body | harness_or_fixture_setup | name 'htime' is not defined |
| expr/test_expr.py | Tests.test_range | pytest | planning_or_execution | RuntimeError: Execution error: step can't be 0 for function range(start [, stop, step]) |
| expr/test_expr.py | Tests.test_zeros | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'zeros' |
| expr/test_expr.py | Tests.test_operators | pytest | expression_typing_scope_or_validation | TypeError: incompatible comparison types: ttuple(types=(bool, float64)) and ttuple(types=(float64, float64)) |
| expr/test_expr.py | Tests.test_array_slicing | pytest | expression_typing_scope_or_validation | RuntimeError: Execution error: invalid expression: step cannot be 0 for array slice |
| expr/test_expr.py | Tests.test_dict_methods | pytest | missing_expression_or_table_api | AttributeError: 'KeysView' object has no attribute 'contains' |
| expr/test_expr.py | Tests.test_dict_missing_error | pytest | planning_or_execution | RuntimeError: Execution error: DataFusion expression failed: Execution error: dictionary key is missing |
| expr/test_expr.py | Tests.test_genetics_constructors | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'parse_locus_interval' |
| expr/test_expr.py | Tests.test_counter | pytest | behavior_or_assertion_mismatch | assert [([], 2), ([1...1), (None, 1)] == [([], 2), ([1...2), (None, 1)]; E     At index 1 diff: ([1, 2, 3], 2) != ([1, 2], 1); Use -v to get more diff |
| expr/test_expr.py | Tests.test_aggfold_agg | pytest | behavior_or_assertion_mismatch | AssertionError: Struct(x=4950, y=9.33262154439441e+157) != Struct(x=4950, y=9.332621544394414e+157) |
| expr/test_expr.py | Tests.test_agg_collect_all_types_runs | pytest | expression_typing_scope_or_validation | TypeError: phased must be bool |
| expr/test_expr.py | Tests.test_aggregator_scope | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: unbound lambda variable "43cafb17b7234b2db03f43ad6f561926" |
| expr/test_expr.py | Tests.test_aggregator_bindings | pytest | expression_typing_scope_or_validation | TypeError: bind() got an unexpected keyword argument '_ctx' |
| expr/test_expr.py | Tests.test_aggregators_linreg | pytest | missing_expression_or_table_api | AttributeError: add_index |
| expr/test_expr.py | Tests.test_aggregator_info_score | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| expr/test_expr.py | Tests.test_aggregator_info_score_works_with_bgen_import | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| expr/test_expr.py | Tests.test_aggregator_group_by_sorts_result | pytest | missing_expression_or_table_api | NotImplementedError: array_sort is not yet enabled for this element type |
| expr/test_expr.py | Tests.test_switch | pytest | planning_or_execution | RuntimeError: Execution error: DataFusion expression failed: Execution error: foo |
| expr/test_expr.py | Tests.test_case | pytest | planning_or_execution | RuntimeError: Execution error: DataFusion expression failed: Execution error: foo |
| expr/test_expr.py | Tests.test_shadowed_struct_fields | pytest | missing_expression_or_table_api | AttributeError: _warn_on_shadowed_name |
| expr/test_expr.py | Tests.test_iter | pytest | expression_typing_scope_or_validation | TypeError: only struct expressions can be expanded as mappings |
| expr/test_expr.py | Tests.test_aggregator_any_and_all | pytest | missing_expression_or_table_api | AttributeError: cache |
| expr/test_expr.py | Tests.test_agg_prev_nonnull | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute '_values_similar' |
| expr/test_expr.py | Tests.test_agg_minmax | pytest | behavior_or_assertion_mismatch | AssertionError: nan != 0.0 |
| expr/test_expr.py | Tests.test_str_parsing | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'is_nan' |
| expr/test_expr.py | Tests.test_floor_division | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: incompatible types Array { array: Primitive(Float32) } and Array { array: Primitive(Float64) } |
| expr/test_expr.py | Tests.test_addition | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: incompatible types Array { array: Primitive(Float32) } and Array { array: Primitive(Float64) } |
| expr/test_expr.py | Tests.test_subtraction | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: incompatible types Array { array: Primitive(Float32) } and Array { array: Primitive(Float64) } |
| expr/test_expr.py | Tests.test_multiplication | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: incompatible types Array { array: Primitive(Float32) } and Array { array: Primitive(Float64) } |
| expr/test_expr.py | Tests.test_modulus | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: incompatible types Array { array: Primitive(Float32) } and Array { array: Primitive(Float64) } |
| expr/test_expr.py | Tests.test_comparisons | pytest | behavior_or_assertion_mismatch | AssertionError: (True, False); assert True == False |
| expr/test_expr.py | Tests.test_bools_can_math | pytest | expression_typing_scope_or_validation | TypeError: arithmetic requires a numeric expression |
| expr/test_expr.py | Tests.test_gp_dosage | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'gp_dosage' |
| expr/test_expr.py | Tests.test_call | pytest | expression_typing_scope_or_validation | TypeError: call requires at most two alleles and a Boolean phased flag |
| expr/test_expr.py | Tests.test_call_unphase | pytest | missing_expression_or_table_api | AttributeError: unphase |
| expr/test_expr.py | Tests.test_large_allele_encoding | pytest | missing_expression_or_table_api | AttributeError: '_PrimitiveType' object has no attribute '_to_encoding' |
| expr/test_expr.py | Tests.test_call_contains_allele | pytest | missing_expression_or_table_api | AttributeError: contains_allele |
| expr/test_expr.py | Tests.test_call_unphase_diploid_gt_index | pytest | missing_expression_or_table_api | AttributeError: unphased_diploid_gt_index |
| expr/test_expr.py | Tests.test_locus_window | pytest | missing_expression_or_table_api | AttributeError: window |
| expr/test_expr.py | Tests.test_dict_conversions | pytest | expression_typing_scope_or_validation | TypeError: array expects a list or an array expression |
| expr/test_expr.py | Tests.test_sorted | pytest | missing_expression_or_table_api | NotImplementedError: expression-valued sorted keys are not yet implemented |
| expr/test_expr.py | Tests.test_sort_by | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute '_sort_by' |
| expr/test_expr.py | Tests.test_max | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'nanmax' |
| expr/test_expr.py | Tests.test_min | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'nanmin' |
| expr/test_expr.py | Tests.test_abs | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'abs' |
| expr/test_expr.py | Tests.test_sign | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'sign' |
| expr/test_expr.py | Tests.test_argmin_and_argmax | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'argmax' |
| expr/test_expr.py | Tests.test_show_row_key_regression | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_expr.py | Tests.test_show_expression | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_expr.py | Tests.test_export_genetic_data | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| expr/test_expr.py | Tests.test_tuple_ops | pytest | expression_typing_scope_or_validation | TypeError: len requires an array, set, or dictionary expression |
| expr/test_expr.py | Tests.test_interval_ops | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'parse_locus_interval' |
| expr/test_expr.py | Tests.test_locus_interval_constructors | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'locus_interval' |
| expr/test_expr.py | Tests.test_locus_window_type | pytest | missing_expression_or_table_api | AttributeError: window |
| expr/test_expr.py | Tests.test_reference_genome_fns | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'contig_length' |
| expr/test_expr.py | Tests.test_initop_matrix_aggregate_rows_by_key_rows | pytest | timeout_or_native_crash | External error: task 913 panicked with message "called `Result::unwrap()` on an `Err` value: InvalidArgumentError(\"use StructArray::try_new_with_length or StructArray::new_empty_fields to create a struct array with no fields so that the length can be set correctly\")" |
| expr/test_expr.py | Tests.test_call_stats_init | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| expr/test_expr.py | Tests.test_mendel_error_code | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'mendel_error_code' |
| expr/test_expr.py | Tests.test_min_rep | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'min_rep' |
| expr/test_expr.py | Tests.test_min_rep_error | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'min_rep' |
| expr/test_expr.py | Tests.test_uniroot_1 | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'uniroot' |
| expr/test_expr.py | Tests.test_uniroot_2 | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'uniroot' |
| expr/test_expr.py | Tests.test_uniroot_3 | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'uniroot' |
| expr/test_expr.py | Tests.test_uniroot_multiple_roots | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'uniroot' |
| expr/test_expr.py | Tests.test_dnorm | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'dnorm' |
| expr/test_expr.py | Tests.test_pnorm | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pnorm' |
| expr/test_expr.py | Tests.test_qnorm | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'qnorm' |
| expr/test_expr.py | Tests.test_dchisq | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'dchisq' |
| expr/test_expr.py | Tests.test_pchisqtail | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pchisqtail' |
| expr/test_expr.py | Tests.test_qchisqtail | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'qchisqtail' |
| expr/test_expr.py | Tests.test_pT | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pT' |
| expr/test_expr.py | Tests.test_pF | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pF' |
| expr/test_expr.py | Tests.test_chi_squared_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'chi_squared_test' |
| expr/test_expr.py | Tests.test_fisher_exact_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'fisher_exact_test' |
| expr/test_expr.py | Tests.test_contingency_table_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'contingency_table_test' |
| expr/test_expr.py | Tests.test_cochran_mantel_haenszel_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'cochran_mantel_haenszel_test' |
| expr/test_expr.py | Tests.test_hardy_weinberg_agg_1 | pytest | expression_typing_scope_or_validation | TypeError: cannot infer a Vail type for Expression |
| expr/test_expr.py | Tests.test_hardy_weinberg_agg_2 | pytest | expression_typing_scope_or_validation | TypeError: cannot infer a Vail type for Expression |
| expr/test_expr.py | Tests.test_pl_to_gp | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pl_to_gp' |
| expr/test_expr.py | Tests.test_pl_dosage | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pl_dosage' |
| expr/test_expr.py | Tests.test_literal_with_nested_expr | pytest | expression_typing_scope_or_validation | TypeError: cannot infer a Vail type for Expression |
| expr/test_expr.py | Tests.test_format | pytest | missing_expression_or_table_api | NotImplementedError: DataFusion has no Hail-compatible Java printf formatting implementation |
| expr/test_expr.py | Tests.test_dict_keyed_by_set | pytest | missing_expression_or_table_api | NotImplementedError: Set/Dictionary/ndarray ordering and keying are unsupported |
| expr/test_expr.py | Tests.test_dict_keyed_by_dict | pytest | missing_expression_or_table_api | NotImplementedError: Set/Dictionary/ndarray ordering and keying are unsupported |
| expr/test_expr.py | Tests.test_frozendict_as_literal | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'frozendict' |
| expr/test_expr.py | Tests.test_literal_with_empty_struct_key | pytest | expression_typing_scope_or_validation | TypeError: keywords must be strings |
| expr/test_expr.py | Tests.test_cumulative_sum | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'cumulative_sum' |
| expr/test_expr.py | Tests.test_nan_inf_checks | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'is_finite' |
| expr/test_expr.py | Tests.test_array_and_if_requiredness | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_expr.py | Tests.test_bit_ops_types | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'bit_and' |
| expr/test_expr.py | Tests.test_bit_shifts | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'bit_lshift' |
| expr/test_expr.py | Tests.test_bit_shift_edge_cases | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'bit_lshift' |
| expr/test_expr.py | Tests.test_bit_shift_errors | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'bit_lshift' |
| expr/test_expr.py | Tests.test_prev_non_null | pytest | missing_expression_or_table_api | AttributeError: module 'vail.agg' has no attribute '_prev_nonnull' |
| expr/test_expr.py | Tests.test_summarize_runs | pytest | missing_expression_or_table_api | AttributeError: summarize |
| expr/test_expr.py | Tests.test_variant_str | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'variant_str'. Did you mean: 'variant_qc'? |
| expr/test_expr.py | Tests.test_collection_getitem | pytest | missing_expression_or_table_api | AttributeError: inner |
| expr/test_expr.py | Tests.test_struct_collection_getattr | pytest | behavior_or_assertion_mismatch | AssertionError: Regex pattern did not match.; Expected regex: 'has no field'; Actual message: 'y' |
| expr/test_expr.py | Tests.test_binary_search | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'binary_search' |
| expr/test_expr.py | Tests.test_struct_slice | pytest | expression_typing_scope_or_validation | KeyError: slice(1, None, None) |
| expr/test_expr.py | Tests.test_numpy_conversions | pytest | expression_typing_scope_or_validation | TypeError: cannot infer a Vail type for int32 |
| expr/test_expr.py | Tests.test_parse_json | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'locus_interval' |
| expr/test_expr.py | Tests.test_expr_persist | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| expr/test_expr.py | Tests.test_struct_expression_expr_rename | pytest | missing_expression_or_table_api | AttributeError: rename |
| expr/test_expr.py | Tests.test_split_line | pytest | expression_typing_scope_or_validation | ValueError: delimiter must be a single ASCII character |
| expr/test_expr.py | test_approx_cdf | pytest | missing_expression_or_table_api | NotImplementedError: approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable |
| expr/test_expr.py | test_approx_cdf_accuracy | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| expr/test_expr.py | test_approx_cdf_col_aggregate | pytest | missing_expression_or_table_api | NotImplementedError: approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable |
| expr/test_expr.py | test_error_from_cdf | pytest | missing_expression_or_table_api | NotImplementedError: approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable |
| expr/test_expr.py | test_cdf_combine | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| expr/test_expr.py | test_approx_cdf_array_agg | pytest | missing_expression_or_table_api | NotImplementedError: approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable |
| expr/test_expr.py | test_export_entry | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| expr/test_expr.py | test_stream_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.NA is not implemented |
| expr/test_expr.py | test_keyed_intersection | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'keyed_intersection' |
| expr/test_expr.py | test_keyed_union | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'keyed_union' |
| expr/test_expr.py | test_to_relational_row_and_col_refs | pytest | missing_expression_or_table_api | AttributeError: _to_relational_preserving_rows_and_cols |
| expr/test_expr.py | test_locus_addition | pytest | expression_typing_scope_or_validation | TypeError: arithmetic requires a numeric expression |
| expr/test_expr.py | test_reservoir_sampling_pointer_type | pytest | missing_expression_or_table_api | AttributeError: module 'vail.agg' has no attribute '_reservoir_sample' |
| expr/test_expr.py | test_reservoir_sampling | pytest | expression_typing_scope_or_validation | TypeError: Table._generate() missing 1 required positional argument: 'rowfn' |
| expr/test_expr.py | test_struct_expr_rename_order | pytest | missing_expression_or_table_api | AttributeError: rename |
| expr/test_expr.py | test_cannot_aggregate_in_local_scan | pytest | behavior_or_assertion_mismatch | Failed: DID NOT RAISE ExpressionException |
| expr/test_expr.py | test_zip_join_producers | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute '_zip_join_producers' |
| expr/test_freezing.py | test_collect_as_set_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_counter_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_collect_as_set_tuple_of_list | isolated_body | expression_typing_scope_or_validation | unhashable type: 'list' |
| expr/test_freezing.py | test_counter_tuple_of_list | isolated_body | expression_typing_scope_or_validation | unhashable type: 'list' |
| expr/test_freezing.py | test_collect_as_set_struct_of_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_counter_struct_of_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_collect_as_set_dict_value_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_counter_dict_value_list | isolated_body | missing_expression_or_table_api | Set/Dictionary ordering and keying are unsupported |
| expr/test_freezing.py | test_collect_as_set_list_list_list_set_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_counter_list_list_list_set_list | isolated_body | missing_expression_or_table_api | Set/Dictionary ordering and keying are unsupported |
| expr/test_freezing.py | test_collect_dict_key_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_collect_dict_key_and_value_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_collect_set_list | isolated_body | hail_ir_jvm_or_backend_internals | name 'frozenlist' is not defined |
| expr/test_freezing.py | test_collect_set_dict_list_list | isolated_body | missing_expression_or_table_api | DataFusion array_distinct does not preserve Vail key equality for this element type |
| expr/test_freezing.py | test_collect_set_tuple_struct_struct_list | isolated_body | expression_typing_scope_or_validation | unhashable type: 'list' |
| expr/test_functions.py | test_deprecated_binom_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'binom_test' |
| expr/test_functions.py | test_binom_test | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'binom_test' |
| expr/test_functions.py | test_pchisqtail | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pchisqtail' |
| expr/test_functions.py | test_pgenchisq | pytest | missing_expression_or_table_api | NotImplementedError: typed text imports currently support primitive fields |
| expr/test_functions.py | test_array | pytest | expression_typing_scope_or_validation | TypeError: array conversion requires an array, set, or dictionary |
| expr/test_functions.py | test_literal_free_vars | pytest | expression_typing_scope_or_validation | TypeError: cannot infer a Vail type for Expression |
| expr/test_math.py | test_logit | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'logit' |
| expr/test_math.py | test_expit | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'expit'. Did you mean: 'expr'? |
| expr/test_ndarrays.py | test_ndarray_ref | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray indexing is not implemented |
| expr/test_ndarrays.py | test_ndarray_ref_bounds_check | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray indexing is not implemented |
| expr/test_ndarrays.py | test_ndarray_slice | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray indexing is not implemented |
| expr/test_ndarrays.py | test_ndarray_transposed_slice | pytest | missing_expression_or_table_api | AttributeError: T |
| expr/test_ndarrays.py | test_ndarray_eval | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_ndarray_shape | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_ndarray_reshape | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray reshape is not implemented |
| expr/test_ndarrays.py | test_ndarray_map1 | pytest | expression_typing_scope_or_validation | TypeError: expected an array or set expression |
| expr/test_ndarrays.py | test_ndarray_map2 | pytest | expression_typing_scope_or_validation | TypeError: arithmetic requires a numeric expression |
| expr/test_ndarrays.py | test_ndarray_sum | pytest | missing_expression_or_table_api | AttributeError: sum |
| expr/test_ndarrays.py | test_ndarray_transpose | pytest | missing_expression_or_table_api | AttributeError: T |
| expr/test_ndarrays.py | test_ndarray_matmul | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray matrix multiplication is not implemented |
| expr/test_ndarrays.py | test_ndarray_matmul_dgemv | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray matrix multiplication is not implemented |
| expr/test_ndarrays.py | test_ndarray_big | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_ndarray_full | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'zeros' |
| expr/test_ndarrays.py | test_ndarray_arange | pytest | planning_or_execution | RuntimeError: Execution error: step can't be 0 for function range(start [, stop, step]) |
| expr/test_ndarrays.py | test_ndarray_mixed | pytest | missing_expression_or_table_api | AttributeError: map |
| expr/test_ndarrays.py | test_ndarray_show | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_ndarrays.py | test_ndarray_diagonal | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'diagonal' |
| expr/test_ndarrays.py | test_ndarray_solve_triangular | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'solve_triangular' |
| expr/test_ndarrays.py | test_ndarray_solve | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'solve' |
| expr/test_ndarrays.py | test_ndarray_qr | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'qr' |
| expr/test_ndarrays.py | test_svd | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'svd' |
| expr/test_ndarrays.py | test_eigh | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'eigh' |
| expr/test_ndarrays.py | test_numpy_interop | pytest | missing_matrix_numerical_api | TypeError: ndarray elements must be int32/int64/float32/float64 |
| expr/test_ndarrays.py | test_ndarray_emitter_extract | pytest | missing_expression_or_table_api | AttributeError: map |
| expr/test_ndarrays.py | test_ndarrays_transmute_ops | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_inv | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'inv' |
| expr/test_ndarrays.py | test_concatenate | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'concatenate' |
| expr/test_ndarrays.py | test_concatenate_differing_shapes | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'concatenate' |
| expr/test_ndarrays.py | test_vstack | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'vstack' |
| expr/test_ndarrays.py | test_vstack_2 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'vstack' |
| expr/test_ndarrays.py | test_hstack | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'hstack' |
| expr/test_ndarrays.py | test_eye | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'eye' |
| expr/test_ndarrays.py | test_identity | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'identity' |
| expr/test_ndarrays.py | test_agg_ndarray_sum_0_to_10 | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_agg_ndarray_sum_ones_1d | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'ones' |
| expr/test_ndarrays.py | test_agg_ndarray_sum_ones_2d | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'ones' |
| expr/test_ndarrays.py | test_agg_ndarray_sum_with_transposes | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray reshape is not implemented |
| expr/test_ndarrays.py | test_agg_ndarray_mismatched_dims_raises_fatal_error | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'ones' |
| expr/test_ndarrays.py | test_maximum_minimuim | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_ndarrays.py | test_ndarray_broadcasting_with_decorator | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'sqrt' |
| expr/test_ndarrays.py | test_ndarray_indices_aggregations | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.nd' has no attribute 'ones' |
| expr/test_ndarrays.py | test_ndarray_log_broadcasting | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'log' |
| expr/test_ndarrays.py | test_ndarray_oom_from_stream_pipeline | pytest | expression_typing_scope_or_validation | TypeError: nd.array requires int32/int64/float32/float64 data |
| expr/test_show.py | test_show_1 | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_2 | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_3 | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_4 | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_5 | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_rows_table | pytest | missing_expression_or_table_api | AttributeError: show |
| expr/test_show.py | test_show_mt_duplicate_col_key | pytest | missing_expression_or_table_api | AttributeError: 'str' object has no attribute 'table_show' |
| expr/test_show.py | test_show_mt_fewer_cols | pytest | missing_expression_or_table_api | AttributeError: 'str' object has no attribute 'table_show' |
| expr/test_types.py | Tests.test_parser_roundtrip | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_eval_roundtrip | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_equality | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_type_jvm_roundtrip | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_pretty_roundtrip | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_coercers_can_coerce | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_nested_type_to_spark | isolated_body | missing_expression_or_table_api | to_spark |
| expr/test_types.py | Tests.test_rename_not_unique | isolated_body | missing_expression_or_table_api | 'tstruct' object has no attribute '_rename' |
| expr/test_types.py | Tests.test_get_context | isolated_body | missing_matrix_numerical_api | tndarray requires a numeric type and nonnegative rank |
| expr/test_types.py | Tests.test_tlocus_schema_from_rg_matches_scala | isolated_body | missing_expression_or_table_api | type object 'tlocus' has no attribute '_schema_from_rg' |
| extract_intervals/test_key_prefix.py | test_mt_lt | pytest | behavior_or_assertion_mismatch | assert 20 == 15; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785deb0>.n_partitions |
| extract_intervals/test_key_prefix.py | test_mt_le | pytest | behavior_or_assertion_mismatch | assert 20 == 15; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785e810>.n_partitions |
| extract_intervals/test_key_prefix.py | test_mt_eq | pytest | behavior_or_assertion_mismatch | assert 20 == 1; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785fda0>.n_partitions |
| extract_intervals/test_key_prefix.py | test_mt_ge | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785d970>.n_partitions |
| extract_intervals/test_key_prefix.py | test_mt_gt | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785e510>.n_partitions |
| extract_intervals/test_key_prefix.py | test_mt_ge_with_bindings | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x14785f170>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_lt | pytest | behavior_or_assertion_mismatch | assert 20 == 15; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x14785da00>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_le | pytest | behavior_or_assertion_mismatch | assert 20 == 15; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x14785dc40>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_eq | pytest | behavior_or_assertion_mismatch | assert 20 == 1; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x14785ddf0>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_ge | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x148baab10>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_gt | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x148bab260>.n_partitions |
| extract_intervals/test_key_prefix.py | test_ht_ge_with_bindings | pytest | behavior_or_assertion_mismatch | assert 20 == 6; +  where 20 = n_partitions(); +    where n_partitions = <vail.Table object at 0x148ba9fd0>.n_partitions |
| extract_intervals/test_key_prefix.py | test_GRCh38 | pytest | behavior_or_assertion_mismatch | assert 10 == 1; +  where 10 = n_partitions(); +    where n_partitions = <vail.matrixtable.MatrixTable object at 0x120ab1940>.n_partitions |
| extract_intervals/test_locus_position.py | test_ht_lt | pytest | timeout_or_native_crash | External test timeout after 185.0s (limit 180s) |
| fs/test_worker_driver_fs.py | test_requester_pays_no_settings | isolated_body | format_io_and_fixture_compatibility | text IO currently supports local paths |
| fs/test_worker_driver_fs.py | test_requester_pays_write_no_settings | isolated_body | hail_ir_jvm_or_backend_internals | name 'ClientResponseError' is not defined |
| fs/test_worker_driver_fs.py | test_requester_pays_write_with_project | isolated_body | hail_ir_jvm_or_backend_internals | name 'secret_alnum_string' is not defined |
| fs/test_worker_driver_fs.py | test_requester_pays_with_project | isolated_body | format_io_and_fixture_compatibility | text IO currently supports local paths |
| fs/test_worker_driver_fs.py | test_requester_pays_no_project | isolated_body | format_io_and_fixture_compatibility | Regex pattern did not match.   Expected regex: 'Bucket is a requester pays bucket but no user project provided'   Actual message: 'text IO currently supports local paths' |
| fs/test_worker_driver_fs.py | test_requester_pays_with_project_more_than_one_partition | isolated_body | format_io_and_fixture_compatibility | text IO currently supports local paths |
| fs/test_worker_driver_fs.py | test_requester_pays_with_project_more_than_one_partition_no_project | isolated_body | format_io_and_fixture_compatibility | Regex pattern did not match.   Expected regex: 'Bucket is a requester pays bucket but no user project provided'   Actual message: 'text IO currently supports local paths' |
| fs/test_worker_driver_fs.py | test_can_access_public_blobs | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_exists' |
| fs/test_worker_driver_fs.py | test_qob_can_use_sas_tokens | isolated_body | hail_ir_jvm_or_backend_internals | name 'AzureAsyncFS' is not defined |
| genetics/test_call.py | Tests.test_hom_ref | pytest | missing_expression_or_table_api | AttributeError: 'Call' object has no attribute 'is_haploid' |
| genetics/test_call.py | Tests.test_het | pytest | missing_expression_or_table_api | AttributeError: 'Call' object has no attribute 'is_haploid' |
| genetics/test_call.py | Tests.test_hom_var | pytest | missing_expression_or_table_api | AttributeError: 'Call' object has no attribute 'is_haploid' |
| genetics/test_call.py | Tests.test_haploid | pytest | missing_expression_or_table_api | AttributeError: 'Call' object has no attribute 'is_haploid' |
| genetics/test_call.py | Tests.test_zeroploid | pytest | missing_expression_or_table_api | AttributeError: 'Call' object has no attribute 'is_haploid' |
| genetics/test_locus.py | test_constructor | pytest | missing_expression_or_table_api | AttributeError: type object 'Locus' has no attribute 'parse' |
| genetics/test_pedigree.py | Tests.test_trios | isolated_body | missing_high_level_api | name 'Trio' is not defined |
| genetics/test_pedigree.py | Tests.test_pedigree | isolated_body | missing_high_level_api | name 'Pedigree' is not defined |
| genetics/test_reference_genome.py | test_reference_genome | pytest | behavior_or_assertion_mismatch | AssertionError: assert ('X',) == ['X']; E     Use -v to get more diff |
| genetics/test_reference_genome.py | test_reference_genome_sequence | pytest | missing_expression_or_table_api | NotImplementedError: FASTA-backed reference construction is not implemented |
| genetics/test_reference_genome.py | test_reference_genome_liftover | pytest | missing_expression_or_table_api | AttributeError: 'ReferenceGenome' object has no attribute 'has_liftover' |
| genetics/test_reference_genome.py | test_liftover_strand | pytest | missing_expression_or_table_api | AttributeError: 'ReferenceGenome' object has no attribute 'add_liftover' |
| genetics/test_reference_genome.py | test_read_custom_reference_genome | pytest | missing_expression_or_table_api | AttributeError: 'NativeBackend' object has no attribute '_references' |
| genetics/test_reference_genome.py | test_custom_reference_read_write | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| genetics/test_reference_genome.py | test_locus_from_global_position | pytest | missing_expression_or_table_api | AttributeError: 'ReferenceGenome' object has no attribute 'global_positions_dict' |
| genetics/test_reference_genome.py | test_locus_from_global_position_negative_pos | pytest | missing_expression_or_table_api | AttributeError: 'ReferenceGenome' object has no attribute 'locus_from_global_position' |
| genetics/test_reference_genome.py | test_locus_from_global_position_too_long | pytest | missing_expression_or_table_api | AttributeError: 'ReferenceGenome' object has no attribute 'locus_from_global_position' |
| ggplot/test_ggplot.py | test_geom_point_line_text_col_area | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_manhattan_plot | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_histogram | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_separate_traces_per_group | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_geom_ribbon | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_default_scale_no_repeat_colors | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_scale_color_manual | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_weighted_bar | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_faceting | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| ggplot/test_ggplot.py | test_matrix_tables | isolated_body | missing_high_level_api | name 'ggplot' is not defined |
| linalg/test_linalg.py | test_from_entry_expr_empty_parts | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_from_entry_expr_options | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray indexing is not implemented |
| linalg/test_linalg.py | test_from_entry_expr_raises_when_values_missing | pytest | missing_matrix_numerical_api | AssertionError: Regex pattern did not match.; Expected regex: 'Cannot construct an ndarray with missing values'; Actual message: "type object 'BlockMatrix' has no attribute 'from_entry_expr'" |
| linalg/test_linalg.py | test_write_from_entry_expr_overwrite | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| linalg/test_linalg.py | test_write_from_entry_expr_overwrite_non_field_expressions | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| linalg/test_linalg.py | test_random_uniform | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'random' |
| linalg/test_linalg.py | test_bm_to_numpy | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_bm_transpose_to_numpy | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_block_matrix_from_numpy | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_block_matrix_from_numpy_transpose | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_block_matrix_to_file_transpose | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| linalg/test_linalg.py | test_numpy_read_block_matrix_to_file | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| linalg/test_linalg.py | test_block_matrix_from_numpy_bytes | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| linalg/test_linalg.py | test_block_matrix_from_file | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| linalg/test_linalg.py | test_block_matrix_persist_unpersist | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_numpy_round_trip_force_blocking | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_to_table | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_to_table_maximum_cache_memory_in_bytes_limits | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_to_matrix_table_0 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_to_matrix_table_1 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'random' |
| linalg/test_linalg.py | test_block_matrix_elementwise_arithmetic | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_sparse_broadcast_add | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_block_matrix_elementwise_close_arithmetic | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_block_matrix_raises | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_matrix_sums | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_sparse_matrix_sums | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_tree_matmul | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_fill | pytest | missing_matrix_numerical_api | AttributeError: 'BlockMatrix' object has no attribute 'block_size' |
| linalg/test_linalg.py | test_sum | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_sum_with_sparsify | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_slicing_0 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_slicing_1 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_slicing_2 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_block_matrix_illegal_indexing | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute '_create' |
| linalg/test_linalg.py | test_diagonal_sparse | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_slices_with_sparsify | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_row_intervals_0 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_row_intervals_1 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_band_0 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_band_1 | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_triangle | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_rectangles | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_export_rectangles | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_export_rectangles_sparse | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_export_rectangles_filtered | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_export_blocks | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_rectangles_to_numpy | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_to_ndarray | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_block_matrix_entries | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_from_entry_expr_filtered | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| linalg/test_linalg.py | test_array_windows | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_array_windows_illegal_arguments | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_per_contig | pytest | missing_matrix_numerical_api | AttributeError: module 'vail' has no attribute '_locus_windows_per_contig' |
| linalg/test_linalg.py | test_locus_windows_1 | pytest | missing_matrix_numerical_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| linalg/test_linalg.py | test_locus_windows_2 | pytest | missing_matrix_numerical_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| linalg/test_linalg.py | test_locus_windows_3 | pytest | missing_matrix_numerical_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| linalg/test_linalg.py | test_locus_windows_4 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_5 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_6 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_7 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_8 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_9 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_10 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_11 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_locus_windows_12 | pytest | missing_matrix_numerical_api | AttributeError: module 'vail.linalg' has no attribute 'utils' |
| linalg/test_linalg.py | test_write_overwrite | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_stage_locally | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_svd | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_filtering | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_is_sparse | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparsify_blocks | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_sparse_transposition | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_row_blockmatrix_sum | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_numpy' |
| linalg/test_linalg.py | test_map | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_ndarray' |
| linalg/test_linalg.py | test_from_entry_expr_simple_gt_n_alt_alleles | pytest | missing_matrix_numerical_api | AttributeError: module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| linalg/test_linalg.py | test_from_entry_expr_simple_direct_from_field | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| linalg/test_linalg.py | test_from_entry_expr_simple_with_float_conversion | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| linalg/test_linalg.py | test_write_from_entry_expr_simple | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| linalg/test_linalg.py | test_write_block_matrix_with_block_size_eq_n_cols | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| matrixtable/test_file_formats.py | test_write | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| matrixtable/test_file_formats.py | test_backward_compatability_mt | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| matrixtable/test_file_formats.py | test_backward_compatability_ht | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| matrixtable/test_grouped_matrix_table.py | Tests.test_errors_caught_correctly | pytest | expression_typing_scope_or_validation | ValueError: positional grouping requires field references |
| matrixtable/test_grouped_matrix_table.py | Tests.test_joins_work_correctly | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: unbound lambda variable "c0439506116446e382acf56970f73252" |
| matrixtable/test_grouped_matrix_table.py | Tests.test_group_rows_by_aggregate | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: unbound lambda variable "da6d5d01bc35439688ebd0d3cc3d5a1f" |
| matrixtable/test_grouped_matrix_table.py | Tests.test_group_cols_by_aggregate | pytest | expression_typing_scope_or_validation | ValueError: invalid expression: unbound lambda variable "5be23f039a0847dca430c3da96b007b2" |
| matrixtable/test_matrix_table.py | Tests.test_row_key_field_show_runs | pytest | missing_expression_or_table_api | AttributeError: show |
| matrixtable/test_matrix_table.py | Tests.test_aggregate_cols_by_init_op | pytest | planning_or_execution | NotImplementedError: matrix-axis aggregate initialization requires native Table support for group-dependent initializers |
| matrixtable/test_matrix_table.py | Tests.test_aggregate_cols_scope_violation | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| matrixtable/test_matrix_table.py | Tests.test_collect_cols_by_key | pytest | expression_typing_scope_or_validation | TypeError: keywords must be strings |
| matrixtable/test_matrix_table.py | Tests.test_read_stored_cols | pytest | format_io_and_fixture_compatibility | FileNotFoundError: No such file or directory (os error 2) |
| matrixtable/test_matrix_table.py | Tests.test_read_stored_rows | pytest | format_io_and_fixture_compatibility | FileNotFoundError: No such file or directory (os error 2) |
| matrixtable/test_matrix_table.py | Tests.test_read_stored_globals | pytest | format_io_and_fixture_compatibility | FileNotFoundError: No such file or directory (os error 2) |
| matrixtable/test_matrix_table.py | Tests.test_codecs_matrix | pytest | missing_expression_or_table_api | AttributeError: type object 'Env' has no attribute 'hail' |
| matrixtable/test_matrix_table.py | Tests.test_codecs_table | pytest | missing_expression_or_table_api | AttributeError: type object 'Env' has no attribute 'hail' |
| matrixtable/test_matrix_table.py | Tests.test_fix3307_read_mt_wrong | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| matrixtable/test_matrix_table.py | Tests.test_rename | pytest | missing_expression_or_table_api | AttributeError: _type. Did you mean: 'dtype'? |
| matrixtable/test_matrix_table.py | Tests.test_range | pytest | missing_expression_or_table_api | AttributeError: add_index |
| matrixtable/test_matrix_table.py | Tests.test_order_by_intervals | pytest | expression_typing_scope_or_validation | TypeError: keywords must be strings |
| matrixtable/test_matrix_table.py | Tests.test_make_table_empty_entry_field | pytest | expression_typing_scope_or_validation | ValueError: field names must be nonempty strings |
| matrixtable/test_matrix_table.py | Tests.test_hardy_weinberg_test | pytest | planning_or_execution | NotImplementedError: matrix list aggregation requires a native per-list hardy_weinberg_test reducer |
| matrixtable/test_matrix_table.py | Tests.test_hw_func_and_agg_agree | pytest | planning_or_execution | NotImplementedError: matrix list aggregation requires a native per-list hardy_weinberg_test reducer |
| matrixtable/test_matrix_table.py | Tests.test_write_stage_locally | pytest | missing_expression_or_table_api | NotImplementedError: matrix codecs and staging are not implemented |
| matrixtable/test_matrix_table.py | Tests.test_row_joins_into_table_2 | pytest | behavior_or_assertion_mismatch | AssertionError: ExpressionException not raised |
| matrixtable/test_matrix_table.py | Tests.test_multi_write | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| matrixtable/test_matrix_table.py | Tests.test_matrix_type_equality | pytest | missing_expression_or_table_api | AttributeError: _type |
| matrixtable/test_matrix_table.py | Tests.test_entry_filter_stats | pytest | expression_typing_scope_or_validation | TypeError: incompatible comparison types: struct{n_filtered: int64, n_remaining: int64, fraction_filtered: float64} and struct{n_filtered: int32, n_remaining: int32, fraction_filtered: float32} |
| matrixtable/test_matrix_table.py | Tests.test_annotate_col_agg_lowering | pytest | planning_or_execution | ValueError: invalid TableIR: row fields must be inside an aggregator in Table.aggregate |
| matrixtable/test_matrix_table.py | Tests.test_partitioned_write | pytest | expression_typing_scope_or_validation | TypeError: MatrixTable.write() got an unexpected keyword argument '_partitions' |
| matrixtable/test_matrix_table.py | Tests.test_partitioned_write_coerce | pytest | expression_typing_scope_or_validation | TypeError: MatrixTable.write() got an unexpected keyword argument '_partitions' |
| matrixtable/test_matrix_table.py | Tests.test_overwrite | pytest | planning_or_execution | FileExistsError: /var/folders/2l/yzgrtp5s1kqgxk5nsw8dc4hw0000gn/T/HLAmZ4KZLZ1GN7LL6xtstU.mt |
| matrixtable/test_matrix_table.py | Tests.test_invalid_metadata | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/0.1-1fd5cc7.vds/_vail_matrix.json' |
| matrixtable/test_matrix_table.py | Tests.test_legacy_files_with_required_globals | pytest | format_io_and_fixture_compatibility | FileNotFoundError: No such file or directory (os error 2) |
| matrixtable/test_matrix_table.py | Tests.test_matrix_multi_write_range | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'experimental' |
| matrixtable/test_matrix_table.py | Tests.test_lower_row_agg_init_arg | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'hwe_normalized_pca' |
| matrixtable/test_matrix_table.py | test_read_write_all_types | pytest | missing_matrix_numerical_api | NotImplementedError: ndarray reshape is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_read | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixRead is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_rows_by_key_with_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateRowsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_rows_by_key_then_aggregate_entries_with_agg_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateRowsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_rows_by_key_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateRowsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_rows_with_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_rows_without_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_choose_cols | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixChooseCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_cols_with_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_cols_with_agg_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_cols_with_scan_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_cols_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_union_cols | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixUnionCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_entries_with_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapEntries is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_entries_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapEntries is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_entries_with_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterEntries is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_entries_without_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterEntries is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_key_rows_by | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixKeyRowsBy is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_rows | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_rows_with_agg_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_rows_with_scan_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_rows_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_globals_with_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapGlobals is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_map_globals_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixMapGlobals is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_cols_with_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_cols_without_cond_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_collect_cols_by_key | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixCollectColsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_cols_by_key_with_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateColsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_cols_by_key_with_agg_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateColsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_aggregate_cols_by_key_without_body_randomness | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAggregateColsByKey is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_explode_rows | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixExplodeRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_repartition | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixRepartition is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_union_rows | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixUnionRows is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_distinct_by_row | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixDistinctByRow is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_rows_head | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixRowsHead is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_cols_head | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixColsHead is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_rows_tail | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixRowsTail is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_cols_tail | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixColsTail is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_explode_cols | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixExplodeCols is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_cast_table_to_matrix | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.CastTableToMatrix is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_annotate_rows_table | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAnnotateRowsTable is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_annotate_cols_table | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixAnnotateColsTable is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_to_matrix_apply | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixToMatrixApply is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_rename | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixRename is not implemented |
| matrixtable/test_matrix_table.py | test_matrix_randomness_filter_intervals | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MatrixFilterIntervals is not implemented |
| matrixtable/test_matrix_table.py | test_struct_of_arrays_encoding | pytest | hail_ir_jvm_or_backend_internals | AttributeError: module 'vail' has no attribute '_with_flags' |
| matrixtable/test_matrix_table_from_parts.py | test_raises_when_no_rows_and_entries | pytest | expression_typing_scope_or_validation | ValueError: provide row and column dimensions through rows, cols, or entries |
| matrixtable/test_matrix_table_from_parts.py | test_raises_when_no_cols_and_entries | pytest | expression_typing_scope_or_validation | ValueError: provide row and column dimensions through rows, cols, or entries |
| methods/relatedness/test_identity_by_descent.py | test_ibd_default_arguments | pytest | harness_or_fixture_setup | AttributeError: module 'vail.utils' has no attribute 'new_local_temp_dir'. Did you mean: 'new_local_temp_file'? |
| methods/relatedness/test_identity_by_descent.py | test_ibd_0_and_1 | pytest | harness_or_fixture_setup | AttributeError: module 'vail.utils' has no attribute 'new_local_temp_dir'. Did you mean: 'new_local_temp_file'? |
| methods/relatedness/test_identity_by_descent.py | test_ibd_does_not_error_with_dummy_maf_float64 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'identity_by_descent' |
| methods/relatedness/test_identity_by_descent.py | test_ibd_does_not_error_with_dummy_maf_float32 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'identity_by_descent' |
| methods/relatedness/test_pc_relate.py | test_pc_relate_against_R_truth | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| methods/relatedness/test_pc_relate.py | test_pc_relate_simple_example | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/relatedness/test_pc_relate.py | test_pc_relate_paths_1 | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| methods/relatedness/test_pc_relate.py | test_pc_relate_paths_2 | isolated_body | missing_expression_or_table_api | MatrixTable caching requires native Table result-cache support |
| methods/relatedness/test_pc_relate.py | test_pc_relate_paths_3 | isolated_body | missing_expression_or_table_api | MatrixTable caching requires native Table result-cache support |
| methods/relatedness/test_pc_relate.py | test_self_kinship_1 | isolated_body | missing_expression_or_table_api | MatrixTable caching requires native Table result-cache support |
| methods/relatedness/test_pc_relate.py | test_self_kinship_2 | isolated_body | missing_expression_or_table_api | MatrixTable caching requires native Table result-cache support |
| methods/relatedness/test_pc_relate.py | test_self_kinship_3 | isolated_body | missing_expression_or_table_api | MatrixTable caching requires native Table result-cache support |
| methods/relatedness/test_pc_relate.py | test_pc_relate_issue_5263 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'pc_relate' |
| methods/test_family_methods.py | Tests.test_trio_matrix_1 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_trio_matrix_2 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_trio_matrix_null_keys | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_trio_matrix_incomplete_trios | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_1 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_2 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_3 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_4 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_5 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_6 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_7 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_8 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_mendel_errors_9 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_tdt | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_family_methods.py | Tests.test_de_novo | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_impex.py | VCFTests.test_info_char | pytest | format_io_and_fixture_compatibility | RuntimeError: Execution error: /Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/infochar.vcf:124: Missing value in INFO array. Use 'hl.import_vcf(..., array_elements_required=False)'. |
| methods/test_impex.py | VCFTests.test_malformed | pytest | planning_or_execution | RuntimeError: Execution error: /Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/malformed.vcf:54: invalid character in numeric literal "rs685723" |
| methods/test_impex.py | VCFTests.test_not_identical_headers | pytest | expression_typing_scope_or_validation | ValueError: invalid sample IDs: VCF headers differ |
| methods/test_impex.py | VCFTests.test_import_vcf | pytest | timeout_or_native_crash | External test timeout after 185.1s (limit 180s) |
| methods/test_impex.py | VCFTests.test_import_vcf_has_good_error_message_when_info_fields_have_missing_elements | pytest | format_io_and_fixture_compatibility | RuntimeError: Execution error: /Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/missingInfoArray.vcf:15: Missing value in INFO array. Use 'hl.import_vcf(..., array_elements_required=False)'. |
| methods/test_impex.py | VCFTests.test_import_vcf_skip_invalid_loci | pytest | planning_or_execution | RuntimeError: Execution error: /Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/skip_invalid_loci.vcf:89: Invalid locus 0:1 for reference GRCh37 |
| methods/test_impex.py | VCFTests.test_export_vcf | pytest | harness_or_fixture_setup | NotImplementedError: vail.context.TemporaryFilename is not implemented |
| methods/test_impex.py | VCFTests.test_export_vcf_quotes_and_backslash_in_description | pytest | harness_or_fixture_setup | NotImplementedError: vail.context.TemporaryFilename is not implemented |
| methods/test_impex.py | VCFTests.test_export_vcf_no_alt_alleles | pytest | behavior_or_assertion_mismatch | AssertionError: False is not true |
| methods/test_impex.py | VCFTests.test_export_sites_only_from_table | pytest | format_io_and_fixture_compatibility | TypeError: export_vcf requires a MatrixTable |
| methods/test_impex.py | VCFTests.test_export_vcf_invalid_info_types | pytest | harness_or_fixture_setup | NotImplementedError: vail.context.TemporaryFilename is not implemented |
| methods/test_impex.py | VCFTests.test_export_vcf_invalid_format_types | pytest | harness_or_fixture_setup | NotImplementedError: vail.context.TemporaryFilename is not implemented |
| methods/test_impex.py | VCFTests.test_export_vcf_haploid | pytest | harness_or_fixture_setup | NotImplementedError: vail.context.TemporaryFilename is not implemented |
| methods/test_impex.py | VCFTests.test_tabix_export | pytest | missing_expression_or_table_api | NotImplementedError: parallel VCF export, tabix, and appended headers are not implemented |
| methods/test_impex.py | VCFTests.test_tabix_export_file_exists | pytest | missing_expression_or_table_api | NotImplementedError: parallel VCF export, tabix, and appended headers are not implemented |
| methods/test_impex.py | VCFTests.test_import_gvcfs | pytest | missing_expression_or_table_api | AttributeError: 'tarray' object has no attribute '_convert_to_json' |
| methods/test_impex.py | VCFTests.test_vcf_parser_golden_master__ex_GRCh37 | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/ex.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | VCFTests.test_vcf_parser_golden_master__gvcf_GRCh37 | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/gvcfs/HG00096.g.vcf.gz.mt/_vail_matrix.json' |
| methods/test_impex.py | VCFTests.test_combiner_works | pytest | missing_high_level_api | ModuleNotFoundError: No module named 'vail.vds' |
| methods/test_impex.py | VCFTests.test_haploid_combiner_ok | pytest | missing_high_level_api | ModuleNotFoundError: No module named 'vail.vds' |
| methods/test_impex.py | VCFTests.test_combiner_parse_allele_specific_annotations | pytest | missing_high_level_api | ModuleNotFoundError: No module named 'vail.vds' |
| methods/test_impex.py | VCFTests.test_same_bgzip | pytest | missing_expression_or_table_api | NotImplementedError: compressed VCF export is not implemented |
| methods/test_impex.py | VCFTests.test_vcf_parallel_separate_header_export | pytest | missing_expression_or_table_api | NotImplementedError: parallel VCF export, tabix, and appended headers are not implemented |
| methods/test_impex.py | VCFTests.test_sorted | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_impex.py | VCFTests.test_empty_read_write | pytest | missing_expression_or_table_api | NotImplementedError: compressed VCF export is not implemented |
| methods/test_impex.py | VCFTests.test_format_genotypes | pytest | behavior_or_assertion_mismatch | AssertionError: assert 'GT:AD:DP:GQ:PL\t1/1:0,6:6:18:234,18,0\t1/1:0,4:4:12:159,12,0\t1/1:0,4:4:12:163,12,0\t1/1:0,12:12:36:479,36,0\t1/1:0,4...1:0,6:6:18:232,18,0\t1/1:0,6:6:18:242,18,0\t1/1:0,3:3:9:119,9,0\t1/1:0,9:9:27:374,27,0\t./.:1,0:1\t1/1:0,3:3:9:133,9,0' in '20\t13029920\trs934335\tC\tG\t18 |
| methods/test_impex.py | VCFTests.test_contigs_header | pytest | behavior_or_assertion_mismatch | AssertionError: expected pattern not found; assert False |
| methods/test_impex.py | VCFTests.test_invalid_info_fields | pytest | harness_or_fixture_setup | ModuleNotFoundError: No module named 'hail' |
| methods/test_impex.py | VCFTests.test_vcf_different_info_errors | pytest | expression_typing_scope_or_validation | ValueError: Check that all files have same INFO fields |
| methods/test_impex.py | PLINKTests.test_import_fam | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | PLINKTests.test_export_import_plink_same | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_import_plink_empty_fam | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_import_plink_empty_bim | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_import_plink_a1_major | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_import_plink_same_locus | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'export_plink' |
| methods/test_impex.py | PLINKTests.test_import_plink_partitions | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'export_plink' |
| methods/test_impex.py | PLINKTests.test_import_plink_contig_recoding_w_reference | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'export_plink' |
| methods/test_impex.py | PLINKTests.test_import_plink_no_reference_specified | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_impex.py | PLINKTests.test_import_plink_and_ignore_rows | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_impex.py | PLINKTests.test_import_plink_skip_invalid_loci | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_impex.py | PLINKTests.test_export_plink | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | PLINKTests.test_export_plink_default_arguments | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_non_default_arguments | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_quantitative_phenotype | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_non_default_bim_arguments | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_call_expression | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_white_space_in_fam_id_raises_error | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_export_plink_white_space_in_varid_raises_error | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_impex.py | PLINKTests.test_contig_recoding_defaults | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_impex.py | PLINKTests.test_export_plink_struct_locus | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'export_plink' |
| methods/test_impex.py | BGENTests.test_error_if_no_gp | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'export_bgen'. Did you mean: 'export_vcf'? |
| methods/test_impex.py | BGENTests.test_import_bgen_dosage_entry | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_GT_GP_entries | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_no_entries | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_no_reference | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_skip_invalid_loci_does_not_error_with_invalid_loci | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_errors_with_invalid_loci | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | BGENTests.test_import_bgen_gavin_example | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | BGENTests.test_import_bgen_random | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | BGENTests.test_parallel_import | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_dosage_and_gp_dosage_function_agree | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_row_fields | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_variant_filtering_from_literals | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_locus_filtering_from_Struct_object | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_locus_filtering_from_struct_expression | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_variant_filtering_from_exprs | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_locus_filtering_from_exprs | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_variant_filtering_from_table | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_locus_filtering_from_table | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_import_bgen_empty_variant_filter | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_n_partitions | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_drop | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_index_multiple_bgen_files_does_not_fail_and_is_importable | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | BGENTests.test_multiple_files_variant_filtering | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_multiple_files_disjoint | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_multiple_references_throws_error | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_old_index_file_throws_error | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | BGENTests.test_specify_different_index_file | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | BGENTests.test_index_bgen_errors_when_index_file_has_wrong_extension | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryFilename' |
| methods/test_impex.py | BGENTests.test_export_bgen | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_export_bgen_zstd | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_export_bgen_parallel | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | BGENTests.test_export_bgen_from_vcf | pytest | harness_or_fixture_setup | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| methods/test_impex.py | BGENTests.test_randomness | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bgen'. Did you mean: 'import_lines'? |
| methods/test_impex.py | GENTests.test_import_gen | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | GENTests.test_import_gen_no_chromosome_in_file | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | GENTests.test_import_gen_no_reference_specified | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | GENTests.test_import_gen_skip_invalid_loci | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | GENTests.test_export_gen | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | GENTests.test_export_gen_exprs | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_import_locus_intervals | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_locus_intervals' |
| methods/test_impex.py | LocusIntervalTests.test_import_locus_intervals_no_reference_specified | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_locus_intervals' |
| methods/test_impex.py | LocusIntervalTests.test_import_locus_intervals_recoding | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_locus_intervals' |
| methods/test_impex.py | LocusIntervalTests.test_import_locus_intervals_badly_defined_intervals | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_locus_intervals' |
| methods/test_impex.py | LocusIntervalTests.test_import_bed | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bed'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_import_bed_recoding | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bed'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_import_bed_no_reference_specified | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bed'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_import_bed_kwargs_to_import_table | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bed'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_import_bed_badly_defined_intervals | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_bed'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | LocusIntervalTests.test_pass_through_args | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_locus_intervals' |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_table_1 | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_table_2 | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_table_3 | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_table_4 | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_table_no_cols | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_import_matrix_comment | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_headers_not_identical | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_headers_same_len_diff_elem | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_too_few_entries | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_wrong_row_field_type | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_wrong_entry_type | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_key_by_after_empty_key_import | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_devilish_nine_separated_eight_missing_file | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | ImportMatrixTableTests.test_empty_import_matrix_table | pytest | missing_expression_or_table_api | AttributeError: export |
| methods/test_impex.py | ImportMatrixTableTests.test_import_row_id_multiple_partitions | pytest | missing_expression_or_table_api | AttributeError: export |
| methods/test_impex.py | ImportMatrixTableTests.test_long_parsing | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_matrix_table'. Did you mean: 'range_matrix_table'? |
| methods/test_impex.py | test_import_matrix_table_round_trip | pytest | missing_expression_or_table_api | AttributeError: export |
| methods/test_impex.py | ImportLinesTest.test_import_lines_bgz | pytest | missing_expression_or_table_api | NotImplementedError: BGZF and gzip text scans are not implemented |
| methods/test_impex.py | ImportTableTests.test_import_table_force_bgz | pytest | missing_expression_or_table_api | NotImplementedError: compressed text IO is not implemented |
| methods/test_impex.py | ImportTableTests.test_import_table_empty | pytest | behavior_or_assertion_mismatch | ValueError: no lines remaining after filters; AssertionError: assert 'Invalid file: no lines remaining after filters\n Files provided: /Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/empty.tsv' in 'no lines remaining after filters' |
| methods/test_impex.py | ImportTableTests.test_type_imputation | pytest | missing_expression_or_table_api | NotImplementedError: Hail type imputation is not implemented; supply types explicitly |
| methods/test_impex.py | ImportTableTests.test_source_file | pytest | missing_expression_or_table_api | AttributeError: add_index |
| methods/test_impex.py | ImportTableTests.test_error_with_context | pytest | missing_expression_or_table_api | NotImplementedError: typed text imports currently support primitive fields |
| methods/test_impex.py | GrepTests.test_grep_show_false | pytest | hail_ir_jvm_or_backend_internals | ModuleNotFoundError: No module named 'vail.backend.service_backend'; 'vail.backend' is not a package |
| methods/test_impex.py | AvroTests.test_simple_avro | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_avro'. Did you mean: 'import_vcf'? |
| methods/test_impex.py | test_import_export_same | pytest | missing_expression_or_table_api | NotImplementedError: compressed VCF export is not implemented |
| methods/test_king.py | test_king_small | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_king.py | test_king_large | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_king.py | test_king_filtered_entries_no_error | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_plink'. Did you mean: 'import_lines'? |
| methods/test_misc.py | Tests.test_rename_duplicates | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'rename_duplicates' |
| methods/test_misc.py | Tests.test_annotate_intervals_bed1 | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_misc.py | Tests.test_annotate_intervals_bed2 | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_misc.py | Tests.test_annotate_intervals_bed3 | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_misc.py | Tests.test_maximal_independent_set | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_maximal_independent_set2 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_maximal_independent_set3 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_maximal_independent_set_types | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_maximal_independent_set_on_floats | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_maximal_independent_set_string_node_names | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'maximal_independent_set' |
| methods/test_misc.py | Tests.test_matrix_filter_intervals | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'parse_locus_interval' |
| methods/test_misc.py | Tests.test_table_filter_intervals | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'parse_locus_interval' |
| methods/test_misc.py | Tests.test_summarize_variants | pytest | expression_typing_scope_or_validation | TypeError: keywords must be strings |
| methods/test_misc.py | Tests.test_verify_biallelic | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'methods' |
| methods/test_misc.py | Tests.test_lambda_gc | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'lambda_gc' |
| methods/test_misc.py | Tests.test_lambda_gc_nans | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'lambda_gc' |
| methods/test_misc.py | Tests.test_segment_intervals | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'segment_intervals'. Did you mean: 'filter_intervals'? |
| methods/test_pca.py | test_hwe_normalized_pca | isolated_body | missing_high_level_api | module 'vail' has no attribute 'hwe_normalized_pca' |
| methods/test_pca.py | test_pca_against_numpy | isolated_body | missing_high_level_api | module 'vail' has no attribute 'pca' |
| methods/test_pca.py | test_blanczos_against_numpy | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'sqrt' |
| methods/test_pca.py | test_blanczos_T | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_blanczos_flags | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_1 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_2 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_3 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_4 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_5 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectral_moments_1 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectral_moments_2 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectral_moments_3 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectral_moments_4 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectral_moments_5 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_and_moments_1 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_and_moments_2 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_and_moments_3 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_and_moments_4 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_pca.py | test_spectra_and_moments_5 | isolated_body | missing_matrix_numerical_api | ndarray indexing is not implemented |
| methods/test_qc.py | Tests.test_sample_qc | isolated_body | missing_high_level_api | module 'vail' has no attribute 'sample_qc' |
| methods/test_qc.py | Tests.test_concordance | isolated_body | format_io_and_fixture_compatibility | [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_qc.py | Tests.test_concordance_n_discordant_1 | isolated_body | format_io_and_fixture_compatibility | [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_qc.py | Tests.test_concordance_n_discordant_2 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'concordance' |
| methods/test_qc.py | Tests.test_concordance_no_values_doesnt_error | isolated_body | format_io_and_fixture_compatibility | [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_qc.py | Tests.test_filter_alleles | isolated_body | missing_high_level_api | module 'vail' has no attribute 'filter_alleles' |
| methods/test_qc.py | Tests.test_filter_alleles_hts_1 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'filter_alleles_hts' |
| methods/test_qc.py | Tests.test_filter_alleles_hts_2 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'filter_alleles_hts' |
| methods/test_qc.py | Tests.test_filter_alleles_hts_3 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'filter_alleles_hts' |
| methods/test_qc.py | Tests.test_filter_alleles_hts_4 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'filter_alleles_hts' |
| methods/test_qc.py | Tests.test_sample_and_variant_qc_call_rate | isolated_body | missing_high_level_api | module 'vail' has no attribute 'sample_qc' |
| methods/test_qc.py | Tests.test_summarize_variants_ti_tv | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'summarize_variants' |
| methods/test_qc.py | Tests.test_charr | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'compute_charr' |
| methods/test_qc.py | Tests.test_vep_grch37_consequence_true | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vep' |
| methods/test_qc.py | Tests.test_vep_grch38_consequence_true | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vep' |
| methods/test_qc.py | Tests.test_vep_grch37_consequence_false | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vep' |
| methods/test_qc.py | Tests.test_vep_grch38_consequence_false | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vep' |
| methods/test_qc.py | Tests.test_vep_grch37_against_dataproc | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vep' |
| methods/test_qc.py | Tests.test_vep_grch38_against_dataproc | isolated_body | missing_expression_or_table_api | compressed text IO is not implemented |
| methods/test_qc.py | Tests.test_vep_grch38_with_large_positions | isolated_body | missing_expression_or_table_api | BGZF and gzip text scans are not implemented |
| methods/test_simulation.py | test_mating_simulation | pytest | format_io_and_fixture_compatibility | FileNotFoundError: [Errno 2] No such file or directory: '/Users/danielking/.codex/worktrees/1611/vail/hail/hail/hail/test/resources/split-multi-sample.vcf.mt/_vail_matrix.json' |
| methods/test_skat.py | test_skat_negative_weights_errors | isolated_body | missing_high_level_api | module 'vail' has no attribute '_linear_skat' |
| methods/test_skat.py | test_logistic_skat_phenotypes_are_binary | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_skat.py | test_logistic_skat_no_weights_R_truth | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_skat.py | test_logistic_skat_R_truth | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_skat.py | test_logistic_skat_on_big_matrix | isolated_body | format_io_and_fixture_compatibility | module 'vail' has no attribute 'import_matrix_table' |
| methods/test_skat.py | test_linear_skat_no_weights_R_truth | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_skat.py | test_linear_skat_R_truth | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'unphased_diploid_gt_index_call' |
| methods/test_skat.py | test_linear_skat_on_big_matrix | isolated_body | format_io_and_fixture_compatibility | module 'vail' has no attribute 'import_matrix_table' |
| methods/test_skat.py | test_skat_1 | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_skat_2 | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_skat_3 | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_skat_4 | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_skat_5 | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_linear_skat_produces_same_results_as_old_scala_method | isolated_body | missing_expression_or_table_api | Hail type imputation is not implemented; supply types explicitly |
| methods/test_skat.py | test_skat_max_iteration_fails_explodes_in_37_steps | isolated_body | missing_high_level_api | module 'vail' has no attribute 'skat' |
| methods/test_skat.py | test_skat_max_iterations_fails_to_converge_in_fewer_than_36_steps | isolated_body | missing_high_level_api | module 'vail' has no attribute 'skat' |
| methods/test_statgen.py | Tests.test_impute_sex_same_as_plink | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'impute_sex' |
| methods/test_statgen.py | Tests.test_linreg_basic | pytest | missing_expression_or_table_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| methods/test_statgen.py | Tests.test_linreg_pass_through | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.linear_regression_rows |
| methods/test_statgen.py | Tests.test_linreg_chained | pytest | missing_expression_or_table_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| methods/test_statgen.py | Tests.test_linear_regression_without_intercept | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.linear_regression_rows |
| methods/test_statgen.py | Tests.test_linear_regression_with_cov | pytest | missing_expression_or_table_api | AttributeError: values |
| methods/test_statgen.py | Tests.test_linear_regression_pl | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pl_dosage' |
| methods/test_statgen.py | Tests.test_linear_regression_with_dosage | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | Tests.test_linear_regression_equivalence_between_ds_and_gt | pytest | format_io_and_fixture_compatibility | FileNotFoundError: No such file or directory (os error 2) |
| methods/test_statgen.py | Tests.test_linear_regression_with_import_fam_boolean | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | Tests.test_linear_regression_with_import_fam_quant | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | Tests.test_linear_regression_multi_pheno_same | pytest | missing_expression_or_table_api | AttributeError: values |
| methods/test_statgen.py | Tests.test_logistic_regression_rows_max_iter_zero | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows'; assert 'Failed to fit logistic regression null model (standard MLE with covariates only): Newton iteration failed to converge' in "module 'vail' has no attribute 'logistic_regression_rows'" |
| methods/test_statgen.py | Tests.test_logistic_regression_rows_max_iter_explodes | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_firth_logistic_regression_rows_explodes_in_12_steps | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows' |
| methods/test_statgen.py | Tests.test_firth_logistic_regression_rows_does_not_converge_with_105_iterations | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows' |
| methods/test_statgen.py | Tests.test_firth_logistic_regression_rows_does_converge_with_more_iterations | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows' |
| methods/test_statgen.py | Tests.test_weighted_linear_regression | pytest | missing_expression_or_table_api | AttributeError: values |
| methods/test_statgen.py | Tests.test_weighted_linear_regression__missing_weights_are_excluded | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute '_linear_regression_rows_nd' |
| methods/test_statgen.py | Tests.test_errors_weighted_linear_regression | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute '_linear_regression_rows_nd' |
| methods/test_statgen.py | Tests.test_logistic_regression_wald_test | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_logistic_regression_wald_test_apply_multi_pheno | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_logistic_regression_wald_test_multi_pheno_bgen_dosage | pytest | missing_expression_or_table_api | AttributeError: cache |
| methods/test_statgen.py | Tests.test_logistic_regression_wald_test_pl | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'pl_dosage' |
| methods/test_statgen.py | Tests.test_logistic_regression_wald_dosage | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_gen'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | Tests.test_logistic_regression_lrt | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_logistic_regression_score | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_logreg_pass_through | pytest | missing_high_level_api | NotImplementedError: Vail backend does not support hl.logistic_regression_rows |
| methods/test_statgen.py | Tests.test_poission_regression_wald_test | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'poisson_regression_rows' |
| methods/test_statgen.py | Tests.test_poisson_regression_max_iterations | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'poisson_regression_rows' |
| methods/test_statgen.py | Tests.test_poisson_regression_lrt | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'poisson_regression_rows' |
| methods/test_statgen.py | Tests.test_poisson_regression_score_test | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'poisson_regression_rows' |
| methods/test_statgen.py | Tests.test_poisson_pass_through | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'poisson_regression_rows' |
| methods/test_statgen.py | Tests.test_genetic_relatedness_matrix | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| methods/test_statgen.py | Tests.test_realized_relationship_matrix | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| methods/test_statgen.py | Tests.test_row_correlation_vs_hardcode | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'row_correlation' |
| methods/test_statgen.py | Tests.test_row_correlation_vs_numpy | pytest | missing_matrix_numerical_api | AttributeError: type object 'BlockMatrix' has no attribute 'from_entry_expr' |
| methods/test_statgen.py | Tests.test_ld_matrix_1 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_matrix' |
| methods/test_statgen.py | Tests.test_ld_matrix_2 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_matrix' |
| methods/test_statgen.py | Tests.test_ld_matrix_3 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_matrix' |
| methods/test_statgen.py | Tests.test_ld_matrix_4 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_matrix' |
| methods/test_statgen.py | Tests.test_split_multi_hts | pytest | timeout_or_native_crash | External test timeout after 185.2s (limit 180s) |
| methods/test_statgen.py | Tests.test_split_multi_table | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'split_multi'. Did you mean: 'split_multi_hts'? |
| methods/test_statgen.py | Tests.test_split_multi_shuffle | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'split_multi'. Did you mean: 'split_multi_hts'? |
| methods/test_statgen.py | Tests.test_issue_4527 | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'split_multi'. Did you mean: 'split_multi_hts'? |
| methods/test_statgen.py | Tests.test_ld_prune | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_inputs | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_no_prune | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_identical_variants | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_maf | pytest | missing_expression_or_table_api | NotImplementedError: MatrixTable caching requires native Table result-cache support |
| methods/test_statgen.py | Tests.test_ld_prune_call_expression | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_missing_entries | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_ld_prune_with_duplicate_row_keys | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'ld_prune' |
| methods/test_statgen.py | Tests.test_balding_nichols_model_phased | pytest | behavior_or_assertion_mismatch | AssertionError: Lists differ: [[1, 0], [1, 0], [0, 0], [1, 1], [1, 1], [1[152 chars], 1]] != [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [1[152 chars], 1]]; E   First differing element 0:; [1, 0]; [0, 0]; E   - [[1, 0],; ?   ^; E   + [[0, 0],; ?   ^; E   -  [1, 0],; ?   ^; E   +  [0, 0],; ?   ^; E   +  |
| methods/test_statgen.py | Tests.test_de_novo | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_statgen.py | Tests.test_de_novo_error | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_statgen.py | Tests.test_de_novo_ignore_computed_af_runs | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'Pedigree' |
| methods/test_statgen.py | Tests.test_warn_if_no_intercept | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'methods' |
| methods/test_statgen.py | Tests.test_regression_field_dependence | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows' |
| methods/test_statgen.py | test_logistic_regression_epacts_wald | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | test_logistic_regression_epacts_lrt | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | test_logistic_regression_epacts_score | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | test_logistic_regression_epacts_firth | pytest | format_io_and_fixture_compatibility | AttributeError: module 'vail' has no attribute 'import_fam'. Did you mean: 'import_vcf'? |
| methods/test_statgen.py | test_logistic_regression_y_parameter_sanity | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'logistic_regression_rows' |
| methods/test_statgen.py | test_gq_from_pl_missing | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'gq_from_pl' |
| methods/test_statgen.py | test_gq_from_pl_all_missing | pytest | missing_expression_or_table_api | AttributeError: module 'vail' has no attribute 'gq_from_pl' |
| plot/test_plot.py | test_cdf | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_histogram | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_pdf | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_smoothed_pdf | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_cumulative_histogram | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_histogram2d | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_scatter | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_joint_plot | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_qq | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_manhattan | isolated_body | missing_expression_or_table_api | add_index |
| plot/test_plot.py | test_plots_deprecated_collect_all | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| plot/test_plot.py | test_visualize_missingness | isolated_body | missing_high_level_api | module 'vail' has no attribute 'plot' |
| table/test_table.py | Tests.test_group_aggregate_by_key | isolated_body | missing_expression_or_table_api | 'GroupedTable' object has no attribute '_set_buffer_size' |
| table/test_table.py | Tests.test_errors | isolated_body | expression_typing_scope_or_validation | Table is immutable |
| table/test_table.py | Tests.test_interval_product_join_long_key | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| table/test_table.py | Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245 | isolated_body | timeout_or_native_crash | worker exceeded 180 seconds |
| table/test_table.py | Tests.test_index_keyless_table | isolated_body | expression_typing_scope_or_validation | Cannot index an unkeyed table |
| table/test_table.py | Tests.test_from_spark_works | isolated_body | missing_expression_or_table_api | type object 'Env' has no attribute 'spark_session' |
| table/test_table.py | Tests.test_table_parallelize_partial_infer_types | isolated_body | missing_expression_or_table_api | partial_type is not implemented in this initial slice |
| table/test_table.py | Tests.test_rename | isolated_body | missing_expression_or_table_api | _indices |
| table/test_table.py | Tests.test_export | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_open' |
| table/test_table.py | Tests.test_export_delim | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_open' |
| table/test_table.py | Tests.test_export_parallel_manifest | isolated_body | missing_expression_or_table_api | types files and parallel text export are not implemented |
| table/test_table.py | Tests.test_write_stage_locally | isolated_body | missing_expression_or_table_api | staging and custom codecs are not supported |
| table/test_table.py | Tests.test_read_back_same_as_exported | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| table/test_table.py | Tests.test_indexed_read_1 | isolated_body | format_io_and_fixture_compatibility | No such file or directory (os error 2) |
| table/test_table.py | Tests.test_indexed_read_2 | isolated_body | format_io_and_fixture_compatibility | No such file or directory (os error 2) |
| table/test_table.py | Tests.test_indexed_read_3 | isolated_body | format_io_and_fixture_compatibility | No such file or directory (os error 2) |
| table/test_table.py | Tests.test_indexed_read_4 | isolated_body | format_io_and_fixture_compatibility | No such file or directory (os error 2) |
| table/test_table.py | Tests.test_expand_types_on_all_types | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| table/test_table.py | Tests.test_rvd_key_write | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| table/test_table.py | Tests.test_show__various_types | isolated_body | expression_typing_scope_or_validation | keywords must be strings |
| table/test_table.py | Tests.test_path_collision_error | isolated_body | planning_or_execution | table output already exists |
| table/test_table.py | test_large_number_of_fields | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| table/test_table.py | test_maybe_flexindex_table_by_expr_direct_match | isolated_body | missing_expression_or_table_api | _maybe_flexindex_table_by_expr |
| table/test_table.py | test_maybe_flexindex_table_by_expr_prefix_match | isolated_body | missing_expression_or_table_api | _maybe_flexindex_table_by_expr |
| table/test_table.py | test_maybe_flexindex_table_by_expr_direct_interval_match | isolated_body | missing_expression_or_table_api | _maybe_flexindex_table_by_expr |
| table/test_table.py | test_maybe_flexindex_table_by_expr_prefix_interval_match | isolated_body | missing_expression_or_table_api | _maybe_flexindex_table_by_expr |
| table/test_table.py | test_group_within_partitions_after_import_vcf | isolated_body | timeout_or_native_crash | called `Result::unwrap()` on an `Err` value: InvalidArgumentError("use StructArray::try_new_with_length or StructArray::new_empty_fields to create a struct array with no fields so that the length can be set correctly") |
| table/test_table.py | test_read_write_all_types | isolated_body | missing_matrix_numerical_api | ndarray reshape is not implemented |
| table/test_table.py | test_map_partitions_errors | isolated_body | behavior_or_assertion_mismatch | Regex pattern did not match.   Expected regex: 'expected return type expression of type stream<struct>'   Actual message: 'Table._map_partitions callback must return a stream expression' |
| table/test_table.py | test_lowered_persist | isolated_body | missing_expression_or_table_api | persist |
| table/test_table.py | test_table_head_and_tail | isolated_body | missing_expression_or_table_api | cache |
| table/test_table.py | test_literal_of_pandas_NA_and_numpy_int64 | isolated_body | expression_typing_scope_or_validation | cannot infer a Vail type for NAType |
| table/test_table.py | test_literal_of_pandas_NA_and_numpy_int32 | isolated_body | expression_typing_scope_or_validation | cannot infer a Vail type for NAType |
| table/test_table.py | test_write_many | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| table/test_table.py | test_indexed_read_boundaries | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute '_with_flags' |
| table/test_table.py | test_table_randomness_range_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableRange |
| table/test_table.py | test_table_randomness_range_matrix_rows_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.MatrixRowsTable |
| table/test_table.py | test_table_randomness_join | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableJoin |
| table/test_table.py | test_table_randomness_left_join_right_distinct | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableLeftJoinRightDistinct |
| table/test_table.py | test_table_randomness_interval_join | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableIntervalJoin |
| table/test_table.py | test_table_randomness_union | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableUnion |
| table/test_table.py | test_table_randomness_map_globals_with_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapGlobals |
| table/test_table.py | test_table_randomness_map_globals_without_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapGlobals |
| table/test_table.py | test_table_randomness_explode | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableExplode |
| table/test_table.py | test_table_randomness_key_by | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableKeyBy |
| table/test_table.py | test_table_randomness_map_rows_with_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapRows |
| table/test_table.py | test_table_randomness_map_rows_with_scan_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapRows |
| table/test_table.py | test_table_randomness_map_rows_without_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapRows |
| table/test_table.py | test_table_randomness_map_partitions | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMapPartitions |
| table/test_table.py | test_table_randomness_read | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableRead |
| table/test_table.py | test_table_randomness_matrix_entries_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.MatrixEntriesTable |
| table/test_table.py | test_table_randomness_filter_with_cond_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableFilter |
| table/test_table.py | test_table_randomness_filter_without_cond_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableFilter |
| table/test_table.py | test_table_randomness_key_by_and_aggregate_with_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableKeyByAndAggregate |
| table/test_table.py | test_table_randomness_key_by_and_aggregate_with_agg_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableKeyByAndAggregate |
| table/test_table.py | test_table_randomness_key_by_and_aggregate_without_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableKeyByAndAggregate |
| table/test_table.py | test_table_randomness_aggregate_by_key | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableAggregateByKey |
| table/test_table.py | test_table_randomness_matrix_cols_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.MatrixColsTable |
| table/test_table.py | test_table_randomness_parallelize_with_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableParallelize |
| table/test_table.py | test_table_randomness_parallelize_without_body_randomness | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableParallelize |
| table/test_table.py | test_table_randomness_head | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableHead |
| table/test_table.py | test_table_randomness_tail | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableTail |
| table/test_table.py | test_table_randomness_order_by | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableOrderBy |
| table/test_table.py | test_table_randomness_distinct | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableDistinct |
| table/test_table.py | test_table_randomness_repartition | isolated_body | missing_expression_or_table_api | _tir |
| table/test_table.py | test_table_randomness_cast_matrix_to_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.CastMatrixToTable |
| table/test_table.py | test_table_randomness_rename | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableRename |
| table/test_table.py | test_table_randomness_multi_way_zip_join | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableMultiWayZipJoin |
| table/test_table.py | test_table_randomness_filter_intervals | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableFilterIntervals |
| table/test_table.py | test_table_randomness_block_matrix_to_table | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.BlockMatrixToTable |
| table/test_table.py | test_table_randomness_table_gen | isolated_body | hail_ir_jvm_or_backend_internals | Hail Scala/backend dependency: ir.TableGen |
| test_call_caching.py | test_execution_cache_creation | pytest | missing_expression_or_table_api | AttributeError: 'LocalFilesystem' object has no attribute 'exists' |
| test_context.py | test_init_hail_context_twice | isolated_body | missing_high_level_api | module 'vail' has no attribute 'experimental' |
| test_context.py | test_top_level_functions_are_do_not_error | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'debug_info' |
| test_context.py | test_tmpdir_runs | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'tmp_dir' |
| test_context.py | test_get_flags | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute '_get_flags' |
| test_context.py | test_flags_same_in_scala_and_python | isolated_body | missing_expression_or_table_api | type object 'Env' has no attribute 'hail' |
| test_context.py | test_fast_restarts_feature | isolated_body | hail_ir_jvm_or_backend_internals | module 'vail' has no attribute '_get_flags' |
| test_exceptions_from_workers_have_stack_traces.py | test_exceptions_from_workers_have_stack_traces | pytest | planning_or_execution | RuntimeError: Execution error: Arrow expression failed: Compute error: integer division or modulo by zero |
| test_hail_in_notebook.py | test_hail_in_notebook | isolated_body | harness_or_fixture_setup | name '__file__' is not defined |
| test_ir.py | test_ir_value_type | pytest | hail_ir_jvm_or_backend_internals | AttributeError: module 'vail' has no attribute 'tstream'. Did you mean: 'streams'? |
| test_ir.py | test_ir_copies | pytest | hail_ir_jvm_or_backend_internals | AttributeError: module 'vail' has no attribute 'tstream'. Did you mean: 'streams'? |
| test_ir.py | test_table_ir_table_type | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.TrueIR is not implemented |
| test_ir.py | test_table_ir_highly_nested | pytest | hail_ir_jvm_or_backend_internals | AttributeError: _tir |
| test_ir.py | test_matrix_ir_matrix_type | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.MakeStruct is not implemented |
| test_ir.py | test_blockmatrix_ir_parses | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.F64 is not implemented |
| test_ir.py | test_blockmatrix_ir_persist | pytest | hail_ir_jvm_or_backend_internals | AttributeError: type object 'Env' has no attribute 'backend' |
| test_ir.py | test_blockmatrix_ir_blockmatrix_type | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.F64 is not implemented |
| test_ir.py | ValueTests.test_value_same_after_parsing | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.Literal is not implemented |
| test_ir.py | CSETests.test_cse | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.I32 is not implemented |
| test_ir.py | CSETests.test_cse_debug | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: ndarray indexing is not implemented |
| test_ir.py | CSETests.test_cse_complex_lifting | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.I32 is not implemented |
| test_ir.py | CSETests.test_stream_cse | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.StreamRange is not implemented |
| test_ir.py | CSETests.test_cse2 | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.I32 is not implemented |
| test_ir.py | CSETests.test_cse_ifs | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.I32 is not implemented |
| test_ir.py | CSETests.test_shadowing | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.ApplyBinaryPrimOp is not implemented |
| test_ir.py | CSETests.test_agg_cse | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.TableRange is not implemented |
| test_ir.py | CSETests.test_init_op | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.I32 is not implemented |
| test_ir.py | CSETests.test_agg_let | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.ApplyAggOp is not implemented |
| test_ir.py | CSETests.test_refs | pytest | hail_ir_jvm_or_backend_internals | NotImplementedError: vail.ir.TableRange is not implemented |
| test_ir.py | test_literal_encodings | pytest | hail_ir_jvm_or_backend_internals | AttributeError: '_PrimitiveType' object has no attribute '_from_encoding' |
| test_ir.py | test_literal_ndarray_encodings | pytest | hail_ir_jvm_or_backend_internals | AttributeError: 'tndarray' object has no attribute '_from_encoding' |
| test_ir.py | test_locus_interval_encoding | pytest | hail_ir_jvm_or_backend_internals | AttributeError: 'tlocus' object has no attribute '_from_encoding' |
| typecheck/test_typecheck.py | Tests.test_varargs | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_helpers | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_nested | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_class_methods | isolated_body | missing_expression_or_table_api | name 'typecheck_method' is not defined |
| typecheck/test_typecheck.py | Tests.test_lazy | isolated_body | missing_expression_or_table_api | name 'lazy' is not defined |
| typecheck/test_typecheck.py | Tests.test_coercion | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_function_checker | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_complex_signature | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| typecheck/test_typecheck.py | Tests.test_extra_args | isolated_body | missing_expression_or_table_api | name 'typecheck' is not defined |
| utils/test_deduplicate.py | test_deduplicate_simple | isolated_body | missing_expression_or_table_api | name 'deduplicate' is not defined |
| utils/test_deduplicate.py | test_deduplicate_max_attempts | isolated_body | missing_expression_or_table_api | name 'deduplicate' is not defined |
| utils/test_deduplicate.py | test_deduplicate_already_used | isolated_body | missing_expression_or_table_api | name 'deduplicate' is not defined |
| utils/test_genomic_range_table.py | test_genomic_range_table_grch38 | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'genomic_range_table' |
| utils/test_genomic_range_table.py | test_genomic_range_table_grch37 | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'genomic_range_table' |
| utils/test_genomic_range_table.py | test_genomic_range_table_canfam3 | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'genomic_range_table' |
| utils/test_genomic_range_table.py | test_genomic_range_table_grcm38 | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'genomic_range_table' |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_methods_1 | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_methods_2 | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_methods_3 | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_read_overwrite | isolated_body | hail_ir_jvm_or_backend_internals | name 'fs' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_exists | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_is_file | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_hadoop_stat | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_subdirs | isolated_body | hail_ir_jvm_or_backend_internals | name 'fs' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_rmtree_empty_is_ok | isolated_body | hail_ir_jvm_or_backend_internals | name 'fs' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_rmtree_empty_subdir_is_ok | isolated_body | hail_ir_jvm_or_backend_internals | name 'fs' is not defined |
| utils/test_hl_hadoop_and_hail_fs.py | test_remove_and_rmtree | isolated_body | hail_ir_jvm_or_backend_internals | name 'fs' is not defined |
| utils/test_pickle.py | test_simple_struct_pickle | pytest | planning_or_execution | RecursionError: maximum recursion depth exceeded |
| utils/test_pickle.py | test_simple_struct_dill | pytest | planning_or_execution | RecursionError: maximum recursion depth exceeded |
| utils/test_pickle.py | test_frozendict_pickle | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'frozendict' |
| utils/test_pickle.py | test_frozendict_dill | pytest | missing_expression_or_table_api | AttributeError: module 'vail.utils' has no attribute 'frozendict' |
| utils/test_pickle.py | test_locus_pickle | pytest | expression_typing_scope_or_validation | TypeError: cannot pickle 'mappingproxy' object |
| utils/test_placement_tree.py | Tests.test_realistic | isolated_body | missing_expression_or_table_api | name 'PlacementTree' is not defined |
| utils/test_utils.py | Tests.test_hadoop_methods | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_utils.py | Tests.test_hadoop_exists | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_exists' |
| utils/test_utils.py | Tests.test_hadoop_mkdir_p | isolated_body | missing_expression_or_table_api | name 'hadoop_open' is not defined |
| utils/test_utils.py | Tests.test_hadoop_mkdir_p_2 | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_exists' |
| utils/test_utils.py | Tests.test_hadoop_copy_log | isolated_body | missing_expression_or_table_api | name 'with_local_temp_file' is not defined |
| utils/test_utils.py | Tests.test_hadoop_is_file | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_is_file' |
| utils/test_utils.py | Tests.test_hadoop_is_dir | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_is_dir' |
| utils/test_utils.py | Tests.test_hadoop_stat | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_stat' |
| utils/test_utils.py | Tests.test_hadoop_no_glob_in_bucket | isolated_body | expression_typing_scope_or_validation | 'HAIL_TEST_STORAGE_URI' |
| utils/test_utils.py | Tests.test_hadoop_ls_simple | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| utils/test_utils.py | Tests.test_hadoop_ls | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_ls' |
| utils/test_utils.py | Tests.test_hadoop_ls_file_that_does_not_exist | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_ls' |
| utils/test_utils.py | Tests.test_hadoop_glob_heterogenous_structure | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| utils/test_utils.py | Tests.test_hadoop_ls_glob_no_slash_in_group | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_ls' |
| utils/test_utils.py | Tests.test_hadoop_ls_glob_1 | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_stat' |
| utils/test_utils.py | Tests.test_hadoop_ls_glob_2 | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_stat' |
| utils/test_utils.py | Tests.test_linked_list | isolated_body | missing_expression_or_table_api | name 'LinkedList' is not defined |
| utils/test_utils.py | Tests.test_struct_ops | isolated_body | missing_expression_or_table_api | drop |
| utils/test_utils.py | Tests.test_expr_exception_results_in_hail_user_error | isolated_body | expression_typing_scope_or_validation | Execution error: invalid expression: array index out of bounds: index=5, length=2 |
| utils/test_utils.py | Tests.test_escape_string | isolated_body | missing_expression_or_table_api | name 'escape_str' is not defined |
| utils/test_utils.py | Tests.test_escape_id | isolated_body | missing_expression_or_table_api | name 'escape_id' is not defined |
| utils/test_utils.py | Tests.test_frozen_dict | isolated_body | missing_expression_or_table_api | name 'frozendict' is not defined |
| utils/test_utils.py | Tests.test_json_encoder | isolated_body | missing_expression_or_table_api | name 'frozendict' is not defined |
| utils/test_utils.py | test_hadoop_ls_folder_glob | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_prefix_folder_glob_qmarks | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_two_folder_globs | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_two_folder_globs_and_two_qmarks | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_one_folder_glob_and_qmarks_in_multiple_components | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_groups | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_size_one_groups | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'hadoop_ls' |
| utils/test_utils.py | test_hadoop_ls_component_with_only_groups | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_hadoop_ls_negated_group | isolated_body | expression_typing_scope_or_validation | unsupported operand type(s) for +: 'generator' and 'str' |
| utils/test_utils.py | test_struct_rich_comparison | isolated_body | expression_typing_scope_or_validation | Table expressions cannot be converted to Python bool |
| vds/test_combiner.py | test_combiner_works | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| vds/test_combiner.py | test_combiner_plan_round_trip_serialization | isolated_body | missing_high_level_api | name 'new_combiner' is not defined |
| vds/test_combiner.py | test_reload_combiner_plan | isolated_body | missing_high_level_api | name 'new_combiner' is not defined |
| vds/test_combiner.py | test_move_load_combiner_plan | isolated_body | missing_high_level_api | name 'new_combiner' is not defined |
| vds/test_combiner.py | test_combiner_run | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'parse_locus_interval' |
| vds/test_combiner.py | test_combiner_manual_filtration | isolated_body | missing_high_level_api | name 'new_combiner' is not defined |
| vds/test_combiner.py | test_ref_block_max_len_propagates_in_combiner | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| vds/test_combiner.py | test_custom_call_fields | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| vds/test_combiner.py | test_missing_ref_gt_does_not_drop_other_fields | isolated_body | missing_high_level_api | name 'make_ref_entry_struct' is not defined |
| vds/test_combiner.py | test_save_filters_saves_filters | isolated_body | harness_or_fixture_setup | module 'vail' has no attribute 'TemporaryDirectory' |
| vds/test_vds.py | test_validate | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_multi_write | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_sampleqc_old_new_equivalence | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_sampleqc_gq_dp | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_sampleqc_singleton_r_ti_tv | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_samples_and_merge | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_samples_array | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_segment_intervals | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_interval_coverage | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_impute_sex_chr_ploidy_from_interval_coverage | isolated_body | missing_expression_or_table_api | module 'vail' has no attribute 'parse_locus_interval' |
| vds/test_vds.py | test_impute_sex_chromosome_ploidy_1 | isolated_body | planning_or_execution | Error during planning: Failed to coerce arguments to satisfy a call to 'take__vail_031690860f8901c9' function: coercion from Struct("locus": Struct("__vail_locus_ordinal": Int32, metadata: {"vail.type": "{\"locus\":{\"name\":\"GRCh37\",\"contigs\":[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9 |
| vds/test_vds.py | test_impute_sex_chromosome_ploidy_2 | isolated_body | planning_or_execution | Error during planning: Failed to coerce arguments to satisfy a call to 'take__vail_414e4d1880893de7' function: coercion from Struct("locus": Struct("__vail_locus_ordinal": Int32, metadata: {"vail.type": "{\"locus\":{\"name\":\"GRCh37\",\"contigs\":[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9 |
| vds/test_vds.py | test_filter_intervals_segment | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_intervals_segment_table | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_intervals_default | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_intervals_default_table | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_chromosomes | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_to_dense_mt | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_read_dense_mt_and_to_dense_mt_equivalence | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_to_dense_mt_haploid | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_merge_reference_blocks | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_truncate_reference_blocks | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_union_rows1 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_union_rows2 | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_combiner_max_len | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_split_sparse_roundtrip | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_split_alters_ref_gt | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_ref_block_max_len_patch | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_filter_intervals_table | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_ref_block_does_not_densify_to_next_contig | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_haploid_lpl_import | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds.py | test_basic_impex | isolated_body | missing_high_level_api | module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_lgt_to_gt | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_lgt_to_gt_invalid | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_local_to_global | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_local_to_global_alleles_non_increasing | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_local_to_global_missing_fill | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
| vds/test_vds_functions.py | test_local_to_global_out_of_bounds | pytest | missing_high_level_api | AttributeError: module 'vail' has no attribute 'vds' |
