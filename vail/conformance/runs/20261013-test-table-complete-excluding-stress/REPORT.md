# Hail `table/test_table.py` results

Run date: 2026-10-13. Only the Hail `table/test_table.py` module was run against Vail using the isolated Python 3.11 test environment. Tests under `backend` were not included. Pytest collected 394 nodes. One Hail stress case, `Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245`, was deselected after it remained inside its gVCF import / 100-partition multi-way join / write call beyond Hail's own three-minute test budget in two attempts. The remaining 393 nodes all completed: 270 passed, 122 failed, and 1 skipped.

## Failure categories

- **79 missing API or attribute failures.** Common gaps include random functions (`rand_bool`, `rand_unif`, `rand_int64`), `GroupedTable._set_buffer_size`, `Table._generate`, `Env.spark_session`, `TemporaryDirectory`, `hadoop_open`, `_with_flags`, ndarray `reshape`, and array aggregation methods. Forty parameterized `test_table_head_and_tail` variants fail because the test's `rtcache` parameter expects a missing expression `cache` feature.
- **25 Python IR instrumentation failures.** Randomness tests that inspect the Hail IR cannot proceed because the corresponding `vail.ir.Table*` node classes are placeholders (for example `TableRange`, `TableJoin`, `TableMapRows`, `TableDistinct`, and `TableMultiWayZipJoin`). These test randomness behavior but are blocked by missing Python-side IR, not by evidence of incorrect random values.
- **13 missing random-function exports.** These are randomness tests blocked before evaluating the operation because functions such as `rand_int64` are not exported.
- **6 explicitly unsupported behaviors.** Includes types-file/parallel export, Java-compatible regex replacement, Hail UTF-16 string behavior, partial type inference, write staging/custom codecs, and one randomness path that needs a backend lowering hook.
- **4 indexed-read fixture/path errors.** The expected persisted table fixtures are unavailable in the adapted test environment.
- **3 type/validation mismatches.** Includes immutable-table assignment raising `TypeError` where Hail expects `NotImplementedError`, and inability to infer a type for pandas `NA`.
- **1 exception type mismatch.** Indexing an unkeyed table raises `ValueError` instead of the expected expression-level exception.
- **1 write-path mismatch.** Existing output path behavior differs from the expected collision exception.
- **1 native empty-struct panic.** `test_group_within_partitions_after_import_vcf` reaches a PyArrow empty-struct construction panic.
- **1 assertion/message mismatch.** `_map_partitions` raises the right general error but its message differs from Hail's expected text.
- **1 strict XPASS.** `test_can_process_wide_tables[3072]` unexpectedly succeeds where the upstream test marks this case as expected to fail.

The one skipped test is a supported skip in the upstream test logic. Pytest reports four warnings. The detailed per-test event stream and raw pytest output are in this directory.
