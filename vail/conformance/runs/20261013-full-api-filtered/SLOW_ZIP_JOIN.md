# Unbalanced multi-way zip-join timeout

Upstream test: `table/test_table.py::Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245`.

The test imports `gvcfs/HG00096.g.vcf.gz` with 100 partitions and `gvcfs/HG00268.g.vcf.gz` with 1 partition, extracts rows, and writes their multi-way zip join. No test completion event arrived for **177.96 seconds**; the declared per-test limit was 120 seconds. Python SIGALRM did not regain control inside native execution. The isolated module was terminated, and later definitions were tested separately.

The leading explanation is replay amplification, based on source inspection rather than a captured profile:

- `join_interval::prepare_zip` constructs an independently executable join for every boundary point/open key interval.
- `clipped_parts` resets each selected input and applies a DataFusion filter above it. A large input partition spanning many intervals can therefore be evaluated repeatedly.
- Inputs without inherited certified bounds are scanned before join execution to determine their first/last keys.
- VCF `min_partitions` currently means repartitioning an explicitly sorted scan. Replaying this input can redo VCF parsing, sorting, and redistribution, not only filtering an already decoded batch.

This code path establishes that repeated work is possible; it does **not** measure how many repeats occurred or establish which step dominated this timeout. A follow-up should separately time metadata planning and execution and profile source reads, parsing, sort, and repartition before changing the implementation. The long-term fix already contemplated for joins is to push the chosen output intervals back to source readers so they scan slices rather than whole overlapping input partitions.

Source anchors: `crates/vail-table/src/join_interval.rs` (`prepare_zip`, `clipped_parts`, `bounds`, `scan_bounds`); `python/vail/vcf.py` (`import_vcf`); `crates/vail-table/src/partitioned.rs` (`prepare_legacy`).
