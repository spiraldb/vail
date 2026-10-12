# Vail storage/order review against `d87680c`

Scope reviewed: storage snapshot/index/read paths; table provider, interval reads, joins/lookup, partition execution, key ordering, and Python type codecs. Read-only inspection; no builds or test suites run. I included tracked changes and untracked table modules.

## Findings

### [P2] Indexed DataFusion partitions reopen every candidate file

**Verified from code; execution trigger is an indexed filter on a single-key `Read` table.** `ReadKeyFilteredExec::execute(partition)` computes that partition's original row range (`provider.rs:420-432`), but then creates `source.interval_batches(&queries)` for the entire query at `provider.rs:433-437`. That storage API selects and reads every file matching the query (`local_table.rs:662-674`), after which the provider drops rows outside the requested partition (`provider.rs:440-452`). The newly added `LocalRead::partition_batches(partition)` already routes through `stream_snapshot_rows(start..end)` and opens only files overlapping that partition (`local_table.rs:620-659`), but this indexed provider path bypasses it.

Consequences: for a broad key predicate, every independently executed output partition decodes the same candidate files, multiplying I/O by the partition count. A partition can also fail because a candidate file belonging only to another partition is unreadable or missing, so the provider partition is not independently readable even though its requested row range does not use that file. For example, a predicate spanning two files with one file per logical partition causes executing partition 0 to open both files before slicing out partition 0's rows.

**Remediation:** intersect the query's candidate file ranges with the requested partition's row range before opening data files, and stream only that intersection. Preserve the residual predicate above the scan. Add a regression case where a filtered partition is executed while a non-overlapping candidate file is unavailable, and assert each candidate file is decoded only by partitions whose row ranges overlap it.

## Review notes

- I did not confirm a data-loss or wrong-result defect in duplicate/null joins or interval inclusivity from static inspection.
- The local write buffer explicitly documents that a single oversized input batch may exceed the byte target (`local_table.rs:101-102`); I did not report that documented behavior as a defect.
- Intentional divergences from the request (IEEE float total order, Vortex null order, unsupported `approx_cdf`, and deferred randomness/cache/persist) are excluded.

## Follow-up validation

### Indexed partition issue: runtime reproduced

I ran a focused Python repro using the existing `_native.abi3.so` (no build): write `range_table(8, n_partitions=2)`, reopen it, confirm `filter(idx >= 0).count()` returns 8, delete the second Vortex file, then collect `loaded.filter(loaded.idx >= 0)._filter_partitions([0])`. Partition 0 fails with `FileNotFoundError` for the deleted file even though that file belongs only to partition 1. This confirms the previously described failure mode end to end. The repro ran under `PYTHONPATH=python uv run --no-sync python` from the Vail workspace; the table was created under the OS temporary directory.

### Fragment order and descriptor round-trip

I inspected the existing storage round-trip test `crates/vail-storage/tests/roundtrip.rs` around its first ordered snapshot case. It deliberately submits files out of key order and asserts that a full read returns globally key-sorted rows; this matches the fragment index's metadata sort and snapshot reader selection. The per-file bounds are certified by `certify_file`, and `FragmentIndex::new` rejects interior overlap. Local storage then associates descriptor file offsets with the original stream order, which is the order produced by the keyed Table stream. I found no second concrete metadata/bounds inconsistency to report.

### Indexed partition issue: fixed

The indexed provider now passes its execution partition's original row range into storage together with the key queries. Storage intersects indexed candidate paths with files overlapping that half-open row range before opening Vortex data files. Original batch offsets, boundary slicing, and DataFusion's residual predicate remain intact. Empty row ranges select no files.

Regressions in `python/tests/test_storage.py` remove every unrelated data file and collect the first, middle, or last filtered partition, for broad and narrow key predicates with an additional residual predicate. A separate case removes all files and verifies that a partition whose file cannot match the key predicate returns empty. The rolled-file storage test covers duplicate keys, multiple files per partition, partially overlapping boundary files, and an empty range inside an absent file. See the [saved validation record](../../../conformance/runs/20261012-indexed-partition-files/SUMMARY.md).
