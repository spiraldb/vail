# Indexed partitions open only related candidate files

Before the fix, writing an 8-row, 2-partition table, removing partition 1's data file, and collecting `table.filter(table.idx >= 0)._filter_partitions([0])` failed with FileNotFoundError. The indexed provider computed its own row range but requested interval candidates from the entire snapshot.

The provider now supplies both key intervals and its half-open snapshot row range to storage. The existing fragment index selects key candidates; descriptor row offsets restrict those candidates to files overlapping the requested execution partition before any data file is opened. Original batch offsets and provider boundary slicing remain unchanged, and DataFusion still applies the residual row predicate. Empty ranges select no files. Snapshot/manifest/index validation remains active.

Regression coverage includes first/middle/last partitions with all unrelated files removed; broad/narrow predicates and a non-key residual predicate; a partition whose absent file cannot match the key query; duplicate keys and multiple rolled files per partition; boundary files only partly overlapping the requested range; and an empty range inside an absent file. Full reads still fail when required files are missing. The Rust interval roundtrip fixture was also updated from its stale field layout to start, start_exclusive, end, end_inclusive without changing its tested interval values.

Validation after native rebuild: **63 focused Python tests passed; 751 full Python tests passed, 1 skipped, 19 subtests passed; 89 Rust storage and Table tests passed**.

```sh
PYTHONPATH=python uv run --no-sync python -m pytest python/tests -q
RUSTC_WRAPPER='' cargo test --offline -j 1 -p vail-storage -p vail-table
```

Validation used the shared working tree, including pending scan/P1 work. Logs and hashes identify that tested snapshot.
