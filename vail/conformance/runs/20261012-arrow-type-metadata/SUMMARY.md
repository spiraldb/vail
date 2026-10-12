# Versioned Arrow logical type metadata

Special types are explicitly tagged with version 1 logical type descriptors on Arrow struct/list carrier fields. Both Python and Rust infer ordinary types for untagged sentinels, validate versions and layouts, and reject invalid nested metadata. Explicit input schemas remain authoritative after physical layout validation.

Typing metadata is restored in Rust with Arrow kernels at expression, partition-batch, collect, and sample export boundaries. Scalar extraction preserves nested arrays. DataFusion collection operations remain library operators; physical views are normalized where constructors discard annotations. DataFusion 55 metadata-bearing Cartesian joins use its existing nested-loop operator because its cross-join scalar conversion drops list metadata. Comparisons no longer infer special semantics from field names.

Validation after native rebuild: **777 Python tests passed, 1 skipped, 19 subtests passed; 155 Rust tests passed** across ordering, execution, storage, and Table crates. The new Python regression module has 26 cases covering ordinary sentinel-shaped types, all special representations, nested collections, malformed/nested versions, explicit tuple schemas, globals broadcasting, joins/lookups, export, and storage. Existing Rust Vortex audit fixtures now explicitly mark their logical types instead of relying on names; their comparison assertions are unchanged.

```sh
PYTHONPATH=python uv run --no-sync python -m pytest python/tests -q
CARGO_INCREMENTAL=0 RUSTC_WRAPPER='' cargo test --offline -j 1 -p vail-ordering -p vail-execution -p vail-storage -p vail-table
```

See `docs/ARROW_TYPE_METADATA.md` for the encoding contract. Validation used the shared working tree, including pending scan, P1, and indexed-read changes; source hashes identify the tested snapshot.
