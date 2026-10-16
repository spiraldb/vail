# Iceberg PR 2835 with Vail snapshot hooks

`iceberg-rust/` is a source snapshot of the Apache iceberg-rust PR 2835 head at
`0252fe9174aa3062da51328c444a765aa7de2dae`. That PR supplies the actual
Vortex data-file writer, reader, field-ID schema handling, metrics, scan
dispatch, and `VORTEX` manifest format. Vail does not implement a separate
Vortex codec in Iceberg. The PR pins Vortex `0.86.1`, Arrow `59.2`, and
DataFusion `55.0.0` in its workspace.

`iceberg-rust-vail.patch` contains every local change relative to that exact
PR commit. The four touched paths add only:

- A Cargo workspace locator for the copied Iceberg crate.
- A public export of `MemoryCatalogBuilder` for local tests and examples.
- A caller-reserved snapshot ID on fast append, validated by the producer.
- A guard against rebasing an initial Vail snapshot descriptor onto a table
  that gained a snapshot during catalog conflict retry.

The snapshot ID hook lets Vail write an immutable descriptor containing the
eventual snapshot identity before the Iceberg metadata commit. The empty-table
guard ensures the descriptor's live-file fingerprint remains accurate after a
retry. Vortex remains a nonstandard Iceberg data-file extension; unmodified
readers must implement PR 2835 or equivalent support.

# DataFusion scalar compaction cardinality

`datafusion-common/` is the published Apache DataFusion `datafusion-common`
55.0.0 crate source (Apache-2.0; license and NOTICE retained).
`datafusion-common-vail.patch` contains the only source change: recursive scalar
view-buffer compaction preserves the length and validity of zero-field structs
with Arrow's `StructArray::new_empty_fields`. The upstream constructor inferred
length from child arrays and panicked when ordered `array_agg` compacted a
nested empty struct. Nonempty structs retain the upstream path.
