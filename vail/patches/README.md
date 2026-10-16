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

# Vortex empty nested-list streams

`vortex-layout-vail.patch` records every source difference from the published
`vortex-layout` 0.86.1 crate, including the preexisting canonical-storage changes.
The maintained snapshot additionally accepts empty
list-typed streams in `layouts/list/writer.rs`. Recursive list element streams
can have zero rows (for example, a dictionary's nested list values in an
all-missing batch); rechunking may then emit no chunks. Like the existing flat
and chunked writers, the list strategy writes a typed zero-row chunked layout
with no segments. A one-chunk lookahead preserves the original stream and
sequence IDs for nonempty input. No artificial element or validity is added.
The matching native empty-stream contract test and public nested collection
round-trip tests cover this delta.

# Vortex nullable struct projection

The same `vortex-layout-vail.patch` also fixes nullable struct reads. Parent
validity is independent of all child values: a defined `{x: null}` is distinct
from a missing struct. The previous expanded expression referred to the original
root's validity; partitioning a one-field struct then evaluated that reference
in the child field's scope, conflating parent and child nullness.

The reader now gives parent validity its own typed Boolean dependency backed by
the existing validity child. A collision-free internal field name separates it
from user fields during expression partitioning. Expanded root, field and nested
projection expressions carry the dependency directly. The reader no longer
applies a second unconditional mask after evaluation, which would incorrectly
null non-strict results such as `is_null` and `fill_null`. This changes no stored
format and adds no row serialization or whole-row materialization. The patch
retains all earlier list/projection changes from the maintained snapshot.

Native Arrow-to-Vortex-to-Arrow and raw Vortex file projections distinguish the
conversion and file boundaries, without asserting physical chunk counts. Public
storage regressions cover one- and multi-field structs, independent nested
validity, empty structs, colliding user names, sliced Arrow validity buffers,
multiple input batches, and key filters.
