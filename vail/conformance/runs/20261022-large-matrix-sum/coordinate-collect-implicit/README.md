# Implicit coordinate collect layout

Material e0129fb7 (40c7c3d4 layout +069840c9 Matrix scalability guard +e012 checked native Arrow extension); public native wire interoperability test97d7a29d. Normal native SHA256 **cda61f064f57a60fa12dba03d8b6a11b895af5598136b79b00d7b264fe39622c**. Rust1.98 release, isolated worktree target, configured kache preserved, normal maturin develop --release --locked (64s). No SO-copy workaround.

This specializes only the actual typed native ArrayAgg factory whose grouped protocol is supported. Retain Arrow row-major batches/filter bitmaps or native merged ListArray states. Native MutableArrayData prepares extension functions once/source, copies implicit coordinate ranges into the necessary coordinate-major output once, then releases sources and retains immutable snapshot. No per-entry group ID, (group,row) pair, counting-sort metadata, or interleave-index buffer. Native Arrow handles physical storage, nested descriptors/nulls. Both try_extend calls propagate inner List/Utf8/run-end offset errors; total outer List offsets are separately checked.

Public runtime: **146 Python PASS,1 known-existing test deselected** (8.51s). That collection_aggregate Matrix capture cardinality failure was proven on normal reviewed c140 native; proof is committed in ../coordinate-collect-snapshot. **12 native tests PASS**, including state-before-first-input, repeated snapshots/update, actual public native ArrayAggGroupsAccumulator consuming emitted state, Utf8View/LargeUtf8/nonnullable nested child descriptors, nullable empty structs, and prior primitive reducer COW/UInt64-state tests. No mock or private-state assertions.

## Representative fresh local comparison

This is a deterministic fixture-free coordinate collect workload, **not an official upstream benchmark**: Table100k rows×100 positions,32 partitions,20% missing element values retained, collect length returned to avoid exporting10M Python values. Profile confirms actual collect/materialization, not count substitution. Both engines concurrency32 on the same14-core96GiB laptop. Stock PyPI Hail0.2.139/Spark3.5.9/Java11, no Hail build. Engines serial,3 repetitions each, no own build/profile competition. Query includes construction+action, excludes engine initialization; first cold query included. Routine background OS activity was not excluded.10Hz process-tree CPU retains exited-child totals; Hail JVM included. Short runs can miss transient RSS peaks; JVM retained heap makes cross-engine memory comparisons limited.

| Engine/source | Median wall(s) | Median core(s) | Median sampled tree RSS(bytes) |
|---|---:|---:|---:|
| Native snapshot baseline c1231d3c/d1f1 |0.0892573|0.336656|671596544|
| Implicit layout e012/cda61f |0.0282271|0.274021|390397952|
| Fresh stock Hail |0.667997|5.490577|1754497024|

Implicit layout improves snapshot baseline3.162x wall/1.229x CPU with41.9% lower sampled RSS. Hail/Vail23.665x wall/20.037x CPU. Summary.json contains all attempts, min/max, independent metrics and source/native provenance; telemetry attached. Snapshot baseline was rebuilt normally using cached target/kache in2s, not copied. Fresh Hail reused for final checked-extend candidate; no duplicate Hail run.

Final1kHz profile has774 samples (including startup/waits): CoordinateCollect inclusive146, MutableArrayData162; **zero ArrayAggGroupsAccumulator/interleave samples**. Before snapshot stage1349 samples: native groups620 inclusive/interleave125; snapshot stage1063: native groups393/interleave130. Counts overlap and are proportional diagnostics, not timing estimates. Required output copy/null-bit extension now dominates actual collect work. Embedded symbols make profile standalone.

## Explicit remaining scope

Public Matrix column collect keeps its existing batched relational fallback: a global nested List state can exceed i32 offsets where batched per-column output remains representable. It needs a proven cardinality bound or a chunked output plan before direct eligibility is safe. Standalone agg.array_agg has an existing global nested-list result and is safely bounded/error-checked here. Internal ordered collect retains existing ordered-scope planning; the adapter preserves incoming/merged source order and null elements.

Take/ordered TopK, set/counter value-hash states, histogram/sketch/regression adapters remain an explicit separate queue; this receipt does not claim all structured algorithms are fixed.
