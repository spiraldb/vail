# Vail

Vail is an early embedded Rust implementation of the Hail Table API, using **DataFusion, Arrow, Vortex, and Embedded execution (DAVE)**, with a Python frontend.

Vail's original code is licensed under [MIT](LICENSE). Upstream dependencies and vendored third-party code retain their own licenses and notices.

The target is Hail Table operation and semantic parity, with Vortex-backed Iceberg storage replacing Hail's native table format. Vortex file-format support comes from [apache/iceberg-rust PR #2835](https://github.com/apache/iceberg-rust/pull/2835), pinned to `0252fe9174aa3062da51328c444a765aa7de2dae`. Vail uses that implementation's reader and writer; it does not maintain a separate Vortex integration in Iceberg. The PR remains an upstream preview, so this does not imply compatibility with arbitrary Iceberg readers.

The first implementation includes a native ordered-collection API, a Python Table slice backed by that Rust code, Vortex files written and read through PR #2835's Iceberg integration, and a typed conformance harness. **This is a partial prototype, not full Hail Table parity.**

## Run the native and Python examples

Rust 1.98.0 is pinned in `rust-toolchain.toml`. The dependency lock keeps DataFusion at 55.0.0 and Arrow at 59.2.0.

```sh
cargo run -p vail-execution --example ordered_table --locked
uv sync --python 3.12 --no-install-project
uv run --no-sync maturin develop --locked
uv run --no-sync python examples/table.py
```

The storage example supports separate writer and reader processes:

```sh
mkdir -p /tmp/vail-demo
cargo run -p vail-storage --example local_roundtrip --locked -- write /tmp/vail-demo
# Pass the printed metadata URI to a new reader process:
cargo run -p vail-storage --example local_roundtrip --locked -- read '<metadata-uri>'
```

Use a fresh directory for each initial write. The pinned Vortex 0.86.1 crates ship generated FlatBuffers Rust code, so the normal Vail build does not require `flatc`. Building the separate Vortex source submodule regenerates that code and requires the FlatBuffers compiler, selected through `FLATC` or `PATH`.

```python
import vail as hl

t = hl.Table.parallelize(
    [{"position": 30}, {"position": None}, {"position": 10}, {"position": 20}],
    schema=hl.tstruct(position=hl.tint32),
    n_partitions=3,
).key_by("position")

assert [row.position for row in t.take(3)] == [10, 20, 30]
assert list(t.order_by(hl.desc("position")).key) == []
```

The Python slice supports primitive numeric, Boolean and string values, nested structs and arrays, typed row projections (`select`, `annotate`, `transmute`), filtering and collection expressions. Python builds typed TableIR and expression IR; Rust lowers the complete supported graph to DataFusion physical plans. Array construction and length reuse DataFusion functions; indexing, slicing and reductions adapt Hail's bounds, missingness and numeric semantics. Globals support annotate/select/transmute, row expression references, cross-table `index_globals()`, `hl.eval`, and Arrow persistence. Higher-order collection lambdas, sets, and general aggregations remain incomplete. Sort keys currently restrict strings to ASCII until Hail's full Unicode comparison is verified.

`Table._same` checks logical schemas, keys, globals, row sequence, and duplicate
multiplicity with Hail's recursive value similarity rules. Row comparison uses a
DataFusion full outer merge join on row ordinals, a comparison expression, and a
limit on mismatches. Globals are a typed singleton relation, bound to row expressions
through DataFusion cross joins and persisted as a checksummed Arrow IPC file.

The Python Table wraps a shared Rust TableIR DAG. `parallelize` retains Arrow input
buffers; `range_table` stores bounds. Transformations build nodes, and actions lower
them to fresh DataFusion plans. Projections, filters, limits, sorts, forced counts,
and equality execute through DataFusion. Hail-specific sources and bounded tail
are DataFusion extension nodes. See [TableIR](docs/TABLE_IR.md) for details and limits.

Explicit sorting streams its input into DataFusion SortExec; it no longer collects
input in Rust first. Tests verify spill under a bounded host-supplied memory pool.
The embedded plan currently uses one ordered physical stream while preserving Hail
logical partition identity. Parallel execution, distributed sorting, full Hail
comparators, and optimizer integration remain future work.

Keyed joins construct independent output partitions from overlapping original key intervals and execute DataFusion sort-merge joins within each interval. See [Interval partition joins](docs/INTERVAL_PARTITION_JOINS.md).

The storage API handles local, unpartitioned initial snapshots with Vortex data files, Iceberg Avro manifests, and an immutable descriptor bound to a reserved snapshot ID. A persisted primitive-key interval index certifies file order and resolves live Iceberg tasks for ordered multi-file reads. Tests reopen a snapshot containing three real Vortex files. Reads perform no row sorting; missing or invalid required ordering metadata is rejected. Range planning currently enumerates all manifest tasks before pruning data files.

Python `Table.write`/`read_table` use local Viceberg directories with atomic overwrite publication. Read construction resolves metadata only; its lazy source streams data when an action executes. General appends, schema evolution and cloud catalogs remain unsupported. See [the storage crate](crates/vail-storage/README.md), [upstream PR pin](patches/README.md), and [fragment-scan design](docs/research/ordered-fragment-scan.md).

Text interoperability includes lazy `import_lines`, delimited `import_table`,
`read_csv`, and newline-delimited `read_json`, plus streaming `Table.export`,
`export_csv`, and `export_json` actions through DataFusion scans and sinks.
See [Text import and export](docs/TEXT_IMPORT_EXPORT.md) for supported options
and explicit limitations, including Hail float and nested text rendering.

## Checks

Save all Vail-owned suite outcomes, command logs and source fingerprints:

```sh
uv run --no-sync python scripts/run_all_tests.py --label integrated
```

Results go under `conformance/runs/`. Known unsupported functionality and an unavailable Hail oracle remain separate from passing tests. See [the Luna implementation review](docs/LUNA_IMPLEMENTATION_REVIEW.md).

Individual checks:

```sh
cargo test -p vail-execution -p vail-storage --all-targets --locked
uv run --no-sync python -m pytest python/tests conformance/test_harness.py -q
uv run --no-sync python -m conformance.upstream_table_cases
```

Storage checks exercise the PR reader directly, preserve a descending sequence across row splits, reopen empty snapshots with globals, and reject stale writers. The Python checks include the primitive fixture suite at multiple partition counts and a fresh-process JSON-lines worker. Fixtures are labeled source-derived until actually captured by a matching Hail oracle. Unsupported fixtures must not count as passing compatibility tests.

The first oracle attempt was unavailable: the pinned Hail checkout lacks generated version modules and its runtime JAR, and the isolated environment lacks the Hail runtime dependencies. See [the harness instructions](conformance/README.md). Passing native fixtures is not yet measured equivalence against an executing Hail backend.

## Design and implementation queue

- [Test-by-test Hail inventory](docs/HAIL_TEST_INVENTORY.md) and [implementation requirements/task assignments](docs/TEST_IMPLEMENTATION_TASKS.md).
- [Implementation plan](docs/PLAN.md): architecture, ordering contract, storage design, delivery gates.
- [Sol and Luna assignments](docs/TASKS.md): bounded work packages and acceptance criteria.
- [Hail compatibility audit](docs/research/hail-table-contract.md) and [machine-readable API inventory](docs/research/hail-api-inventory.json).
- [DataFusion ordering audit](docs/research/datafusion-ordering.md).
- [Vortex and Iceberg audit](docs/research/vortex-iceberg-storage.md) and [PR #2835 ordering review](docs/research/iceberg-pr-2835-ordering.md).
- [Python frontend integration](docs/research/python-frontend.md).
- [Pinned source revisions](upstreams.lock.json).

The first implementation wave is in progress. The work queue keeps unimplemented methods and scope boundaries visible; completed primitive examples are not claims of complete Table compatibility.

### Python table persistence

`table.write(path)` and `vail.read_table(path)` use local Viceberg directories.
They preserve row types, keys, row order, and logical partition boundaries,
including empty partitions and zero-column rows. The writer rolls Vortex files
at a 64 MiB Arrow buffer target or 1,048,576 rows, and at partition boundaries.
Keyed files have a certified interval index; reads follow it without a whole-table
sort. Unkeyed files carry an explicit persisted sequence to preserve row order.

`write(path, overwrite=True)` publishes a completed immutable generation through
an atomic `_vail_table.json` replacement. Previous generations remain on disk;
garbage collection is not implemented. Paths embedded in Iceberg metadata are
absolute, so moving a directory is not supported. Concurrent writers fail while
the directory's `.write.lock` is held; a process crash may leave a stale lock.

This API currently supports local paths and `file://` URIs without spaces or
non-ASCII characters. Certified keys support integers, booleans, and ASCII
strings. Legacy `.ht` input, custom codecs, staging, and the
private `read_table` options are not implemented and are rejected explicitly.
Tables hold an immutable Rust TableIR DAG, including a snapshot-pinned read
source and typed scalar expression IR. Construction reads schema/snapshot metadata
only; data files open when an action pulls batches. MapRows and Filter process
batches incrementally; Head stops upstream reads once satisfied. Collect localizes
its result and Tail retains a bounded suffix. Write consumes the execution stream
with bounded file buffers and backpressure; explicit sorting is a DataFusion operator
which can spill under a configured memory limit. Each action executes afresh; TableIR has no result cache.
