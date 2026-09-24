# Vail

Vail is an early embedded Rust implementation of the Hail Table API, using **DataFusion, Arrow, Vortex, and Embedded execution (DAVE)**, with a Python frontend.

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

Use a fresh directory for each initial write. Vortex compilation requires `flatc` on `PATH`.

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

The Python slice supports primitive numeric, Boolean and string values, nested structs and arrays, typed row projections (`select`, `annotate`, `transmute`), filtering and collection expressions. Python builds a typed expression IR; native Rust lowers it to DataFusion physical expressions. Array construction and length reuse DataFusion functions; indexing, slicing and reductions adapt Hail's bounds, missingness and numeric semantics. Higher-order collection lambdas, sets, globals and aggregations remain incomplete. Sort keys currently restrict strings to ASCII until Hail's full Unicode comparison is verified.

`parallelize`, `range_table`, field collection, `key_by`, `order_by`, `head`, `tail`, `take`, `collect`, `count`, `_force_count` and `n_partitions` execute through the native bridge. `range_table` constructs Arrow batches in Rust using Hail's range partition rules; it still materializes the input.

The native execution API sorts Arrow partitions with DataFusion `SortExec` and `SortPreservingMergeExec`. It accepts a host-supplied task context and memory pool; tests force sort spill. Input batches and collected results are still materialized in memory. Arbitrary-plan ordering proof propagation, lazy scans, full Hail comparators and aggregators, and full frontend reuse remain work in the plan.

The storage API handles local, unpartitioned initial snapshots with Vortex data files, Iceberg Avro manifests, and an immutable descriptor bound to a reserved snapshot ID. A persisted primitive-key interval index certifies file order and resolves live Iceberg tasks for ordered multi-file reads. Tests reopen a snapshot containing three real Vortex files. Reads perform no row sorting; missing or invalid required ordering metadata is rejected. Range planning currently enumerates all manifest tasks before pruning data files.

Storage is not yet exposed as Python `Table.write`/`read_table`; overwrite, general appends, schema evolution and cloud catalogs remain unsupported. The public prototype accepts and returns materialized Arrow batches. See [the storage crate](crates/vail-storage/README.md), [upstream PR pin](patches/README.md), and [fragment-scan design](docs/research/ordered-fragment-scan.md).

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
