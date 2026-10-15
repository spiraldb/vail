# Native Python / Rust boundary

The expression and Arrow transport migration replaces the runtime JSON/IPC
boundary with PyO3 handles and Arrow C Streams. The base is `ffddaa0` on top of
the previously merged randomness and logical-planning work. See the current
[boundary contract](../ARROW_PYTHON_BRIDGE.md).

## Representation and ownership

- Python expression methods construct the Rust `Expr` enum directly. Child
  expressions share `Arc<Expr>` nodes. Transient Python constructor arguments and
  shallow inspection views contain native children, rather than encoded trees.
- Python type classes retain native `Type` handles. Rust owns recursive type
  structure, Arrow conversion, and validation. Aggregate result-type hints are
  typed Rust fields, not generic descriptor values.
- Arrow C Stream imports transfer release-callback ownership; result exports retain
  shared record batches. The bridge checks schema and scalar cardinality.
  Configuration maps cross as ordinary values, without encoding strings or bytes.
- Scalar literals retain native scalars or Arrow buffers. Typed set literals are
  deduplicated after numeric coercion with native scalar equality and canonical
  ordering, including nested sets. Unchanged ordinary buffers remain shared.
- Captured globals and lookups retain their owners on expression handles. Returning
  an existing handle reuses its dependency metadata. Shared-node equality and a
  64-level repeated-subexpression graph avoid expanding that graph into a tree.

## Remaining serialization

Persistence and external formats still require bytes: Vortex table files, globals
Arrow IPC files, matrix descriptors, reference files, CSV/JSON/VCF, and conformance
fixture/report files. VCF strings require format-specific escaping. Arrow's
versioned Vail logical-type metadata remains a portable JSON descriptor for
external Arrow input and storage. Those tags also accompany Arrow C Stream
schemas. Rust owns this codec; Python does not maintain a second type decoder.
Human-readable logical and physical plan strings remain diagnostic output.

None of these formats is used to encode runtime expression trees or row buffers
at the PyO3 boundary. Constructing Arrow arrays from Python rows, concatenating
multiple batches, sorting/canonicalizing sets, and converting results to Python
objects can still allocate or copy; native transport does not make all operators
zero-copy.

## Validation

Final results: **1,281 Python tests passed, four pre-existing failures, one skipped,
and 19 subtests passed; 226 Rust tests passed; six upstream randomness tests passed.**
Raw logs and binary provenance are in
[`conformance/runs/20261015-native-memory-boundary`](../../conformance/runs/20261015-native-memory-boundary/summary.json).
The final full Python run overlapped Rust compilation and took 106.27 seconds;
the previous sequential run with the same four failures took 84.73 seconds.
These runs are regression checks, not isolated throughput benchmarks.

The full local Python run includes `python/tests`, `conformance/test_harness.py`,
and `conformance/test_upstream_backend.py`. Native boundary regressions cover
shared buffer addresses in both directions, garbage-collection lifetime,
independent repeated C Stream exports, immutable expression nodes, shared graph
identity/equality, native types, empty-struct row cardinality, NaN payloads and
signed zero, and execution with Python JSON and Arrow IPC codecs disabled.

The full Rust run covers library and integration tests in `vail-execution`,
`vail-table`, `vail-storage`, `vail-ordering`, and `vail-vcf`. The upstream
`test_randomness.py` module runs through the existing Vail backend adapter.

Four local Python failures were already present before this migration:

1. `test_arrow_type_metadata.py::test_tagged_types_survive_execution_export_and_storage[dtype4-value4]`:
   Vortex cannot write the empty-list case.
2. `test_computed_keys_order.py::test_mixed_and_nested_keys_and_missing_values_round_trip`:
   `_same` reports unequal nested/missing mixed keys after the storage round trip.
3. `test_globals.py::test_globals_reads_are_lazy_snapshot_pinned_and_checksummed`:
   the test expects checksum validation; storage intentionally no longer checksums
   globals and the corrupted file produces an I/O error instead.
4. `test_logical_nested_aggregation.py::test_grouped_nested_reducers_have_logical_aggregate_and_unnest`:
   the structural assertion requires `Unnest`, whereas coordinate reductions use
   a direct logical UDAF. Its value assertions pass.

The slowest full-suite test is the small row-aligned matrix transformation chain.
A separate cProfile run took 30.96 seconds: 17.46 seconds in four native
`eval_global_arrow` calls, 5.13 seconds in native counts, and 5.02 seconds in native
`aggregate_globals` calls. Python expression/handle inspection is a small part of
this profile. This identifies a remaining Rust planning/action latency concern;
it does not establish a performance comparison against the pre-migration build.
