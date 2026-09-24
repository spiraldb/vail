# Sol and Luna work assignments

The newer [test-driven inventory and work packages](TEST_IMPLEMENTATION_TASKS.md) track the current dispatch. The Table action slice and primitive fragment interval planner have been implemented; the full persisted multi-file index is still outstanding. Source-extracted upstream execution results are recorded in [upstream-table-results.json](../conformance/upstream-table-results.json).

Status as of 2026-09-24: typed Rust expressions power Python select/annotate/transmute, Table.filter and array construction/indexing/slicing/numeric reductions. The verified Luna run has 25 passing Rust tests, 62 passing Python/harness tests, 19 subtests, and nine passing recorder tests. Original Hail bodies have 89 passing cases (nine complete definitions plus 80 head/tail variants), with full annotate still failing at `_same`. Globals fixtures remain unsupported and the live Hail oracle is unavailable. See [the verified run](../conformance/runs/20260924T205540Z-integrated-verified/summary.json), [baseline](BASELINE_SUMMARY.md), and [review](LUNA_IMPLEMENTATION_REVIEW.md).

| Agent | Model | Completed assignment | Implementation ownership |
| --- | --- | --- | --- |
| `hail_table_contract` | GPT-6 Luna | Audit, API ledger, typed fixtures, isolated oracle worker and harness tests | Complete executable API/IR coverage and capture live Hail oracle results |
| `datafusion_ordering` | GPT-6 Sol | Tested native ordering API, Rust example, and PR #2835 ordering/source review | First slice delivered; native semantic kernel and broader Table operators remain |
| `vortex_iceberg_storage` | GPT-6 Sol | PR #2835 reader/writer integration, initial and empty snapshots, validation/race tests, combined DAVE integration and dependency pin | Required key-interval index and ordered fragment planner, streaming public API, overwrite/append and schema evolution |
| Integrator | Parent agent | Architecture, compiled PyO3 bridge, fixture adapter and Python integration tests | Adapt the full Hail frontend, shared typed IR, Python storage binding and Table closure |

Owners may work concurrently only after the relevant interface is agreed. They should edit separate modules; one owner edits the root Cargo workspace and lockfile at a time. Any changes in upstream checkouts require a separately tracked patch branch/check-out, rather than mixing research baselines with implementation edits. Each package produces a reviewable diff, relevant checks, and known gaps.

## First implementation wave: G0/G1

### C1 — Expand the executable compatibility ledger (Luna)

**Inputs:** pinned Hail source and `scripts/inventory_hail_api.py`.

**Scope:** extend the current direct-member inventory with inherited protocols, Table constructors and `read_table`, expression/aggregate/scan functions reached by each operation, and the associated Table/value IR nodes. Record signatures, defaults, return types, key/global/order/partition effects, source validation, and test coverage. Classify MatrixTable and Spark boundaries explicitly. Avoid counting AST overloads as distinct runtime methods.

**Deliverables:** `conformance/api-ledger.json`, API-to-IR map, typed fixture schema, and a small oracle runner operating in an isolated Hail environment. Preserve Hail's license notices for reused fixtures/frontend code.

**Acceptance:** public members cannot vanish without an explicit disposition; fixtures distinguish missing from empty and preserve nested/genomic types; order-sensitive cases compare sequences and ties/undefined order use permitted-outcome checks. Include duplicate join keys, descending null placement, `order_by` unkeying, row/global source errors, and logical partition behavior.

**Dependencies:** none for inventory/fixtures; Rust comparison runner follows E2. Do not reimplement Rust comparators or join algorithms in this package.

### E1 — Resolve a buildable DAVE dependency cohort (Sol execution)

**Inputs:** `upstreams.lock.json`; candidate DataFusion `55.0.0`/Arrow `59.2`; Vortex and Iceberg Rust PR #2835's dependency cohort.

**Scope:** minimal Cargo workspace/toolchain, one batch crossing DataFusion and Vortex, Iceberg metadata compile smoke, and a PyO3 feasibility dependency check. Inspect the selected versions' actual APIs; the audit of newer DataFusion HEAD is not proof those APIs exist in the candidate cohort. Do not pull in iceberg-rust's DataFusion adapter unless necessary and compatible.

**Deliverables:** pinned toolchain, `Cargo.toml`/`Cargo.lock`, one executable smoke example, recorded `cargo tree` dependency review. Sol storage provides required feature flags; integrator reviews the bridge boundary.

**Acceptance:** scan → sort → collect compiles/runs against one compatible Arrow/DataFusion type graph; file codec prerequisites recorded; no dependency on the local sibling directory layout in a published package.

**Dependencies:** none. E1 owns shared manifests/lockfiles during this wave.

### E2 — Prove ordering under adversarial execution (Sol execution)

**Scope:** minimal `OrderRequirement`/`OrderProof`, shared comparator interface, and ordered result collection. Use primitive keys first. Implement local sort plus sorted merge and conservative property invalidation. Recognize ordered ranges only when provenance and row/boundary validation support them. Keep keys/uniqueness/tie stability distinct.

**Acceptance:** interleaving sorted partitions, delayed first stream, empty partitions, duplicate/null/descending keys, renamed/projected sort fields, and hash exchanges cannot produce false global-order proofs. Plain multi-partition DataFusion collection is never silently used as keyed collection. Correctness holds at different batch sizes/partition counts and with external-sort spill. Check the optimized plan, not just pre-optimization nodes.

**Deliverables:** native proof module, small in-memory fixtures, ordered collector, targeted tests and a short measured plan report.

**Dependencies:** E1; comparator fixtures from C1. Do not expand into the whole Table API.

### S1 — Integrate Iceberg Rust PR #2835 (Sol storage)

**Scope:** use [apache/iceberg-rust PR #2835](https://github.com/apache/iceberg-rust/pull/2835), pinned at `0252fe9174aa3062da51328c444a765aa7de2dae`, for genuine Vortex files referenced by Iceberg snapshots. Route writes through its `VortexWriterBuilder` and reads through its Iceberg Arrow reader. Do not reimplement Vortex format support in Iceberg. Keep any required Vail snapshot-ID/commit hooks in a separate, minimal patch. Retain Iceberg JSON/Avro metadata formats and test storage independently of the Hail frontend.

**Acceptance:** a fresh process resolves the catalog snapshot and decodes the Vortex rows; wrong/unsupported file formats fail explicitly. The manifest reports the actual format. Document which stock readers reject the extension and why. File IDs, nested schema/validity, and schema evolution must round-trip through an explicit mapping.

**Deliverables:** pinned PR source/provenance, minimal Vail adapter, separately recorded metadata-only hooks, and a real PR writer/reader integration fixture. Sol execution reviews the pinned reader's inner and outer ordering behavior.

**Dependencies:** E1 for build execution. Schema/patch design can proceed alongside E1. No substitution of Parquet storage as completion of this package.

### S2 — Snapshot-bound key-interval index and ordered fragment reader (Sol storage + Sol execution)

**Scope:** immutable semantic descriptor plus a required typed composite-key interval index for persisted keyed/explicitly ordered tables. Sol storage establishes and verifies local key order/bounds while writing, publishes index pages atomically with snapshot metadata, and resolves index entries to live PR #2835 scan tasks. Sol execution implements metadata-only range seeking, lexicographic fragment scheduling, bounded prefetch and optional streaming merge of declared overlapping sorted runs. Reuse the snapshot-ID/producer hooks. See [the ordered-fragment design](research/ordered-fragment-scan.md).

**Acceptance:** shuffled Iceberg manifest/file enumeration yields fragments in typed key-interval order without sorting table rows. Non-overlapping intervals concatenate; supported overlapping sorted files merge with bounded active streams. `head` and key-range predicates open only relevant fragments. Missing/invalid/unknown index or comparator fails explicitly and requires an explicit repair/import outside read. Missing globals/type/reference metadata also fails. Assert no row-sort node in keyed storage read plans. Commit conflict cannot reuse the old index. Fresh-process reopen, time travel, and a compaction rewrite preserve rows/globals/order. Verify an actual replace/rewrite transaction for full overwrites; `FastAppendAction` alone does not satisfy that requirement. Unsupported deletes are rejected, not ignored.

**Deliverables:** descriptor/index schema and versioning, write-side comparator/bound validation, commit coordinator API, ordered fragment planner, metadata validation and range-pruning tests, documented retention rule.

**Dependencies:** S1, E2 shared comparator/proof interface. The first scan may use a correct single-stream ordered path; uncontrolled asynchronous flattening is not acceptable.

### P1 — Prove the frontend/backend seam (integrator, Luna support)

**Scope:** adapt the smallest pinned Hail frontend closure necessary for a Table range/parallelize → annotate/filter → key/order → take example. Bridge structured IR/type requests and result values through PyO3 to the native core. Record the alternative legacy text-IR/wire-codec costs and choose one bridge. Isolate unconditional PySpark imports and backend initialization paths.

**Acceptance:** Python example runs with no JVM/Spark/service; native Rust produces the same typed result without Python. Globals, type queries, a source mismatch error, and a reference definition cross the bridge. Explicit `import vail` works without a global `hail` module alias. Cancellation/GIL/runtime ownership is documented and exercised.

**Dependencies:** E1/E2; C1 protocol map. Luna can prepare import/signature checks while native work proceeds.

## Following implementation waves

| ID / owner | Bounded package | Depends on | Acceptance |
| --- | --- | --- | --- |
| E3 / Sol execution | Hail type/comparator/scalar kernel, exact missingness/equality/hash/numeric rules | C1, E2 | Oracle comparison on primitive/nested types and accepted key domains; nested parent validity; no SQL-default drift |
| P2 / Integrator + Luna | Rust Table builders and Python frontend calls for G2 operations | P1, E3 | Same core/IR on both entrypoints; signatures and validation preserved; no duplicate Python execution engine |
| S3 / Sol storage | Public `write`, `read_table`, `checkpoint`, overwrite, metadata-only changes and `write_many` adapter | S2, P2 | Matches Hail API behavior except bytes/layout; correct failure/partial-completion behavior for multiple outputs; no unsupported cross-table atomicity claim |
| E4 / Sol execution | Full-key equi-joins, lookup, semi/anti joins, intervals/all-matches | E3, P2 | Hail multiplicity/null/prefix/interval semantics, field/global conflict rules, valid output ordering |
| E5 / Sol execution | Grouped/scalar aggregators, union/distinct/collect-by-key/multi-way zip join | E3, E4 as needed | Every assigned aggregator and schema/global rule has oracle fixtures; keyed output order checked |
| E6 / Sol execution | Sequential scans and indices, then associative parallel prefixes | E3, E5 | Partition carry, exclusive/inclusive behavior and float/RNG semantics match; ordering proof survives optimization |
| E7 / Sol execution | Range repartition, coalesce/persistence, bounded-memory fast paths | E2, S2, E6 | Logical partition API contracts hold; split boundaries/duplicates handled; spill/backpressure measured |
| C2 / Luna | Remaining API/frontend adapters: rename/transmute/flatten/expand, display/summary, interop/import/export | P2, semantic prerequisites | API ledger has no unexplained gaps; accepted formats and return types covered; Spark/Matrix decisions recorded |
| S4 / Sol storage | Descriptor-aware compaction/retention and broader external-format/delete interoperability | S2, S3 | Pinned-reader/time-travel safety; no stale certificate reuse; explicit unsupported-feature handling |
| R1 / Integrator + Luna | Packaging, wheels and docs for the new format | G4 operators, S3 | Clean-process installation/native lifecycle and Vortex/Iceberg schema/global/reference/key roundtrip. Legacy `.ht` decoding/conversion is deferred. |
| R2 / Sol execution + Sol storage | Performance and robustness against Hail baseline | G4 | Partition/batch/spill/adversarial conformance passes; benchmark report and all remaining compatibility gaps resolved or explicitly scoped |

The adaptation work includes top-level functions needed to construct and evaluate Table expressions, not just methods named in `table.py`. Methods such as `write_many`, `multi_way_zip_join`, `all`, `any`, `summarize`, `sample`, and partition introspection remain visible in C1/C2 even when their implementation is assigned to another owner.

## Shared interfaces to settle before merging the first wave

1. `TableType`/logical type IDs and field identity, row/global expression axes, key representation, and supported error payload.
2. Hail comparator/equality/hash abstraction and versioning; `OrderRequirement` versus physical `OrderProof`.
3. Session/runtime/resource ownership and result stream/cancellation API.
4. Snapshot descriptor schema, file identity and field-ID mapping, required semantics versus recomputable order evidence.
5. Python IR bridge/version handshake and typed fixture interchange format.

When a package reveals missing upstream capabilities, its owner reports the smallest required extension and a correctness-preserving fallback. A fallback that changes the public Hail contract, replaces Vortex storage, or removes an inventory item does not satisfy its acceptance criteria.
