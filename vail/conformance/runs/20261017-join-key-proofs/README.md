# Join key proofs: normal-policy cloud validation

Exact upstream cases, full inputs (`smoke_fixtures=false`), run serially on the existing 32-vCPU / 128-GiB GCP VM with 10 Hz CPU/RAM/I/O telemetry. Reuse the unchanged Hail baseline from the same VM. No global one-row setting, debug instrumentation, or singleton-prefetch experiment is included in these measurements.

Production source is main `c510e35dbfb9ee1bfeac7117b7843bdf3b77e30a` plus the key-proof/coalesce changes in `8341d3c6a366dd55a0e0e472a94ccbcb24679c60`. SHA-256 verification matched all five production files changed since original benchmark source `2a0c62741209f14e83f689e0d4609c50a0dc2986`: advanced_numeric_aggregates.rs, join_interval.rs, joins.rs, lib.rs and logical_aggregation.rs. The raw runner output is adjacent. Subsequent commits change tests only.

| Case | Original Vail s | Updated Vail s | Hail s | Updated Vail core-s | Updated peak GiB |
|---|---:|---:|---:|---:|---:|
| Wide inner join | 39.460 | 9.946 | 3.645 | 14.24 | 11.386 |
| Wide left join | 37.842 | 9.551 | 3.848 | 15.38 | 10.710 |
| Wide map/filter | 6.156 | 5.671 | 3.572 | 9.59 | 5.992 |
| Partition join 10 / 1000 | 10.060 | 2.738 | 7.083 | 34.15 | 0.727 |
| Partition join 100 / 1000 | 8.784 | 2.365 | 5.488 | 22.06 | 0.752 |
| Partition join 1000 / 10 | 9.148 | 2.862 | 7.202 | 35.22 | 0.752 |
| Partition join 1000 / 100 | 8.188 | 2.381 | 5.293 | 22.34 | 0.771 |
| Partition join 1000 / 1000 | 8.463 | 2.468 | 5.067 | 20.81 | 0.733 |
| Worst-case downsample, accounting fix baseline | 12.089 | 3.018 | 4.669 | 20.83 | 0.316 |

The region-memory cases still trail Hail and retain large buffers. Work continues on repeated closed-expression materialization and batch sizing. The partition sweep and parallel downsample are separately reviewed changes already on main. These observations are single query runs, not confidence intervals.

Root key-proof changes remove no-op naive coalesces, preserve unique nonnull Range-key proofs through key-forwarding row maps and order-preserving subsets, and omit the final ordinal sort only for Inner/Left joins with both inputs proven unique and a single join key. Other cases retain their existing sort. Public tests cover rows/order/value columns, unmatched and empty-right left joins, coalesce partition limits and invalid zero. The original focused test pass preceded a formatting step that accidentally inserted a filename into the test file; review caught that artifact before integration, it was removed, and the entire exact repaired 13-test IR file passed afterward. No performance run used the malformed test file.

Required review: chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact candidate `2d4009414a2de83b9e05d499d1fa965b037332e9` against `c510e35dbfb9ee1bfeac7117b7843bdf3b77e30a`. Source at `8341d3c6a366dd55a0e0e472a94ccbcb24679c60` is unchanged; later children replace private-plan/identity acceptance assertions, add empty-right coverage, repair the formatter artifact, and record validation. Reviewer cleared proof/coalesce semantics, public tests and normal-policy evidence with the residual Hail gap explicit. Reviewer ran no builds/tests and made no edits. No blocking findings remain for this candidate. Closed-expression caching and batch sizing are outside this approval.

## Closed nested constants and wide-output batching

The follow-up production candidate is `0c8bfa4524f5342ef7f6d83a44d7604b053e5dc2`, based on `ac0511e1c8950a854c528938402c932c016a222f`. The adjacent `wide-production-validation.json` records the five production source hashes, native/wheel identities, resource observations and download checksum. All source hashes matched the cloud source used to build the wheel. Three repeated full-input runs used normal production settings, without a global one-row override or diagnostic operator patches.

| Case | Median Vail s | Hail s | Vail core-s, median-wall run | Vail peak GiB, median-wall run |
|---|---:|---:|---:|---:|
| Wide inner join | 0.827 | 3.645 | 4.30 | 0.763 |
| Wide left join | 1.009 | 3.848 | 5.87 | 2.157 |
| Wide map/filter | 0.833 | 3.572 | 4.30 | 1.154 |

The exact final candidate passed 1,857 Python tests, one skip and 19 subtests, plus 19 public wide-array Rust tests and five public closed-constant Rust tests. The 64 passing benchmark controls were run with the preceding production policy candidate; they are distinct from these final-source affected-case repetitions and full Python run. The same-VM Hail baseline is unchanged. CPU/RAM/I/O telemetry was sampled at 10 Hz; hardware cycles/instructions were unavailable.

Immutable closed nested expressions are cached lazily at the physical expression root. Empty and unselected inputs remain unevaluated; failed evaluations are not cached. Advisory batching models demanded list-containing outputs with statically known primitive repeats, preserves session context and state, and caps DataFusion filters' internal coalescers. Its 64 MiB target is advisory: a single row can exceed it, and scalar intermediates, unknown estimates, List32 limits and sources that do not honor the advice are outside the guarantee. Field lookup is indexed once per row map; the physical-plan rewrite retains DAG sharing.

Required review chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `0c8bfa4524f5342ef7f6d83a44d7604b053e5dc2` against `ac0511e1c8950a854c528938402c932c016a222f`, plus this documentation-only validation/disposition child. The reviewer cleared source, public tests and matching normal-policy resource provenance, ran no builds/tests and made no edits. Grouped-collect parallelism and SourceOrdinal ordering certificates are excluded from this approval.

## Grouped collection

Candidate `5a26d965b1b4c225696772f360194005e52248d9` is based on reviewed main `841b3ca099b917b3038635bee24c4e246787a115`. Public grouped `agg.collect` can use DataFusion partial/merge aggregation; internal MatrixTable collections explicitly retain source order, preserving column/cell alignment and duplicate-coordinate selection. Total collection retains its previous execution and ordering behavior. The public grouped order contract follows pinned Hail source `e10cc355bc8cd0de9c5f29d5182d31527898bae1`, `hail/python/hail/expr/aggregators/aggregators.py`, collect documentation around lines 504–535; [Hail's public documentation](https://www.hail.is/docs/0.2/aggregators.html#hail.expr.aggregators.collect) likewise disclaims element order.

The adjacent `grouped-collect-validation.json` records exact source/native identities and three full-input same-VM measurements: 4.312, 4.395 and 4.367 seconds, with 26.52–26.79 core-seconds and 7.05–7.37 GiB peak RSS. The earlier Vail observation was 5.882 seconds; the unchanged Hail baseline is 4.477 seconds. The current small lead does not establish confident superiority. A subsequent profile still shows wide-row copies in the terminal ordered merge during forced count; work on that consumer is separate.

The full run passed 1,856 tests with three failures caused by stale grouped-list order expectations, one skip and 19 passing subtests. Only those grouped expectations were changed to multiset comparisons; total ordering, random scalar values and other aggregate assertions remain strict. A subsequent 137-test focused run passed, including all three corrected tests, distinct nested grouped payloads with duplicates/missing values, Matrix column/entry alignment and duplicate-coordinate last-value behavior. This is not a claim of an all-green full rerun. The final public collect docstring was repackaged afterward without changing the native binary; both wheel native hashes match.

Required review chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `5a26d965b1b4c225696772f360194005e52248d9` against `841b3ca099b917b3038635bee24c4e246787a115`, plus this documentation-only validation/approval child. The reviewer cleared source, preserved internal ordering contracts, public test changes and accurate validation provenance, ran no builds/tests and made no edits. Scan certificates and forced-count consumer changes remain excluded.


## Scan ordering certificates and complete-row count execution

Candidate `ed38d5ce2ca1347163dd62655851460e38560ef1` has the production sources of `cb45f65ad4ed7b3e1db6eb694555e7c4abc8c1f9`, based on reviewed main `de90334624fac52996aafc03b7e340c0568392b8`. The adjacent `scan-count-validation.json` records six matching local/cloud source hashes, native/wheel identities and repeated measurements.

Source ordinals advertise partition ordering only with a valid partition-constant witness; mixed-tag inputs retain separate per-tag ordinal counters. Cross joins preserve the verified right-side partition constant without claiming arbitrary cross-join ordering. Transparent ordered nodes refresh their schema after rewrites; frozen nodes retain their captured contract. Grouped output is protected only after projecting its private tag, avoiding overwriting a user aggregate. Consumer distribution matching remains scoped to the same producer contract and only tolerates matching unknown-column placeholders.

Forced count still compiles and evaluates every row, projected field and transformation. It may omit only a terminal no-fetch ordering merge when the input fanout fits the bounded executor. Transparent projections and action context are rebuilt; a single-child ordered concatenation is replayed to retain normalization casts and tagging. Volatile expressions, fetched merges, multiple concatenation children and other opaque consumers retain the original path. This is not metadata counting or row/field pruning.

Native count/sum prefix summaries project raw qualified dependencies before the independent-consumer boundary. Aggregate expressions remain above the boundary and the main full-row branch is unchanged. Opaque nested kernels expose free columns/globals as explicit logical arguments, which DataFusion dependency traversal sees.

The repaired cloud suite passed 1,866 tests, one skip and 19 subtests. The subsequently uploaded scan module was explicitly rerun: 50 passed. Local Rust consumer-action and text-I/O suites passed six and two tests; cargo check passed. Earlier candidate `c2fe6757` failed 75 tests because ordered tagging overwrote the last user aggregate; those failure artifacts are retained separately and were never merged.

Three full-input scan runs took 3.662, 3.769 and 3.837 seconds, versus the unchanged same-VM Hail observation of 3.780 seconds. Peak RSS was approximately 0.203 GiB and CPU use 40–42 core-seconds; Hail used 23.27 core-seconds. Grouped collection took 4.395, 4.579 and 4.366 seconds versus Hail 4.477. These small elapsed-time differences do not establish confident superiority. Further profiling remains necessary.

All 64 previously passing upstream benchmark controls passed again. Their Vail total was 108.760 seconds versus the unchanged Hail total of 448.959 seconds. In this control run, grouped collection (4.578 versus 4.477 seconds) and Matrix row count scan (3.852 versus 3.780 seconds) remained slightly slower; the other 62 cases were faster. The repeated target measurements above remain the better description of near-parity variability. Controls archive SHA-256: `87d5532d65e42bc84c611d7fd77625b35901502d92b18e210a2ae2aab3eebc93`.

Required review chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `ed38d5ce2ca1347163dd62655851460e38560ef1` against `de90334624fac52996aafc03b7e340c0568392b8`, plus this documentation-only validation/disposition child. The reviewer cleared source, regression tests and matching resource provenance, ran no builds/tests and made no edits. Subsequent optimizations require separate review.


## Nested projection broadcast and alignment

Exact candidate `10581c1a5f28d9c463757d808b8e74bf64053c26` is based on reviewed main `03225117da391bd69f60ccd569899ecfe6d815c4`. The adjacent `projection-cache-validation.json` records eight verified source hashes, native/wheel identities and raw-result archive checksums. Data payloads remain Arrow arrays.

A post-optimizer, pre-consumer-binding rewrite caches scalar expansion only at top-level nested projection outputs. It recognizes ordinary nested literals and nullary immutable functions, plus exact typed Vail semantic kernels with empty captures, closed IR, no global references and no random-context effects. Successful deterministic results may be reused even when their operation can fail: original volatility/error-order properties remain unchanged, failures are not cached, and partial selections delegate to the original expression. Array-returning functions retain their original behavior. The first retained nonempty batch length is reused; other lengths use transient expansion. The 16 MiB bound is per expression and limits retained bytes, not aggregate or transient peak memory. Cold races can allocate duplicate temporary arrays; evaluation is outside the cache lock. Rewrites and snapshots get fresh state; immutable retained arrays may safely outlive an action.

The initial immutable-only rule did not wrap the expensive Matrix range/map expression because `StreamRange` is conservatively marked MayError. Its timing fluctuations were not attributed to the cache. Typed eligibility then produced one `ProjectionBroadcast` in the actual target plan. Three scan runs took 2.735, 1.815 and 2.500 seconds (median 2.500), compared with unchanged same-VM Hail 3.780 seconds. CPU fell from 40–42 to 27.09–27.94 core-seconds. Grouped collection remained near parity/slightly slower in the repeated runs (4.528, 4.802 and 4.538 versus Hail 4.477); further work remains.

Matrix alignment uses primitive ID slice comparisons when all relevant Arrow arrays have zero null count, including explicit all-valid bitmaps. Nullable inputs retain the existing validation and errors. Its resource effect is not isolated from the cache candidate, and no independent speedup is claimed.

The final cloud Python suite passed 1,872 tests, one skip and 19 subtests. Public projection regressions exercise nullable nested values, multiple batches/partitions, row-dependent branches, dynamic values, repeated actions, distinct global captures, selected/unselected errors and seeded per-row random context. The final 64 previously passing benchmark controls all passed and all were faster than the fixed Hail observations in that run: 107.739 seconds total versus 448.959 seconds. This single control observation does not establish a confident grouped-collection win. Public Rust collect regressions passed 3/3 after restoring the original implementation. Execution-library tests passed 45/45 and Matrix-layout tests 8/8 before the typed helper extension; the deep-graph test required `RUST_MIN_STACK=16777216`. Its default-stack failure is classified as stack-size-dependent, not claimed to be an established baseline failure. The typed helper passed cargo check.

A separate attempt to delegate grouped collect to DataFusion's grouped array accumulator passed its correctness tests but regressed to 5.503, 5.574 and 5.597 seconds and 9.27–9.50 GiB. It is preserved on `dk/collect-groups-experiment`; the final candidate's `collect_aggregate.rs` is byte-identical to reviewed main 03225117. Useful public multiset/empty-group regressions remain. Private cache-state/probe tests were removed at review; their earlier passes are not acceptance evidence.

Required review chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `10581c1a5f28d9c463757d808b8e74bf64053c26` against `03225117da391bd69f60ccd569899ecfe6d815c4`, plus this documentation-only validation/disposition child. Source, public tests and matching resource provenance were cleared. The reviewer ran no builds/tests and made no edits. Python localization and future grouped optimizations are excluded.

## Metadata-certified grouped ranges

Exact candidate `53d52d98c628fc8fef66622aff6966c0d121a575` is based on reviewed main `4885d21ce625a70fa3eb9a10783f55a91ee36f89`. `grouped-range-validation.json` records the eleven matching local/cloud source hashes, native/wheel identities, raw archive checksum and all 64 control comparisons.

Matrix localization retains direct persisted key columns where their lineage is known. A grouped-input certificate requires direct prefix-key expressions, known nonempty source bounds, strict separation of adjacent bounds, matching source identity and physical partition assignment, and the source's existing ascending/nulls-last ordering contract. Checked metadata-only conversion restores widened Call integers and equivalent string layouts before constructing typed split points. Unknown, overlapping, touching or unsupported bounds retain ordinary DataFusion planning. Exchanges and unfamiliar physical operators invalidate the proof. Initial admission can decline; an installed certificate must re-establish its contract after a rewrite or reject that replacement. It advertises independently certified ordering rather than inherited arbitrary child ordering. Execution simply forwards the original child; DataFusion performs the aggregation.

Exact native Arrow grouping types bypass redundant restoration. Final table globals are rebound only when post-extraction output expressions reference them; external globals remain bound, and unbound ordinary columns use an empty globals schema. Globals used inside reducers or initializers retain their earlier binding. Public regressions cover persisted multicolumn, null, string, struct and Call keys; duplicate/touching boundaries; aliases, filters and empty input; localization; and globals in reducer, initializer and final-result scopes.

The final cloud Python suite passed 1,876 tests, one skip and 19 subtests. A clean final combined-source Rust rerun passed 11 public grouped-range tests and three collect tests; its table-crate tree exactly matches the root candidate (`16d5ff768f46b3683a2a67e1a3caa3025cdf091f`). All 64 previously passing benchmark controls passed and were faster than the unchanged same-VM Hail observations: 110.921 seconds total versus 448.959 seconds. This total fluctuated upward from the preceding main's control pass; no aggregate-total improvement over that main is claimed.

Six full-input grouped-collection runs took 4.060, 3.830, 4.153, 3.936, 4.070 and 4.013 seconds, median 4.036, versus the fixed Hail observation of 4.477 seconds. All six beat that observation, but the roughly 1.11x margin is modest, not a repeated Hail-baseline confidence interval. CPU use was 18.77–22.38 core-seconds, median 20.735, versus Hail's 25.15. Peak RSS remained 6.21–7.64 GiB versus Hail's approximately 1.12 GiB; this is not a memory improvement. Actual-plan diagnostics show DataFusion's sorted partition-local aggregation without the hash exchange or final sort; diagnostics are explanatory evidence, not test acceptance assertions.

Intermediate range-only measurements, localization-only measurements and their logs are preserved separately. The range-only candidate still had the final globals join and sort; its timings are not attributed to the final implementation. Downloaded archive SHA-256: `a036267b79b4c6536f105d9109dfdae57a9845269a064c43abe456a4529414cd`. Local source and downloaded wheel hashes were verified against the cloud manifest. Required review chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `53d52d98c628fc8fef66622aff6966c0d121a575` against `4885d21ce625a70fa3eb9a10783f55a91ee36f89`, plus this documentation-only validation/disposition child. Source, public tests and matching runtime/resource provenance were cleared. The reviewer ran no builds/tests and made no edits. Both current and previous GCP benchmark VMs were verified `TERMINATED`; monitoring is disabled.
