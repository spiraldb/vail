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
