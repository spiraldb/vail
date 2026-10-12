# Scan planning review

Compared the scan-related changes against baseline `7689173d38233665fada383aaa3a6a45b8ecfc96`. Reviewed the lazy initializer and its physical execution node, scan lowering and partition restoration, nested scope UDAFs and Arrow state, scan projections, and the scan tests. The in-progress `aggregates.rs` stddev changes and CDF/random behavior are outside this review.

## Findings

No concrete correctness defect found in the reviewed scope.

The configuration path builds a singleton globals projection and only evaluates it when the initialized scan is executed. `OnceCell::get_or_try_init` shares that initialization across output partitions; the initialized plan is checked against template schema and partition count. Scan lowering carries DataFusion aggregate state across non-empty partition summaries, then restores the source partition layout. Nested scopes encode their state in Arrow structs/lists and reuse DataFusion accumulators and `GroupValues`/`GroupsAccumulatorAdapter`. The inclusive summary window is consumed as a finisher argument, so its aggregate is evaluated even though returned values use the exclusive carry.

## Coverage notes

- `test_scan_array_sum_rejects_mismatched_lengths` exercises the indexed-state shape validation path because `array_sum` delegates to `array_agg(sum, value)`. The test now checks both aliases directly, with the mismatch in the final non-empty partition. A separate Python-level test with the name `scan.array_agg` could still make this coverage easier to discover, but the behavior is covered.
- `initialized_scan.rs` tests lazy initialization and one-time construction with two concurrent partitions at the generic physical-node level. An optional Python-level check that a globals-configured scan initializes once under concurrent partition execution would cover the integration from TableIR configuration capture through Arrow values into the built plan.
- Resolved during review: `test_configured_scan_preserves_empty_execution_partitions` now checks an entirely empty input with a configured reducer and three retained partitions. A direct empty-input probe also returned no rows and retained all three partitions.

## Focused verification

- `./.venv/bin/python -m pytest -q python/tests/test_scan.py -k 'result_projection or array_sum_rejects or restores_empty or fold_initializer or grouped_counter' --disable-warnings --maxfail=1` — 5 passed.
- Direct empty-input probe: three partitions retained; output was empty.
- Direct `scan.array_agg` shape probe with the mismatched array in the second/final partition raised the expected identical-length error.
