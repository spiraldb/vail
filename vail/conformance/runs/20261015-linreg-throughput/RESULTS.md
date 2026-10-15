# Regression aggregation throughput investigation

Baseline package is the extracted Oct15 release wheel used for the engine before source changes: native SHA256 e60c32b38c7d1ad3bb5a8e76c7cef5f0ee7eb9857d075abf8c9476009f963357. The source-directory native extension is stale and was not used.

Exact upstream workload:75 regressions over the7.5M-row many_ints table, four predictors, via agg.array_agg. Samply500Hz captured first45 action seconds; sample is diagnostic and not a full timing. PairedGCP completed baseline: Hail11.348s, Vail170.919s (different machine from this local sample).

Main thread22546samples:42.8% include LinregAccumulator.update_batch;23.5% include make_array;23.3% include Arrow take; these inclusive percentages overlap. Leaf hotspots include memmove12.8%, f64 MutableArrayData extension9.7%, Linreg update7.8%, free_tiny5.3%. DataFusion GroupsAccumulatorAdapter appears in67.2%. The packed predictor lists require building/interleaving per-row arrays and nested gathering, followed by per-row ListArray slicing and Vec allocation inside regression.

Implemented general change: represent the fixed predictor vector as a struct of Arrow Float64 child columns. Gather/group paths can then use primitive child arrays. Accumulator borrows legacy list slices, accepts columnar struct input, and reuses one predictor scratch buffer perbatch instead of perobservation allocations. Formula, state, merge and nalgebra/statrs finalization unchanged. PublicAPI unchanged. Legacy float/list packed inputs remain accepted.

The results below supersede the initial pending-build status. Final pre-latency-fix package passed all 137 affected Python tests; real-file reader and planning/global integration tests also passed. Small-input fanout latency is being corrected separately before final acceptance.

## Columnar-predictor release (before aggregate fanout)

Native63e0bde7 equivalent numeric implementation in after-KeyBy package native2bda1452. Exact workload completed locally under Samply in29.777s,31.881coreseconds,1.173GiB peakRSS. Existing local baseline capture had not finished at45s; no exact same-laptop full baseline time yet. Do not compare GCP170.919s directly with this local timing as a measured speedup.

After profile removes the perrow allocation/list slicing hotspots. Linreg update is now32.1% self samples; Arrow primitive takes22.5%; GroupsAccumulatorAdapter10.1%; unnest5.9%. Predictor-list make_array and nested take_list no longer dominate. Scope-limited totalaggregation DataFusion fanout is being built separately to address remaining onecore execution.

## Bounded 32-worker aggregate input

After adding DataFusion BatchSplitStream at the custom source boundaries, the exact published fixture (7,500,000 rows, one source partition) completed in **5.9666 s**, **63.117 CPU s**, **10.578 average cores**, **3.746 GiB peak RSS**. These are local measurements under Samply with 10 Hz telemetry. Compared with the earlier local columnar-predictor run (29.777 s,31.881 CPU s), wall time improved about 5x while reported CPU time increased about 2x. Retired instructions were similar (437.5B before,427.7B after), as were cycles (119.7B before,121.0B after); CPU seconds alone do not establish duplicated computation. The fixture was regenerated from the same published upstream input; no Hail rerun. Do not compare this local result to the GCP Hail result as a paired speedup.

Declared diagnostic acceptance: exact upstream workload finishes within 90 action seconds, below 16 GiB RSS, preserving all aggregate results covered separately by semantic tests. Prior unbounded 32-worker input exceeded the 16 GiB RSS guard; bounded input passes. Measured multicore usage is empirical evidence from this machine, not a fixed concurrency API guarantee. No timings are inferred from plan names or explain output.

Artifacts: `after-bounded-parallel-result.json`, `after-bounded-parallel-telemetry`, `after-bounded-parallel-profile.json.gz`, and `after-bounded-parallel-hotspots.json`. Native SHA256 `c1b6985899640fb9349977ebf1319c175ae519982ec9fdec7025c06c53418679`; matched extracted frontend includes the old JSON expression boundary, so this isolates execution/batching changes rather than measuring the newer native handle frontend.
