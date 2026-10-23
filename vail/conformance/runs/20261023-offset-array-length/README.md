# Direct outer-list lengths — 2026-10-23

User requested specializing Hail list length to read Arrow offsets directly and produce Int32 in one pass everywhere. Exact baseline main691b0da0 already contains local CSE and packed coordinate counts. Candidate routes all3 production array_length_udf sites to one semantic immutable UDF, including nested aggregation position generation. List values are never sliced or inspected; nested arrays return outer length. Validity is shared. Scalar input stays scalar. Fixed-size and large lists also supported, with checked Int32 range.

Normal Rust1.98+kache builds in isolated worktree targets; baseline normal env /tmp/packed-count-baseline-env, candidate /tmp/shared-annotation-env. No manual native copying. Reuse exact /tmp/entries-profile25.mt fixture, query from original upstream many_aggs (33 named reductions,2535cols), EXECUTION_CONCURRENCY=32. experiment.py captures query wall/self-process CPU/lifetime peakRSS and native hash. Alternating fresh processes3runs/variant; profiles separate200Hz including startup. Ambient laptop activity uncontrolled; observed min/max are descriptive, not confidence intervals. Profile categories overlap when inclusive. Tests use public UDF/Arrow API and Hail expressions.

## Results

Three fresh processes per variant, alternating baseline/candidate. Baseline wall median1.2000s (1.1828–1.3350); candidate0.8199s (0.8072–0.8270), a31.7% reduction. CPU13.0274core-s (13.0014–13.0830)→8.1399 (8.0128–8.1720), a37.5% reduction. Lifetime peakRSS median7.974→6.222GB (−22.0%); this includes fixture loading and is not query-only peak. See pairs.json/summary.json. Do not compare wall changes directly with earlier runs under different ambient laptop conditions. No new Hail comparison is claimed here.

Separate200Hz profiles sampled13.1225→8.5067core-s including startup/planning. The former DataFusion general_array_length stack decreases4.7897inclusive sampled core-s→zero samples. No child slicing/dimension filling/outputcast path remains for Hail array length. Zero samples are not a guarantee across every workload; inclusive categories overlap. Updated script includes the new module separately.

Validation:199Python public expression/integration tests and7native public tests pass. Native contracts cover sliced/nullable/empty/nested, List/LargeList/FixedSizeList, corresponding scalar forms and checkedInt32boundary. Python tests include set/dictionary lengths through list storage, physical mapped expressions, logical projections, nested aggregations and repeated actions. All3former production array_length_udf call sites replaced; nested range retains explicit final Int64 conversion because range requires it. Output lengths are Int32 everywhere.

Original and symbolized profiles, weighted stacks, full build/test logs, independent N3 raw receipts, driver and source/native provenance retained. Reviewer source-clear805fcbef plus scalar refinement9a6a360e; final evidence/integration receipts follow separately.

