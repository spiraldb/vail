# H045 scalar correlation bitmap count: review candidate

Production source:9a527ced, based on reviewed main669af5b0. Development branch dk/correlation-bitmap-count. Status: **unmerged; resource acceptance pending.** This scoped change does not close grouped correlation or input cast work in H045.

The public matrix query is `mt.aggregate_entries(agg.corr(mt.DP,mt.GQ))` over the real63,083,475-entry native fixture `/private/tmp/entries-profile25.mt`. All observed outputs are exactly0.7536333631956934.

The previous CPU-clock cloud profile attributed241/243 exclusive CorrAccumulator update samples to its duplicate scalar validity-count loop, approximately2.33%of totalCPU. The committed annotation documents exact PCs. This candidate keeps the checked UInt64 pair count and native six-field correlation state protocol. It uses cached Arrow null counts for zero/one validity bitmap, and Arrow shifted word iterators plus AND/popcount for two bitmaps. Parent validity is already incorporated by corr_pair. No allocation, per-pair configuration, widened child, bespoke SIMD, math/state change, or grouped change is introduced.

## Correctness and provenance

Both variants were built normally with kache, using the same stable checkout/target, RUSTFLAGS='-C target-cpu=native', CARGO_INCREMENTAL=0,jobs2. Normal wheels were installed into separate environments; no extension copying. InterpreterPython3.12.12. Baseline wheel build1m08s; candidate2m27s. The required Rust package/test graph compiled in8m06s; this graph should be reused for subsequent execution-only tests rather than creating further variants.

- Baseline main669 native SHA256:9ee229e8adb724fb3d06a676eaae5f407f07dd43668badc5824820021a52db8b.
- Candidate9a native SHA256:ab991ed6abd678de4fecd9d787e1b4810beac853532d91efc64e0ba73e7b30bc.
- Public Python numeric/correlation tests:63passed baseline;63passed candidate.
- Public Rust UDAF integration suites:17passed (numeric_aggregates7;coordinate_reducer_states10). The new public test executes240 combinations of independent offsets0/1/7/63/64/65,lengths0/1/63/64/65/129,child masks and parent absence. It checks explicit expected correlation/empty/degenerate results and exact UInt64 count-state boundaries: MAX−expected accepts the batch,MAX−expected+1 rejects. Existing mixed/grouped/state/repeated-evaluation/full-UInt64 tests remain.

## First bounded resource result (retained)

Fresh-process ABBA,6queries perprocess, discard first2; report median of4samples perprocess then median of2process medians. OPENBLAS_NUM_THREADS=1 and OMP_NUM_THREADS=1 in both variants. Builds and tests had ended before recording.

| Metric | Baseline | Candidate | Interpretation |
|---|---:|---:|---|
| wall |305.03ms|327.26ms|7.3%higher, unresolved |
| processCPU |3.47495s|3.28449s|5.5%lower |

Individual process wall medians were baseline288/322ms and candidate294/361ms. The run is too small/noisy to establish a wall improvement; the observed wall regression is retained and prevents claiming this as an accepted performance win. No additional timing windows were selected. CPU medians consistently favored the candidate, but the baseline cloud2.33%CPU ceiling does not predict this different machine's exact speedup.

Two subsequent3second macOS software samples show exclusive CorrAccumulator update leaf counts257→13. These are raw samples from all threads, including waiting threads; **they are not normalized CPU percentages** and are not hardware utilization counters. Representative optimized AArch64 assembly is included: baseline counts individual bits; candidate emits word population counts via compiler-generated `cnt.8b`/`addv.8b`. This verifies the intended scalar validity traversal was replaced, using ordinary Arrow/Rust code. No cache/bandwidth/ALU saturation claim is made. Sampled-run timings are not folded into the ABBA result.

The target, source checkout and timing slot were released immediately after recordings to the ArrayLength agent. No VM was restarted. Frozen raw logs, per-process samples, profiles and assembly are under receipts/. Root/reviewer should decide whether this modest CPU improvement with unresolved wall behavior meets the resource gate; no main update has been made.


## Final predeclared controlled trial

Protocol was frozen at7302323a before observing results. Artifacts/source did not change. After ArrayLength released exclusive compute, one final trial ran8A/B pairs and4interspersed same-artifact A/A pairs, balanced/seeded randomized order.24fresh processes each executed20queries (4warmups+16steady),480queries total. Primary latency was the arithmetic mean of16steady queries/process; compare paired geometric candidate/baseline ratios. All native hashes and all480exact outputs were asserted. No build/test/profile overlap, outlier removal, selected windows, further trials, or source tuning. Raw process records, system metadata, predetermined ordering, complete block summaries and seeded paired-bootstrap calculations are in receipts/controlled/.

| Primary metric | Baseline arithmetic process average | Candidate | Paired candidate/baseline ratio |95%paired-bootstrap interval |
|---|---:|---:|---:|---:|
| mean wall |325.41ms|316.40ms|0.9722|0.9552–0.9900 |
| mean processCPU |3.72166s|3.60928s|0.9699|0.9613–0.9801 |

Seven of eight A/B pairs favored the candidate. This longer trial supports a **modest2.8%latency and3.0%CPU reduction on this workload**, rather than the first short run's wall regression. The first run remains above and in receipts. Intervals characterize this paired trial, not all hardware/pipelines.

Same-native A/A controls expose remaining process variation: nominal B/A mean-wall ratio1.0101 (interval0.9816–1.0411), CPU0.9939 (0.9904–0.9974). The small same-artifact CPU bias is retained; no post-hoc subtraction/calibration was applied. No further trial will be run. Final resource acceptance remains with root/reviewer, and source is unmerged until that decision. Compute was immediately released back to ArrayLength before report packaging.
