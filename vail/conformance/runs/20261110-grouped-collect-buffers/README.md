# Grouped collect: avoid copying nested payload into partial states

Reviewed production revision: `79744c0316884b341ed04cc2f02d77fb6bd3d7ba`.
Normal release wheel native SHA-256: `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193`.
Baseline: local main `757457cff611f36c1bb10e76119a45ae590c6196`, normal wheel native `f5fbcd3f360979ee824b70699fff10ab609c0016dca32dadcbbc3329474be79e`.
Both use mimalloc, identical Python code and unchanged inputs. No cloud workloads.

DataFusion's plain `array_agg` retains input buffers during updates but can recursively copy nested data when emitting a combined partial-state array. `VailCollectStateExec` instead calls DataFusion's existing `GroupsAccumulator::convert_to_state` kernels once per input batch: singleton list states borrow the payload. Existing repartitioning and final aggregation still merge duplicate keys and construct results. This removes the avoidable partial gather; it does not remove every copy or introduce a new grouping algorithm.

The physical optimizer admits only the migrated, unordered partial-hash route, one ordinary grouping set with nonempty keys, and exclusively native non-DISTINCT/non-ordered `array_agg` aggregates with at least one nested list payload. Sorted aggregation, global aggregation, mixed aggregate functions, and migration-disabled sessions retain their existing route. Evaluators/accumulators are prepared once per execution, not per value. Output properties conservatively claim neither unique keys nor grouping/order distribution. Aggregate argument/filter evaluation order matches the admitted DataFusion route, and child replacement checks the exact prepared schema.

One state row per input can increase keys, offsets, and exchange work versus collapsed groups. Final grouping of independent owners can still need a copy. Duplicate-heavy behavior must remain a performance consideration; this is not a universal collection speedup or a memory bound.

## Local measurements

Median of per-process medians, three timed full actions after one full warmup per fresh process. Fixed Rayon 4, BLAS/OpenMP 1; Hail uses Spark `local[4]`. Vail's existing Tokio pool is unchanged. No builds, tests, or profiles overlapped timing. Exact file rosters/content hashes verified before and after both cohorts. Original benchmark actions end in `_force_count`; these timings are execution checks, not cross-engine value conformance.

| Workload | Baseline wall → candidate | Wall change | Baseline core-s → candidate | Core-s change |
|---|---:|---:|---:|---:|
| Grouped collect, full Hail input | 0.36340 → 0.36397 s | **+0.16%** | 3.77786 → 3.32799 | **−11.91%** |
| Duplicate-key nested collect control | 0.38736 → 0.38992 s | +0.66% | 4.22962 → 4.16602 | −1.50% |
| Grouped collect, native subset | 0.04017 → 0.04056 s | +0.98% | 0.13207 → 0.12949 | −1.95% |
| Grouped take/rekey control | 2.39552 → 2.44028 s | **+1.87%** | 2.88448 → 2.95622 | **+2.49%** |

The first balanced cohort used 18 workers/54 timed actions: Hail,B,C,C,B,H for the primary case and B,C,C,B for the controls. Its primary wall change was +2.26%, CPU −11.36%; grouped take was +3.45% wall/+3.47% CPU. Because that control was adversely affected, a reverse-order C,B,B,C block repeated only primary collect and grouped take (8 workers/24 timed actions). All original results remain included. The table combines four fresh processes per Vail version for those two cases; the other controls and Hail each have two. All four candidate primary process CPU medians are below all four baseline medians. Combining cohorts changes the median nonlinearly; neither individual cohort nor the control has been subtracted from the reported result. The grouped-take regression remains unexplained and is accepted explicitly alongside the targeted CPU improvement, not dismissed as noise.

The Hail input is the existing 250,000 × 1,000, 32-partition gnomad-DP fixture. The duplicate control changes only the original grouped-take key expression's aggregation to `collect`, retaining nested payload, approximately 50 source rows per key. The native control uses the existing 1,000 × 2,535, four-partition native fixture; it is not the full Hail dataset or a native/Hail input speed comparison.

In the balanced first cohort's fresh Hail anchor, Hail was 0.47850 s / 2.68913 core-s versus candidate Vail 0.38482 s / 3.31905 core-s: Vail was about 1.24× faster in wall time but still used about 23% more CPU. These are small local cohorts, with appreciable Hail process variation; this change does not establish CPU parity with Hail or update the full benchmark suite. Hail CPU includes stable observed live descendant endpoint deltas, which may omit unobserved short-lived children.

## Correctness and profile

The final normal build passed 20 Rust tests and 65 Python aggregate tests. Public multi-batch coverage includes duplicate/null keys, empty batches, nested null/empty lists, multiple outputs, multiple partitions, and repeated execution. Existing filtered/empty-group and globals tests also passed. Initial API compile failures and the superseded first wheel are retained in `/private/tmp/vail-collect-state-20261110`; they are not final validation.

One separate samply capture ran the actual original Hail-input grouped-collect action after timing: one full warmup then 17 actions over 6.06 s, 56.37 process core-s. Sampled stacks contain `convert_to_state`; no partial aggregate state-gather stack was observed. Source and the admitted route establish why that partial gather is absent; missing samples alone would not prove absence. Arrow interleave still appears (two positive CPU samples), so this is not a claim that final/exchange copies are impossible. The remaining CPU stacks are dominated by the Hail decoder's `BufferedScalars::probe` and `prepared_scalar_row`. Inclusive stack percentages overlap and approximate interval attribution; no kernel-time percentage is inferred from a broad execution-node name match.

The normal Firefox-compatible profile and symbol sidecar are saved under:
`/Users/danielking/.codex/visualizations/2026/10/23/01a1666a-5c91-7043-b5a8-da5a82f930bd/grouped-collect-20261110/`.
Their hashes are in `manifest.json`. This profile is diagnostic, not a timing sample.

A harness file initially named `profile.py` shadowed Python's standard module during Hail import and launched an unintended candidate capture. That attempt failed before valid Hail timings, its owned group was verified absent, and its logs/capture are retained under `/private/tmp/vail-collect-state-20261110/final/failed-harness-import`. The file was renamed `record.py` before all reported measurements. The failed capture is excluded from profile conclusions.

`bench-results.json` and `repeat-results.json` retain all raw timed samples and owned-process cleanup receipts. `combined-results.json` contains the exact table and both cohort summaries. Input rosters and package identities are in `bench-config.json`; normal build and gate identities/commands are in `ready.json`, `rust-final.json`, and `python-gates.json`. Full build logs remain in the temporary task directory. Every measured worker exited successfully and its owned process group was absent before the next worker.

Independent reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared exact source `79744c031`, corrected an earlier finding that had referenced the wrong DataFusion aggregate stream, and accepted the measured CPU/latency tradeoff. Final evidence review and integration receipt are recorded separately when complete. No GitHub PR, remote push, or cloud VM operation is part of this task.
