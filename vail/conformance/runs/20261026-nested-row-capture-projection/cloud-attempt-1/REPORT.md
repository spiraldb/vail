# H012 matched Linux validation — attempt 1

The narrow ordinary-Struct ROW capture projection reduces the projected workload's wall time by 5.31% and core-seconds by 5.88% in this run. All four adjacent balanced projected pairs improve. The whole/full-capture control is 2.71% slower in wall time (three of four pairs adverse), with core-seconds 0.16% higher. The scalar control is mixed: median wall time is 0.74% lower and core-seconds 1.42% higher. These adverse controls remain unresolved; this report does not attribute them to noise or claim universal improvement.

| Consumed workload | Baseline wall s | Candidate wall s | Baseline/candidate | Baseline core-s | Candidate core-s |
|---|---:|---:|---:|---:|---:|
| Projected nested ROW capture | 0.534776 | 0.506382 | 1.0561 | 13.203144 | 12.427377 |
| Entire ROW capture/full payload | 0.835293 | 0.857911 | 0.9736 | 21.435905 | 21.470421 |
| Scalar ROW capture | 0.284797 | 0.282697 | 1.0074 | 7.089047 | 7.190009 |

Each table cell is the median of four independent process medians. Each process has four timed observations/query and three warmups, with the frozen forward/reverse query order. There are four independent processes/version, not sixteen independent trials. Process order is B,C,C,B,B,C,C,B. Per-pair projected wall speedups are 1.0548, 1.0416, 1.0830, and 1.0435; core speedups are 1.0607, 1.0642, 1.0825, and 1.0486. All raw actions and adverse control pairs are retained.

Median whole-process peak RSS is 1,165,561,856 baseline and 1,165,051,904 candidate bytes (effectively flat). This includes construction, all three workloads and their warmups, and cannot be attributed to a particular query or used as an allocation/owner-release claim. Action wall/core timings exclude construction and warmup; monitoring overhead is not subtracted. No source I/O or decode pruning claim is made.

## Exact gates and provenance

- Executable protocol `8d76d30a`, source-only validation `d5868382`; unchanged attempt with no retry or expanded workload.
- Baseline production `6a4ea2b44a3ecd50cae85f80116b9e7a04a5a9dc`, normal Linux native SHA256 `ec71f970260859c2510490dc8d48d48538d45bad1b364887a60f60f1479e2b63`.
- Candidate production `517c87538d58689e864af795015020e908de0be0`, normal Linux native SHA256 `4761b8ec4e70325e922bb330987ef87c7a9f50bfba4f93f9a69c0c761337244c`; wheel SHA256 `0cfbb10d3ef971a1f36bf1f911dedeb2187f96c2d171c550216ca1c903637816`.
- Complete 2,101 source files and 18 protocol assets verified. Normal build 99.29s; no binary transplant. Full source/frontends/input/thread settings and commands are retained in artifact-lock and command receipts.
- Baseline counterfactual: six existing Rust tests pass, the new allocation test fails as expected, one pre-existing benchmark remains ignored. Protected source restoration verifies all candidate hashes before building candidate. The candidate has **7 Rust PASS + 1 pre-existing ignored**, **446 Python PASS** before any timing.
- Public row-summary reducers plus Python arithmetic/Unicode-length fold independently produce the full expected results. Input schema/range/Int32 premises pass. Projected/scalar: `{total: 1536178152, present: 57513340}`; whole/full additionally `{payload: 1766337300, small: 317757180}`. Every action verifies the complete result. This is a separate query/derivation, not a separately implemented file decoder.

## Post-candidate attribution

Only after all eight timing processes completed, one candidate confirmation and one 3-second, 99 Hz normal-user `cpu-clock:u` DWARF sample ran. It recorded 7,133 samples, zero reported lost samples. Eight sample-process actions completed; the sampler began after its first successful action. Original guest perf permission 4 was changed temporarily to 2 and restored/verified to 4. All owned process groups and confirmation cleanup are positively verified clean.

Candidate `evaluate_flat` is 55.19% inclusive, REE `run_end_encoded_cast` 11.87% inclusive / 1.74% flat, and `take_list` 9.60% inclusive / 2.24% flat. The prior baseline profile had an unused-string `take_bytes` chain through REE cast into `evaluate_flat` (2.04% flat / 3.87% inclusive). No `take_bytes` symbol appears in the candidate flat report at its 0.5% threshold or inclusive report at its 1% threshold. This supports removal of that sampled unused-string copy path, together with the source and public allocation counterfactual; it is not proof of literally zero samples/copies below thresholds. Remaining list/cast work includes required selected-array reconstruction and is not all removable. Inclusive percentages overlap and are not additive, and the earlier baseline profile is attribution evidence rather than a paired throughput trial. The original raw candidate `perf.data` is preserved inside `host/receipts.tar.gz`.

## Cleanup and receipts

Provider STOP bound 2,700 seconds was verified before boot, with a 43-minute guest shutdown guard. The host verified receipt archive SHA256, restored permissions/all groups clean, proactively stopped the VM, and verified **TERMINATED**. `host/result.json` has `error: null`, `scope_verified: true`. No further VM work or rerun is requested by this report.

`SHA256.json` authenticates original host receipts, original unpacked text/JSON/XML receipts and the original archive. `raw/` omits only the uncompressed duplicate perf.data; its original bytes remain in that archive. `derived-summary.json` is arithmetic derived from the immutable comparison summary. Production remains unmerged pending independent review and root's resource-tradeoff decision.
