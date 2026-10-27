# H051 current baseline confirmation and one CPU sample — SOURCE ONLY

No production implementation, build, installation, VM start or query is authorized by this document. Root must authorize execution after review. The clarified design is `609e5d83`; preparation binds schema/index contracts, while each arrived batch binds its actual child lengths, masks and offset owners. Safe Struct constructors can scan logical null masks; there is no metadata-only complexity claim.

## Exact existing inputs and executable assets

`expected.json` pins current main c3f3155e, compiled production 00a8d7d7, normally installed Linux native SHA 6cb1fe232e4b352e1f41caf92530445ec58cfa364ddbdf93c58a7c667cf051ec, all 69 frontend files and three original fixture metadata hashes. Production diffs between compiled revision and main are empty (conformance documents excluded). This is the accepted normal H089 candidate environment, not a newly built current-revision artifact. No binary transfer or artifact fabrication occurs.

`ASSETS.json` lists every executable file plus existing interpreter, tools, frontend and fixture dependencies. Upload this entire directory, excluding __pycache__, and verify the manifest; run.py independently verifies each script/expected hash before query. No external test-list/source assets are needed. Metadata records 24,885 rows and 2,535 columns; this protocol does not scan to count dimensions. GQ must be present as Int32; absence stops without substitution.

## Single public pipeline

The worker reads the original native matrix, then computes `t = m.entries().select('GQ')` and fully consumes the three-result aggregate: sum GQ, count defined GQ, count GQ >= 20. It deliberately does not select entries/rows before entries(). The frozen full result `{total:1246497536, present:57513340, high:24865771}` is a previously observed complete baseline result, **not an independent oracle**. The earlier equivalent pure projection's output is retained in the native-entry-projection evidence. There is no DP/AD query, variant, fixture rewrite or broad benchmark scan.

One confirmation process prints the physical plan as a diagnostic, not a correctness oracle. It stops if MatrixEntriesByKeyRunsExec is absent; do not alter metadata or force a private path. Any upstream vail_matrix_take is recorded separately: projection cannot undo copies that producer already performs. One full successful action is required before sampling begins in a separate process. Constructor/plan/result failure stops the experiment.

## Fixed resource and cleanup protocol

Root first verifies exclusive primary VM `vail-bench-20261021`, 32 CPUs, no competing workload, provider maxRun <=30 minutes with STOP and a guest shutdown guard. host.py only accepts an already running bounded VM; it never starts one. BLAS/OMP/MKL/NUMEXPR threads=1, Rayon/Tokio=32; no PYTHONPATH; workers verify settings, native/frontend/input hashes.

Confirmation has external60s, per-action10s, live process RSS6GiB, disk floor10GiB. Sampling has those same bounds, at most12 full actions and a hard6s after its first successful action. Only then one normal-user perf cpu-clock:u,99Hz,DWARF sample attaches to that PID for3s. Successful perf completion prevents launching further actions. If12 actions finish before sampling completes, or any action exceeds the post-warmup window, stop and retain failure; do not lengthen/retry. Total guest supervisor alarm180s and host remote budget300s bound setup/cleanup. No comparative speedup estimate is intended.

Perf access is a separate explicit reversible guest permission experiment: require noninteractive sudo -n first; capture original paranoid=4, temporarily set2, normal-user perf only, finally restore captured4 and verify. No sudo-perf, install, persistent configuration or broader events. Commands/query/sampler own separate process groups; TERM3s then KILL3s checks surviving descendants, with unresolved groups a retained failure. Outer finally runs on exceptions/signals and verifies restoration; unresolved privileged cleanup requires immediate VM STOP, never a success claim.

Host finally packs/downloads/hashes all receipts even after failure and proactively STOPs the VM and verifies TERMINATED. It never suppresses STOP on a failed download. Example after root GO and complete bundle upload: `python3 host.py --remote-protocol /home/danielking/h051-profile-20261026/protocol --remote-out /home/danielking/h051-profile-20261026/run-1 --local-out /private/tmp/h051-profile-run-1`. Paths must be fresh. No retries or expanded budgets are authorized.

## Interpretation and next decision

Retain full outputs, plan, process wall/core-seconds, process RSS (construction/warmups included), raw sample, flat and inclusive reports and restoration/STOP receipts. Check sample counts/loss/unwind quality before attribution. Inclusive stacks overlap and cannot be added. Distinguish producer copies from entry-operator concat/spill/take; borrowed full-owner retention from bytes copied; selected nullable payload behavior from unused sibling copying. This baseline cannot prove future allocation/spill savings, source IO savings, SIMD utilization, or general speedup. If the intended path or a valid sample is not observed, report that result and stop; implementation remains a separate root decision.
