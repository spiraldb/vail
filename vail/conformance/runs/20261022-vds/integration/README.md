# VDS integration evidence

## Candidate and validation

Development source revision `2e985e086d488b1b8b334794fb2ff07941def25a` passed
134 public tests in32.98s with native SHA256
`1f13452ed3916ffddb9bafb911481abc1db2fe7e03161db36aaf69bde56e3fb7`.
Tests cover scan, metadata, optimizer contracts, VDS independent Hail golden
and local stream scans. Six new nested indexed initializer cases passed.
The integration merge with main10ac7462 is72f4c720. The column-demand fix is
cbe66195, followed by a compile-only empty-schema type disambiguation at
ac709e13. Exact compiled source revision is `ac709e13`; subsequent changes are
documentation and evidence only. Its native SHA256 is
`68254f5ee21c520edc7e3be33a2d74b496c7a41c477d2408106a7262bda8e12b`.
The merged release build succeeded; **173 public tests passed in21.74s**,
covering the earlier suites plus aligned batch maps, VDS functions and matrix
layout. No Rust cfg tests were run. Public command:

```sh
PYTHONPATH=/Users/danielking/.codex/worktrees/vds-holes-experiment/packages/integration \
/Users/danielking/projects/vail/vail/.venv/bin/python -m pytest -q --import-mode=importlib \
 vail/python/tests/test_scan.py vail/python/tests/test_arrow_type_metadata.py \
 vail/python/tests/test_kernel_optimizer_contract.py vail/python/tests/test_vds.py \
 vail/python/tests/test_stream_aggregate_scan.py vail/python/tests/test_aligned_runend_experiment.py \
 vail/python/tests/test_vds_functions.py vail/python/tests/test_matrix_layout.py
```

## Historical aligned-map strategy comparison

Independent release binaries for baseline a5415231, mutable holes045e7392,
and batch/run-end7ad31de3. One warmup, three measured actions per case,
separate processes, exact result digests checked. Measurements are shared-host
and not interleaved; peak RSS is process-wide high-water memory including
setup/warmup and cannot isolate the expression alone.

| Case | Baseline time / peak RSS | Mutable holes | Batch / run-end |
|---|---:|---:|---:|
| numeric-512x8 | 0.1906s / 325 MiB | 0.1185s / 325 MiB | 0.0633s / 324 MiB |
| nested-512x8 | 0.1786s / 329 MiB | 0.1437s / 329 MiB | 0.0952s / 329 MiB |
| nested-512x128 | 0.3448s / 439 MiB | 0.2636s / 404 MiB | 0.0869s / 484 MiB |
| nested-4096x128 | 1.7700s / 1092 MiB | 1.7757s / 1049 MiB | 0.2660s / 1765 MiB |

The 4096×128 nested workload covers524288 entry positions. Batch evaluation
substantially reduces expression invocation overhead there, at higher peak
memory. Run-end arrays are genuinely constructed but explicitly decoded once
per unique captured field per batch: Arrow59 arithmetic rejects compressed
inputs. This is not end-to-end compressed execution, and whole captured
nested row/global fields can expand. No cohort-scale memory claim is made.

The separate native-binding profile on7ad31de3 measured Python output decode
at0.0077–0.0087s per81-row VDS collection; native collection1.885/1.498/1.486s.
Those sampled diagnostic actions are not additional controlled speedups.
The final cleanup's sampled profile likewise found restoration below1%,
while planning and general carry/local-state merging remain.

## Final cleanup small VDS comparison

Equivalent native fixture values,81variant rows/five samples, collected dense
output. One warmup and three measured execution actions. Previous batch3.0359s,
cleanup1.6594s, Hail1.5411s (medians). Matching Vail result SHA256
`b38514dc33323fcadddfe7b288fbd050315d9d2609074ba20580916f711c35b0`.
These host-noisy results show a targeted improvement, not a Hail win. Hail
telemetry does not have matched CPU/RSS scope. Python planning is separate
in both harnesses; JVM startup and input reads are excluded from these actions.

## Scope and review

No public backend API added. New internal typed aggregate-key access certifies
fold-state/result equivalence; cached factory descriptors do not change scan
identity. Unsupported VDS APIs/storage remain explicitly rejected as documented
in the module README. Main review thread:
`01a14128-21b8-7a53-b399-4ad8f9b31f4f`. Exact2e985e08 received development
clearance after private hash-spy test removal and follow-up empty-state review.
Integration review requested demand-driven column capture and accurate evidence.
Source/test review cleared cbe66195; matching final runtime evidence and documentation
are submitted for final main-integration approval. The exact approval/disposition
is recorded in the main integration commit; no main update precedes that approval.
The compile-only follow-up
replaces ambiguous `vec![].into()` with `Fields::empty()` and changes no semantics.

## Column-demand follow-up resource evidence

Public fresh-process benchmark `wide_columns_benchmark.py`, identical arguments
128rows/32columns/4096payload bytes. The callback uses row data only. Baseline
72f4c720 native SHA256
`f063c9e21f0f8eb0057dd9e1cb97aebbfb030b730c1c43a55bf8f728a8718c03`;
final ac709e13 native hash above. Both produce count4096/checksum784384.
Baseline peak RSS1105477632bytes (1054MiB), final1086488576bytes (1036MiB):
about18MiB less, consistent with avoiding the16MiB per-entry text gather.
This is one fresh process per variant, not a stable timing study. Observed
action times0.787s/0.210s and CPU0.346s/0.248s are recorded without a general
speedup claim. No implementation counters/spies were used.

The larger original128×128/16KiB baseline failed with Arrow offset overflow,
so it is not a completed before/after comparison. The bounded comparison
retains substantial memory from other capture expansion; this fix does not
claim to eliminate that remaining cost.

The final merged VDS binary's separate warm three-action median is0.7632s,
CPU0.9230s, process peakRSS473MiB, matching the earlier exact dense digest.
These host-noisy values are not paired with a new Hail run and therefore
do not revise the earlier no-Hail-win conclusion.
