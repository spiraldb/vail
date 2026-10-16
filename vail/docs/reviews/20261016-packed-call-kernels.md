# Packed Call kernels review and validation

Code revision: `f2eda3c04b4eed8cc844219a8d0100ec2cee1bcb`, comprising packed kernels
`f3adcb3c3039917c81803720947c0a0b8dac3aeb` and the storage-projection correction.
Base: `036c7a7f675fe855358d9951fda77431d6489ed8`.

## Implementation

Call predicates, ploidy and alternate-allele counts operate on the existing
packed Int32 Arrow buffer. Common Calls share a 512-byte property table; larger
payloads use exact integer arithmetic. Validation checks canonical ploidy/payload
bits without decoding then repacking every Call. Missing parents and invalid
present words retain their error/missingness behavior. Array outputs allocate per
batch; there are no per-Call or per-allele heap allocations. Word access shares
the input value buffer. The obsolete Python arithmetic/CASE lowering is removed.

General entry-presence guards remain to preserve lazy errors for absent entries.
Nested storage dependency paths stop at opaque logical values instead of sending
virtual Call accessor names to Vortex. Root metadata is parsed once per requested
root; indexed top-level and nested fields avoid repeated linear scans.

## Review findings addressed

Required reviewer: [reviewer](codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f).

- Replaced private flag-layout assertions with public expression lowering and
  execution tests against allele-based expected results.
- Reproduced a stored Call accessor failing projection pushdown, fixed typed
  storage paths, and added public Table/MatrixTable write/read regressions.
- Indexed root metadata and field lookup to remove quadratic projection planning.
- Resolved the pilot's apparent entry-filter regression with ten measured balanced
  AB/BA pairs. The initial interference-affected batch remains preserved and is
  excluded from the confirmation results below.

Final disposition: **APPROVED** on 2026-10-16 for exact code revision
`f2eda3c04b4eed8cc844219a8d0100ec2cee1bcb` against base `036c7a7f`.
The reviewer authorized a documentation-only child containing this record and
fast-forward integration through that child if main remained the reviewed base.
All source and performance findings were resolved. The reviewer independently
reviewed source and evidence, ran no builds/tests, and made no edits.

## Correctness and allocation validation

249 targeted Python tests passed, covering Call properties, malformed signed
words, slices and parent missingness, stored nested Calls, matrix predicate
laziness/layout, Table/MatrixTable operations, Arrow metadata, storage, intervals,
tuples, and projection. Three public Rust integration tests passed, including
small words and all triangular boundaries through 32767 for both phase states.

Packed predicates allocate five times per batch at both 64 and 8192 Calls;
word access allocates three times and shares the input buffer. The manual native
throughput probe measured common mixed phased/unphased/missing inputs at
1.616 ns/Call for is_het, 1.613 for is_hom_var and 1.318 for n_alt_alleles.
These figures do not characterize the uncommon large-payload fallback.

## Paired benchmark confirmation

Same laptop, Python environment, unchanged upstream benchmark bodies and fixture
(24,885 rows by 2,535 samples). Fresh processes, balanced alternating variant
order, warmup pair excluded. Observed background host load was recorded; these
are loaded-host comparisons, not an idle-host claim. No OS daemon was modified,
and Hail was not rerun.

| Workload | Measured pairs | Baseline wall | Candidate wall | Baseline CPU | Candidate CPU |
|---|---:|---:|---:|---:|---:|
| Sex-specific QC | 2 | 11.735526 s | 1.886235 s | 32.048345 core-s | 5.228241 core-s |
| Entry filtering | 10 | 1.081256 s | 1.074084 s | 2.877655 core-s | 2.872201 core-s |

QC paired wall ratios (candidate/baseline) were 0.162457 and 0.158995. Entry-filter
median paired wall ratio was 0.994618 (bootstrap 95% interval 0.988752–0.997601);
CPU ratio was 0.998351 (0.991940–1.003078). The earlier 18% control difference did
not recur. The QC sample is deliberately small; its individual pairs are reported
rather than claiming a precise population confidence interval.

Candidate native SHA256:
`060203c90b3a68e3f1701c1d5e057a09fd6b2d65230a3808d9d0fd640c735d9c`.
Baseline native SHA256:
`c601aee1b1cc02ab70ec08a5d74c21f432fed367cb3a8d62f3d0286875867deb`.

Raw logs, source/native/input identities, 10 Hz CPU/RSS telemetry, background
snapshots and reports are retained locally under
`vail/conformance/runs/20261016-packed-call-kernels/`, particularly
`entry-filter-pairs/`, `qc-confirmation-pairs/`, and `qc-profile/`.
A separate profiled QC run took 1.875860 s / 4.951834 core-s; its timing is excluded
from paired medians. No worker self samples were in the old Call decode/repack
functions or CASE merge. Copying and list-offset handling now dominate.
