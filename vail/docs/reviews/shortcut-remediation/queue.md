# Architectural remediation queue

Status reconciled against main `471f8479b0544fd6d0f27abf1c05c1f027906594`
on 2026-10-16. The original module-audit base is `2c02020`. The 134 raw findings
in `findings.json` overlap and include unmeasured concerns; they are not 134 unique
confirmed bugs. Original read-depth coverage remains in `module-audit.json`.
Passing a compatibility suite alone does not close a complexity or ownership issue.

## Reviewed and integrated

| ID | Issue | Disposition / evidence |
| --- | --- | --- |
| S001 | Stream scans recomputed every prefix | Shared incremental native reducer state; reviewed at `71a471e1`, integrated through `dcbaac28`. Public stream API and behavior tests replace private forged IR assertions. |
| S002 | VCF export collected the matrix into Python | Bounded native sink with batch-bound field access; reviewed/integrated through `dcbaac28`. Historical memory observations remain qualified in the review record. |
| S003 | Table/list/matrix reducer algorithms were duplicated | Shared configured reducer contracts; reviewed/integrated through `dcbaac28`. Later QC adapters use the same list reducer kernels. |
| S004 | Deferred physical compilation split planning | One logical tree and explicit retained lowering context/action binding; reviewed/integrated through `dcbaac28`. Runtime costs still queued below. |
| S005 | Kernel identities/effects blocked lawful reuse | Stable complete native identities/effects and scoped RNG handling; reviewed/integrated through `dcbaac28`. Runtime DAG CSE remains open. |
| QC follow-up | Per-cell Call decode allocations and unnecessary GetField masks | Reviewed/integrated through `0cbeba43`; see `conformance/runs/20261016-sex-specific-qc/README.md` and `REVIEW.md`. Workload-specific timings do not certify universal matrix performance. |
| Prev-nonnull follow-up | Non-scalar previous-value scan used expensive window machinery | Reviewed supported-domain streaming path integrated through `471f8479`; see `conformance/runs/20261016-streaming-prev-nonnull/README.md` and `REVIEW.md`. Conservative source capability and summary replay limits remain explicit. |

The initial architecture review and final corrected validation are recorded in
`REVIEW_HANDOFF.md`, `required-review.json`, and `evidence/upstream20/`.
The final architecture run had 261 native passes; local Python had 1756 passes
and two known storage failures. Earlier upstream17/18 counts are superseded by
the corrected per-test randomness reset; interrupted upstream19 is discarded.
Those historical results do not certify later changes or close the remaining queue.

## Current storage correctness work

Branch `dk/storage-roundtrip-correctness` starts from main `471f8479`.
Both defects were reproduced with its matching native extension
`83a0b3ad91b4224cc13e00435d3bd89e75e3f5fc3a971311c458371837f23aa5`.

| ID | Defect | Status / acceptance |
| --- | --- | --- |
| ST001 | Empty nested-list storage write fails with `ListLayoutStrategy needs at least one chunk` | Writer fix validated; 15 focused public nested-collection cases pass. Full Vail native/Python validation passes; final integration review pending. Preserve empty versus missing lists at every nesting level; support ordinary batch/slice boundaries without sentinel data. |
| ST002 | A present struct whose children are all missing reads back as a missing parent | Reader fix validated; raw Vortex boundaries and focused public cases pass. Parent and child validity remain independent; full Vail native/Python validation passes, final integration review pending. |
| ST003 | Nested sliced Arrow input panics in C Stream import | Reproduces on main before storage. Published Arrow60 struct-slice correction backported to pinned59.2; matching native/public Arrow validation passes; final integration review pending. Preserve zero-copy buffers; no fixture normalization workaround. |

## Prior review findings requiring current-code audit

| ID | Concern | Status / acceptance |
| --- | --- | --- |
| A001 | Unknown partition count treated as one when restoring RNG/scan layouts | Reassess under intentional partition divergence. Do not restore unsupported partition-identity guarantees; retain correctness, deterministic random identity, pruning and actual parallel execution. |
| A002 | Logical type metadata restoration scattered through operators | Audit native migration and semantic-type preservation; serialized metadata is allowed for external Arrow/storage, not an IR transport. |
| A003 | Query memory/spill/cancellation resources fragmented across sessions/task contexts | Audit action ownership and concrete consequences; distinguish demonstrated defects from architectural risk. |
| A004 | Replay scope inferred from requested partition IDs | Audit repeated/interleaved actions, cancellation and retained run lifetime. |
| A005 | Interval bound algebra duplicated and interval candidate matching repeatedly nested | Audit pruning without file reads and output-sensitive interval algorithms. |
| A006 | Expensive compositional string/variant/bind/list APIs | Audit actual complexity and semantics; prefer existing engine kernels when suitable, typed native primitives where needed. |
| A007 | Join plan sharing mistaken for build-side execution reuse | Audit actual plan/action execution counts, not Arc identity. |
| A008 | Upstream scope excludes filesystem modules but misses Hadoop definitions in mixed modules | Audit and consolidate module+definition policy. Preserve historical reports. |
| A009 | Independent reporting/selection tools count incompatible scopes | Audit current tools, share policy/outcome schema where necessary and label evidence accurately. |

## Contracts and remaining work

Native expression/type handles and Arrow C Streams replaced serialized PyO3 IR
and row transport before this audit. External persisted formats and their metadata
remain legitimate serialization boundaries. Audit their ownership and cost model
rather than treating migration itself as a correctness certificate.

UTF-8 codepoint string semantics and intentional partition divergence remain.
Exact Hail partition counts/selection/layout are not required. Real file/segment
pruning must avoid impossible reads; persisted output must remain sorted and
key-range-disjoint across files, with truthful physical execution metadata.

Open concerns include runtime shared-DAG evaluation/CSE, repeated action/reset
work, per-batch scalar IR specialization, Arrow concatenation, segment pushdown,
lookup/join costs and tiny-query latency. Check current code and consequences
before classifying historical reports as still-present defects. Upstream missing
APIs/collection blockers and the phased-BNM fixed-golden mismatch remain explicit;
the fresh exact-main paired probe establishes that narrow mismatch predates the
architecture integration, not that all seeded consumers are Hail-equivalent.

Every new main update requires the standing exact-revision review in `AGENTS.md`,
including follow-up review after material fixes. Development-branch validation and
concrete evidence precede that gate; no subagent may update main.
