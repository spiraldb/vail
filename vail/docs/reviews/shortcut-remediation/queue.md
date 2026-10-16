# Architectural remediation queue

Original audit base: `2c02020`; final integration includes main `dc51582f`. This queue separates confirmed costs/correctness defects from
unproven design concerns. A passing compatibility suite is not an acceptance
criterion by itself. Each fix needs a stated invariant, general implementation,
focused correctness/complexity or boundedness evidence, and integration review.
Module coverage is tracked in `module-audit.json`; pending modules have not yet
been audited in this pass. Individual agents record findings with file/line evidence.

## Known implementation defects: fix now

| ID | Issue | Status / owner | Required architecture |
| --- | --- | --- | --- |
| S001 | Stream aggregate-scan recomputes every prefix | Implemented; native shared scan state and native19 integration pass; follow-up review pending | Shared accumulator state; no prefix rescans for bounded-state reducers; preserve RNG and nested scopes. |
| S002 | VCF export collects the whole matrix into Python | Implemented; public file tests and isolated bounded-memory evidence pass; follow-up review pending | Native bounded batch sink with backpressure and error propagation; only header/sample metadata may be eager. |
| S003 | Matrix/list reducers duplicate table algorithms | Shared factory and native19 integration pass; follow-up review pending | One configured reducer factory/state contract adapted to row-owned lists and scans; remove per-allele repeated call scans and duplicated numerical semantics. |
| S004 | General legacy physical compiler and deferred planning bridges | Implemented and native19 validated; required follow-up review pending | One optimized logical tree; semantic extensions consume already-planned children; preserve ordering and RNG without exposing Hail partition identity as a required contract. |
| S005 | Pure kernels get fresh identities and blanket Volatile flags | Implemented and native19 validated; required follow-up review pending | Stable native identities, accurate effects, and explicit metadata/RNG boundaries; prove folding/reuse is legal. |

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

## Already resolved before this branch

- JSON expression trees and IPC row transport at the PyO3 boundary were replaced
  in `2c02020` by native expression/type handles and Arrow C Streams. This audit
  still checks their ownership and cost model; it does not assume the migration
  certifies all expression/planning paths.

## Intentional contracts

UTF-8/codepoint string semantics and the documented partition divergence remain
in force. Avoiding incompatible Hail partition assertions is not a shortcut.
Vortex pruning must avoid opening impossible segments; persisted output remains
sorted and key-range-disjoint across files. Do not silently preserve deprecated
Hail controls as a design requirement.

## Module-audit additions

The machine-readable [findings queue](findings.json) consolidates each agent report,
including evidence, confirmed versus unmeasured status, and acceptance criteria.
These are open work unless explicitly marked fixed and centrally validated.
Partial audits remain partial in the module inventory.

## Validated development historical candidate10

All five initial remediations are implemented. The final coherent candidate has
239 native tests passing across 20 binaries and 227 focused planner/matrix/scan/
randomness tests passing. The full Python suite has 1,343 passes, 2 known storage
failures, 1 skip, and 19 passing subtests (96.45 seconds). Exact logs and native/source
hashes are in `evidence/` and `required-review.json`. Passing tests do not close
the separately queued performance or architectural findings.

The two remaining baseline failures are the Vortex empty-list writer and loss
of parent validity for a present struct whose children are all missing. The
remediation's prior replay, scalar capture, type, filter, shuffle and ordering
failures are fixed in this candidate. Actual file pruning passes a deleted-
nonmatching-file test with a deleted-matching-file positive read control.

This branch has not updated main. The exact76cbbeb0 follow-up review was delivered
after user authorization and cleared most findings; R7 public-test replacements
and the reconciliation with newer main still require validation and review.
See `REVIEW_HANDOFF.md` and `required-review.json`.

The audit contains 180 reviewed modules/resources and 3 removed modules; the raw
findings records overlap and are not a count of unique confirmed defects. Known
open costs include runtime shared-DAG evaluation, repeated globals planning,
per-operand reset work, mixed-scalar capture specialization per batch, reader
segment pushdown gaps, and the tiny matrix latency recorded in the findings.

Main advanced independently to `6bce5077d98f61e49f0b7ec9241f855f493b54d3` during
this work. These results apply to base `2c02020` plus this development branch.
Reconcile the newer main and revalidate before any final merge approval. The
only production edits after historical candidate10 validation remove two extra final blank
lines; `required-review.json` lists them explicitly.

## Integration with newer main

The pending development merge includes main `6bce5077`. Earlier module-audit
coverage and historical candidate10 results do not certify these additions. Native execution
merge details are in `merge-execution-audit.json`; fresh coherent runtime evidence
and exact-revision source review are required before main is updated.

Source investigation disproved the purported RNG compatibility requirement in
newer main's indexed collection fallback: pinned Hail handles each StreamZip
input's randomness independently. The fallback preserved an accidental Vail
rebinding bug. It is removed in the integration in favor of native n-ary zip,
index-values, and one shared correlation state. See
`merge-semantic-decisions.json` for source provenance and corrected expectations.

## Concurrent follow-up outside this candidate

Sex-QC profiling identified per-cell packed-call temporary allocations and a
GetField parent-null Vec<bool> mask even for a non-null parent. The tables agent
is replacing them with existing stack-backed decoding and Arrow validity/child
borrowing on its separate development branch. These costs are not fixed or
claimed accepted by the architectural integration candidate.
