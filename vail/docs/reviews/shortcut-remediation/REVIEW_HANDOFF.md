# Required final integration review

Reviewer: `codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Branch: `dk/architectural-shortcut-removal`. Code revision: `658f789a`.
Integration base: `6bce5077d98f61e49f0b7ec9241f855f493b54d3`.
Main parent: `dc51582fcb2413be6f14372ea8887a4cabc85f88` (reviewed Python Call
predicates merged cleanly; native17 Rust unchanged).
Main has not been updated. The final review request names the complete committed
candidate, including this evidence; earlier partial clearance does not approve it.

The five remediations replace stream prefix rescans, Python matrix collection
for VCF output, duplicated reducers, deferred physical compilation, and unstable
kernel identities/effects. Logical lowering now shares an explicit action-local
LoweringContext; runtime operators consume prepared inputs. Action templates
bind scalar globals afresh for each action. Consumer replay is certified through
known stateless wrappers only; exchange/unknown subtrees remain guarded.
Cancellation releases shared production only after the last owner leaves.

Earlier source review at `76cbbeb0` cleared scalar capture ownership/cardinality,
scoped native RNG, predicate-once filtering, and R2/R3/R4/R6/R8. R7 requested public
behavior tests. The new consumer tests check independent/interleaved actions,
partial cancellation and later successful execution; private topology/Weak/counter
assertions were removed. Material main-reconciliation changes require fresh review.

## Material reconciliation decisions

- Native free-parameter analysis proves singleton expressions closed before
  globals hoisting; nested lexical callbacks remain in their binding scope.
- Shared lowering memo fixes recursive duplicate planning in a tiny matrix
  union/entries workflow. The regression completes, but still takes about 26 seconds;
  this is not a broad latency acceptance claim.
- Duplicate scan windows share declared state, summary and carry; scalar CSE
  remains enabled. Stateful output itself is not guessed replayable.
- CallStats empty AF is wholly missing, agg.all ignores missing, and zip/enumerate
  preserve each input RNG stream without adding an output-index rebinding.
  `merge-semantic-decisions.json` cites pinned upstream Hail source for these
  decisions. Tests retain public values/types, independent correlation, nested
  RNG, truncation demand, missingness and repeated-action checks.
- The maintained DataFusion 55 patch preserves empty-struct cardinality/validity
  during scalar compaction. Only four source lines differ from the published
  crate; see `patches/datafusion-common-vail.patch`. Vendor source is not claimed
  freshly audited. Native VCF batch formatter binds fields once and publishes
  atomically without Python matrix collection.
- Upstream adaptation omits only proven standalone structural-IR helper calls,
  with lexical guards and a source-span manifest. Behavioral assertions remain.
  Adapter tests verify unrelated/shadowed helpers and real failures still execute.

## Coherent build 17 evidence

Native SHA256: `8853c655d212de27bfc8f1894395a1860563a956243c49a248fb8fbf3a827416`.
Final package18 overlays only the two reviewed Python source files on the exact
native17 wheel; all Rust source hashes remain unchanged. All 260 native tests pass across 41 binaries,
including consumer replay/cancellation, scan replay, pruning and metadata.
Full local Python plus conformance adapter suite: 1742 passed, 2 known baseline
storage failures, 1 skipped, 19 passing subtests, 245.74 seconds under concurrent suite load.
Failures remain empty-list Vortex writing and present all-missing-child struct
validity on storage roundtrip. Tests were not weakened to hide them.

Stream sweep 1024→65536 elements: 5.5→60.9 ms, peak RSS 314→332 MB. Keyed head over
2^31−1 source rows takes 2–3 ms; field sweep 65→1025 takes 9.7→126 ms. These are bounded
workload observations, not a throughput guarantee for all operators. Historical
candidate 10 VCF RSS measurements remain historical and are labelled as such.
Final full-upstream results and scope are in `evidence/upstream18/`.
Historical pre-Call results are separately retained in `evidence/upstream17/`.

## Limits and remaining queue

`queue.md`, `findings.json`, `module-audit.json`, and `merge-execution-audit.json`
record overlapping findings and actual read depth; 134 records are not 134 unique
confirmed bugs. Open costs include runtime DAG CSE, per-batch mixed-scalar IR
specialization, Arrow concatenation, segment-level pushdown, lookup/join costs,
and slow tiny matrix workflows. Separate byte-aware batching and sex-QC work
is not included. Existing storage failures are unresolved.

Intentional breaks remain UTF-8 codepoint strings and no exact Hail partition
layout contract. Pruning/file avoidance and correct native physical distribution
metadata remain required. Full upstream excludes typecheck-decorator and Hadoop /
Hail FS tests, while expression type tests remain included.

## Final upstream18 outcome

All 56 eligible modules completed, with no timeout or native panic observed.
Of 1468 definitions: 453 passed, 617 failed/setup-blocked, 398 collection-blocked.
Parameter cases: 480 passed, 1212 nonpassing. Dedicated randomness module: 6/6.
Specifically randomness: 58 passed, 2 blocked by missing BlockMatrix.random and
import_bgen, 38 collection-blocked. All 44 matrix randomness behavioral tests pass.
These are observed passes, not a claim that collection-blocked tests pass.
The first-blocker categories and raw-log hashes are in `evidence/upstream18/`.

The broader random-data classification also retains the known phased
balding_nichols_model fixed-golden mismatch (phased behavior/repeatability tests
are separate), partition-layout differences, and worker-error compatibility gaps.
This merge does not claim complete upstream randomness compatibility.
