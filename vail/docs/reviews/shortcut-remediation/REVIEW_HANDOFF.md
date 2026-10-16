# Final integration review record

Reviewer: `codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Branch: `dk/architectural-shortcut-removal`. Production/follow-up code: `de8ce111`.
Main parent: `dc51582fcb2413be6f14372ea8887a4cabc85f88`.
Review approved exact `71a471e17dda5f7f246f8d69da6a27675b86f4f5` on 2026-10-16.
Main at review was dc51582f. The reviewer authorized a documentation-only child
recording this disposition and fast-forwarding main through that child.

## Architecture and material reconciliation

The five fixes provide incremental shared-state stream scans, a bounded native
VCF sink, configured reducers shared by table/list/matrix adapters, one logical
planning tree with prepared physical inputs, and stable native IR identity/effects.
Logical lowering shares an explicit retained action-local LoweringContext;
scalar globals bind afresh per action. Singleton hoisting proves lexical closure.
Consumer cancellation/replay is certified through known stateless wrappers,
including join ordering; exchange and unknown outputs remain guarded.

Input-preserving zip/enumerate RNG, mathematical paired correlation, missing-aware
agg.all and wholly missing CallStats AF at AN=0 follow the pinned upstream source
cited in `merge-semantic-decisions.json`. Tests cover nested scope, repeated actions,
truncation demand and error/missingness behavior. The maintained four-line
DataFusion scalar-compaction patch preserves empty-struct cardinality/validity;
see `patches/datafusion-common-vail.patch`. No vendor-wide audit is claimed.

## Review findings addressed after b2f97e7e

- Both shared scan-window registries use HashMap keys containing the full unaliased
  DataFusion expression and ordinary equality for collision resolution. Declaration
  order is preserved. Public wide-scan regression covers 64 distinct arguments
  and eight repeated aliases in native and general-state paths.
- Stream tests use documented public stream_range/aggregate_scan APIs. The forged
  private IR assertion was replaced with public misuse and nested typed results.
- Structural-helper adaptation conservatively detects exception bindings, match
  captures, deletion and other lexical rebinding. Public regressions require real
  errors/behavior to remain executable. The pinned 97 omission spans are unchanged.
- The original upstream autouse randomness reset is restored through public
  reset_global_randomness before every test. Full pytest and original-body variants
  preserve reset-before-fixtures behavior, including fixture caching. Tests execute
  independent tests in both orders and parameter variants with dependent fixtures.
- Fresh exact-main/candidate paired phased-BNM evidence establishes the existing
  fixed-golden mismatch for identical public inputs/resets. Both native libraries
  return the same 25 phased genotypes and repeat them exactly; both differ from
  Hail at index 6. This remains an unresolved compatibility defect, not an intentional
  break. See `evidence/phased-bnm-pair/` for full outputs, source hashes and build log.

Earlier source review at 76cbbeb0 cleared scalar capture/cardinality, scoped native
RNG, predicate-once filtering and R2/R3/R4/R6/R8. Original private consumer topology
and lifetime tests were replaced by public interleaved/independent/cancelled-action
outcomes. The material integration and follow-up received exact final approval at 71a471e1.

## Final validation

Native19 SHA256: `2abcd90ae79d3c94f42c2d8205946a8c0ce9d731242754a5e01a4204bbace383`.
All 261 native tests pass across 41 binaries. Full local20: 1756 passed, two known
baseline storage failures, one skipped, 19 passing subtests (238.72 seconds under
host load). Failures remain empty-list Vortex writing and loss of parent validity
for a present struct with all missing children. They were not hidden or weakened.
All 37 final adapter/harness tests pass, including the latest original-body-only
fixture-cache follow-up. Full local20 collected before that isolated test edit;
its final behavior is covered by the focused run. Production Python/native and the
full upstream pytest runner were unchanged after full20 launch.

Corrected full upstream20 results are recorded in `evidence/upstream20/` and
`evidence/final-validation-summary.json`. Upstream17/18 are superseded historical
observations because their root adapter omitted per-test randomness reset.
Interrupted upstream19 is discarded. Their counts are not final acceptance.

Stream19 sweep 1024→65536: 10.5→243 ms, peak RSS 317→338 MB under concurrent native
compilation. Native17's historical lower-load sweep was 5.5→60.9 ms. These are
bounded workload observations, not universal latency/throughput acceptance.
Tiny matrix workflows remain slow; one earlier focused run took about 26 seconds.

## Scope and limits

The audit and queue include overlapping findings and actual read-depth boundaries;
134 records are not 134 unique confirmed bugs. Runtime DAG CSE, per-batch scalar
IR specialization, Arrow concatenation, segment-level pushdown, lookup/join costs,
and tiny-query latency remain open. Separate sex-QC allocation and byte-aware
batching work is not included. Existing storage defects remain unresolved.

Intentional differences are UTF-8 codepoint strings and no exact Hail execution
partition-layout contract. Real pruning/file avoidance and valid physical metadata
remain required. Full upstream excludes typecheck-decorator and Hadoop/Hail FS
cases; expression type tests remain. Collection/setup blockers, missing sampling
APIs and the phased-BNM exact-seed defect do not count as passing randomness tests.

## Corrected upstream20 outcome

All 56 eligible modules completed: 453 definitions passed, 617 failed/setup-blocked,
398 collection-blocked. Parameter cases: 480 passed, 1212 nonpassing. No timeout,
native panic or unreached case was observed. Dedicated randomness: 6/6 pass.
Specifically randomness: 58 pass, two missing APIs (BlockMatrix.random/import_bgen),
38 collection-blocked. All 44 matrix randomness behavioral tests pass. These are
observations under the restored reset fixture, not inferred from historical runs.
The phased-BNM fixed-golden assertion remains executable and failing.

## Final disposition

The required reviewer approved exact 71a471e1 after the b2f97e7e review and this
follow-up. Both specialist passes cleared all remaining blocking findings.
Approval covers integration, with the documented compatibility, performance and
storage limits retained; it does not certify general upstream equivalence or
latency/throughput acceptance. The reviewer ran no builds/tests and made no edits.
The only subsequent change is this authorized documentation record; source, tests,
harness and evidence remain exactly as reviewed. See `required-review.json`.
