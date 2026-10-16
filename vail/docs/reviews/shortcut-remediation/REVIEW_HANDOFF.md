# Required follow-up review

Reviewer: `codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Branch: `dk/architectural-shortcut-removal`, based on `2c02020`. This branch has not updated main.

Please review the development branch's final commit before it is merged to main.
The previous review requested changes; it does not approve this candidate.
The handoff was delivered after explicit user authorization. Exact revision
`76cbbeb0cada9efc955254d9ce5eea0c3339f082` received partial source clearance:
scalar capture, scoped randomness, predicate-once filtering and R2/R3/R4/R6/R8
are addressed. R7 remains request changes for newly added private RNG-consumer
tests. Public behavior replacements are written in the pending integration;
they still require validation and follow-up review. The reconciliation with
newer main is a separate material revision and remains unapproved.

The five remediations replace stream prefix rescans, Python matrix collection
for VCF output, duplicated reducers, general physical/deferred compilation, and
per-instance pure-kernel identities/effects. Follow-up fixes cover scalar capture
ownership/cardinality, native scoped randomness rewriting, predicate-once lazy
filtering, physical schema rewrites, partition-tag corruption, and canonicalized
consumer fan-out before independent state reset. Most new private strategy/identity assertions were replaced by public outcomes;
the remaining RNG consumer assertions are being replaced in the integration.

Final coherent candidate10 evidence:

- Native library SHA256:
  `4a7eee5356b152ef64d9ff439ef5f6aca7d74aa123cb39faf7294b1f65139269`.
- All 239 native tests pass across 20 binaries.
- All 227 focused planner/matrix/scan/randomness tests pass.
- Full Python: 1,343 passed, 2 known baseline storage failures, 1 skipped, 19 passing
  subtests; 96.45 seconds. Both failures are unchanged storage defects: empty-list
  Vortex writing and loss of a present all-missing-child struct's parent validity.
- Pruning test removes impossible files and succeeds, then removes the matching
  file and requires failure. Diagnostics do not read the missing file.
- Isolated VCF sink: 100k rows / 20 samples, 169.3 MB peak RSS; 2M rows / 20 samples, 221.2 MB.
  Keyed localization/import still sorts upstream; timings were under host load.
- Pure and random shared-expression planning probes are bounded through depth 128.
  Runtime CSE remains queued. Mixed scalar captures specialize native IR per batch;
  that O(IR) cost is explicit, with no value cache or per-row list broadcast.

See `required-review.json`, `evidence/`, `module-audit.json`, and `findings.json`.
The audit has 180 reviewed modules/resources and 3 removed modules; findings
overlap and include unmeasured concerns. Several performance costs remain open,
including the measured 23 second tiny matrix workflow and storage segment pushdown.
Review functional and architectural invariants without inferring full performance
acceptance from test counts. Do not approve main until material fixes have been
reviewed against the exact development revision and its evidence.

Main advanced independently to `6bce5077d98f61e49f0b7ec9241f855f493b54d3` during
this work. These results apply to base `2c02020` plus this development branch.
Reconcile the newer main and revalidate before any final merge approval. The
only production edits after candidate10 validation remove two extra final blank
lines; `required-review.json` lists them explicitly.
