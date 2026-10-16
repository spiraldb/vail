# Required pre-main review

Reviewer: `codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`.

Reviewed candidate: `d7950ff05c2eb291b206d1868bf674e0d2f8619f` against main `0cbeba43bb3f822f16044a3ee3f4f1acc44ed267`. Final production source `dbf3e9f8d3f25dc6345430a5cbefcb57f82c2dd5`, with public test additions and matching evidence in descendants.

Final disposition received 2026-10-16:

> Final disposition: APPROVED for main integration of exact d7950ff05c2eb291b206d1868bf674e0d2f8619f against0cbeba43, productiondbf3e9f8 with reviewed public test additions. Prior blockers are resolved: state-independent canonical fold matching, conservative replay capability, one optimized partition domain, predecessor-only summaries, cancellation retry, explicit action reset, recursive canonical storage, and public blackbox tests. Matching84 Python+2 public native tests pass; exact native83a0b3ad... final three100M runs establish median5.134s with13.4–14.0CPU-seconds and1.02–1.30GiB RSS for that workload. Report correctly qualifies historical speedup, bounded baseline, different-host Hail comparison, cancellation timing and observable buffer-test limits. Approval covers this supported domain, not universal streaming coverage or removal of summary replay cost. Reviewer inspected source/evidence only, ran no builds/tests and made no edits. You may append only a documentation record quoting this disposition and --ff-only merge main through that child, provided main remains the reviewed ancestor and no further source/tests/harness/evidence changes are introduced.

Addressed findings: reject state-dependent/non-null/aliased/noncanonical folds; remove ordered concatenation of argument projections; use one optimized replay domain and restrict specialization to independent deterministic source lineages; make summaries predecessor-only and cancellation retries fresh; explicitly reset action caches; canonicalize nested view storage; use public output/action tests with appropriately limited resource claims. All reviewed blocking findings resolved. This child changes only this review record.
