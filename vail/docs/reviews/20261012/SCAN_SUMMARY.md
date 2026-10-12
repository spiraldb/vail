# Additional scan changes: review and disposition

Baseline commit: `7689173d38233665fada383aaa3a6a45b8ecfc96` on `main`.
The new scan changes remain uncommitted for review.

Three GPT-6 Luna reviewers covered planning/initialization, nested scopes/state,
and API/type/error contracts. Parent review checked their claims against source,
focused reproductions, and the Hail runtime. This is sampled review, not proof of
exhaustive parity.

| Feedback | Disposition |
| --- | --- |
| P2: nested fold aliases could collide with a user field named `__vail_scan_variable_0`. | Fixed by selecting aliases absent from the evolving schema and resolving the exact recorded mapping. Regression reproduces the original failure and passes after rebuild. |
| Explicit coverage of final-partition array-length mismatches. | Both `array_sum` and `array_agg` are tested. They share the indexed-state path; the initial suggestion that they used different paths was corrected. |
| Empty configured scans must retain real output partitions. | Added a three-partition empty-input test with a globals-configured histogram, including selecting each physical partition. |
| `_localize=False` must not bypass scan rejection in total aggregation. | Added a direct regression test. |
| Initialization must be lazy and shared between concurrent partitions. | Rust tests verify no construction-time initialization, exactly one build across concurrent output partitions, singleton cardinality, and schema/partition-count checks. An additional Python-level initialization counter is optional coverage. |
| Statistics suffer large-offset cancellation. | Hail runtime confirms its statistics use the same unstable formula. A stable-variance experiment was dropped to preserve the requested semantics. Floating reduction order can still affect results across engines. |

No confirmed defect remains in the reviewed additional scan changes. Earlier
storage, tuple-validation, Arrow encoding, and `head` issues from the broad review
were outside this scan work and are not claimed fixed.

Detailed reviews: [planning](scans-planning.md), [scopes](scans-scopes.md),
[contracts](scans-contract.md).

Final validation: **695 Python tests passed, 1 skipped, 19 subtests passed;
109 Rust tests passed; 9 targeted upstream scan cases passed**. See the
[saved conformance record](../../../conformance/runs/20261012-complete-scans/SUMMARY.md).

Subsequent P1 follow-up: the user requested fixing the numerical cancellation and tuple-validation findings. Stable population standard deviation is now implemented for total, grouped, and scan aggregation, and its intentional numerical difference from Hail is documented. See [the review follow-up](SUMMARY.md#p1-follow-up). The validation counts above describe the earlier scan snapshot.
