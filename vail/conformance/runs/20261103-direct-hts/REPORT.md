# Direct Rust HTS throughput experiment

The experimental direct operator exceeds 2× for the measured real split workload, including collecting every output Arrow buffer. It is disabled by default (`VAIL_EXPERIMENT_DIRECT_HTS=1` enables it), remains in the isolated development worktree, and is not a generic optimizer implementation or production admission.

## Final artifact 11c

Apple M3 Max laptop; persisted first 200 profile variants × 2,535 samples; output 204 rows × 2,535 cells (517,140 slots). Normal release/kache build. Same binary and inputs with the direct flag off/on. Each workload uses B,D,D,B fresh processes, seven warm samples per process; reported values are medians of the two process medians. Rayon=4, OMP/OpenBLAS=1, nice=10; these settings do not limit the entire process to four threads. No own builds, tests, or profiles overlapped timings.

| Workload | Baseline wall | Direct wall | Wall speedup | Baseline core-seconds | Direct core-seconds | CPU speedup |
|---|---:|---:|---:|---:|---:|---:|
| Original split force-count | 219.83 ms | 90.45 ms | 2.430× | .38522 | .15801 | 2.438× |
| Internal core Arrow output collection | 222.35 ms | 90.91 ms | 2.446× | .38815 | .15889 | 2.443× |
| Public localize_entries + Arrow collection | 225.95 ms | 95.77 ms | 2.359× | .39180 | .16364 | 2.394× |
| Additional numeric-entry checksum aggregation | 443.13 ms | 282.58 ms | 1.568× | 2.30960 | 2.05809 | 1.122× |

Action-only Arrow collection, excluding Python query construction, is 186.39→71.09 ms (2.622×). The checksum action computes additional aggregates over all GT/AD/PL/GQ/DP values; its cost and weaker gains are retained separately.

All timed Arrow outputs equal the saved baseline table, with comparisons outside timing. Public-output samples check schema/field metadata too. A separately persisted supplemental equality receipt covers both boundaries. The internal baseline and direct tables can report 21,136,305 versus 20,877,733 Arrow bytes while equality passes, reflecting physical layout differences; no memory-saving inference is made. The public API benchmark uses `localize_entries(...).to_arrow()`; the core sink is a separate private diagnostic boundary. This is a warm laptop subset, not a full-dataset rerun or a new comparison against Hail.

Unrelated empty/short/wide list controls change by +0.58%/−2.50%/+1.93% in wall time and +0.74%/−2.77%/+1.79% CPU (positive means slower). No statistical/noise explanation is asserted. Earlier 11b generated unchanged/moving/mixed controls and high-allele/absent-field checksums are preserved separately; their timings are not transferred to 11c.

## What executes

The original plan prepares all-alternate nested AD/PL arrays, carries them through entry expansion, selects the desired alternate arrays, and separately downcodes calls and computes GQ. The direct plan keeps the original cell owners and selected normalized variant descriptors, performs shared numerical reductions once per original entry, and writes final typed GT/PGT/AD/PL/GQ buffers. Unchanged fields are gathered for final output occurrences. There is no per-entry evaluator, schema, function, or builder setup.

AD totals and PL bucket values use reusable primitive scratch, not intermediate Arrow lists. PL preparation retains the baseline O(n²) all-alternate algorithm. Original normalization, unchanged-key checking, moved-row sorting, and ordered union remain. Both branch reads remain; an empty selected-allele branch does no genotype reductions.

The outer result still traverses generic DataFusion Unnest. DF55's `unnest_list_array` constructs take indices over result children, so this prototype is not proof of optimal copy count or universal zero-copy execution.

## Causal branch experiment

The same 11c binary was tested with a Python harness that moves branch filtering after direct genotype production, preserving output values. Process-median ABBA results:

- Early allele selection: 90.47 ms wall / .15787 core-seconds.
- Eager genotypes before branch filtering: 95.17 ms / .20740 core-seconds.

Early selection removes 4.9% wall and 23.9% CPU relative to eager production. It does not explain the whole 2.44× wall gain; the larger gain persists in the fused direct dataflow.

A separate, untimed diagnostic records eight actual kernel invocations (2.2–5.9 ms each) and eight empty-branch invocations (7–20 µs). Invocation elapsed times may overlap; their sum is neither query wall time nor CPU time.

## Correctness and limitations

Final 11c passes 27 existing split tests and 21 additional complete-output comparisons covering 2/3/8 alleles, haploid/phased/missing calls, no GT/PL, DP-only entries, filtered entries, and zero samples. The original twelve full-output cases retain SHA256 `5b0b24d3e636fe2e2fc82106f5544b645c51a7d3ca7726ff2912e755449b5fee`. Three malformed-call error expectations differ; the user explicitly permits relaxing buggy-data validation, but this remains a documented experimental difference. First 11b also failed ten empty-entry-Struct cases; those failures are retained and corrected in 11c using an explicit Struct length.

This is not an admitted memory design: output and scratch cover a whole source batch, absent fields still cause avoidable builder work, staging masks use Vec<bool>, conservative List32 child bounds reject some theoretically representable outputs, and arbitrary passthrough nested take retains its allocation/ownership behavior. No RSS, I/O savings, all-query improvement, or faster-than-Hail claim follows.

## General optimizer target

The required transformation crosses relational expansion and array-expression boundaries:

1. Push row/allele-only selection before expensive entry transforms where effects permit. Keep original row/entry owners and parent/allele coordinates until the consumer demands values.
2. Compose nested map/field-selection/index consumers into one prepared entry program. Evaluate shared pure reductions per original entry into primitive scratch; do not materialize all-alternate Arrow lists just to select them again.
3. Let a fused list-flatmap destination writer append final output fields directly in output order, in bounded batches. Schema/field paths, callback bindings, kernel dispatch and builders are prepared outside entry loops. Borrow ordinary inputs and gather passthrough fields once for final occurrences.
4. General single-list Unnest flattening can bypass identity take where contiguous child ranges/null rules prove equivalence. This is a further generic copy-elimination opportunity, not yet implemented here.

Earlier indexed-input fusion removed original-owner gathers but still retained staged Arrow producers/consumers; its end-to-end results did not reach 2×. Therefore the next rule must fuse production into the output writer, rather than merely substitute indexed getters. Existing general DataFusion planning/order/effect machinery should remain responsible for legal placement; the experimental HTS function is the performance reference, not a hardcoded benchmark-recognition rule.

Before general admission: demonstrate this shape on independent nested flatmap/list workloads; retain whole/opaque/effect fallbacks, nullable/sliced/hidden-span semantics and query-local binding; bound output and scratch separately; run small-list/scalar/absent-field/high-allele controls; obtain exact source/evidence review. This report does not establish production admission.

## Receipts

`direct-matched-11c/summary.json` and `direct-public-matched-11c/summary.json` contain per-process medians and all raw JSONL observations; `direct-ablation-11c/` contains the causal comparison. `source-provenance.json`, `working-source.patch`, and `new-sources.tar.gz` freeze the source. Earlier 11b observations, failed/successful test logs, complete small-output oracles, normal build log, harnesses, and kernel diagnostic logs are retained alongside this report.
