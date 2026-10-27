# Supported benchmark validator audit

Source-only audit of **112 supported cases /90 unique benchmark functions** in the recovered e10cc355 upstream sources. All9 source SHA256s match the recovered authenticated packet. No case was dropped; no query, build, VM action, or benchmark ran. Per-case source lines, hash, arguments, action calls, result usage, existing checks, proposed validator and gaps are in `case-validator-map.json`. This is a design mapping, not implemented or executed validation.

| Validator class | Cases | Existing behavior | Required observation |
|---|---:|---|---|
| COUNT_AND_SCHEMA | 39 | Force/count actions return integers | Capture exact count plus schema/input-manifest checks; count alone does not prove decoded payload |
| FILE_SEMANTIC | 8 | Writes/export sideeffects then tempcleanup | Read/reopen/parse before cleanup; logical schema/count/value digest, not compressed-byte equivalence |
| STREAM_FIELDS | 38 | Computed pipelines end in force-count | Check named fields/order/presence using closed forms or frozen shared-fixture oracle; all intended outputfields |
| SCALAR_COMPLETE | 13 | Aggregations/counts execute and results discarded | Capture entire returned scalar/tuple/array/struct; compare exact or explicit tolerant oracle |
| DISPLAY | 5 | show prints bounded output | Capture stdout/display semantics; does not validate unshown values |
| BOUNDED_RESULT | 6 | take/collect eagerly return boundedprefix | Capture complete typed returned objects; order/missing/NaNs and nested entries |
| HOST_SCALAR | 1 | CPUhash loop computes localx | Observe finalx using explicit Python/platform hash contract |
| HOST_STREAM | 1 | gzip loop drains toEOF | Track independent decompressed byte/line count+digest and successfulCRC |
| PLAN_ONLY | 1 | PythonTable construction remains lazy | Capture finalschema/query object; keep timed benchmark actionfree |

The current worker calls `fn(**kwargs)` without observing a return and records passed after the temporary directory closes. More importantly, each upstream function usually discards the action result internally; capturing fnreturn alone would still yield None. Action-boundary observation must retain the existing receiver/result/output path and validate outside the timed window before cleanup, or narrowly authored benchmark adapters must return the same observable result without replacing the timed operation. Do not add a new terminal collect/aggregate to a count benchmark and label its timings comparable.

95 cases execute public actions but do not assert their final return,8 write files without checking them,5 print show output,2 host loops discard accumulated observations,1 intentionally constructs a lazy query, and1 asserts only its force-count15. The sex-QC case does use its intermediate aggregate_cols result to build later expressions; the final pipeline is still unchecked. `many_aggs` constructs33aggregation expressions without assertions; the gVCF import helper sets GRCh38/force only. No hidden validation was found in these included helper paths.

## Concrete correctness distinctions

- Full force-count is an eager action; successful action is not evidence that transformed values, ordering, or missing-entry masks are correct. Current Vail force_count drains output batches and adds batch.num_rows; this supports execution completion, not a payload oracle. Historical exact e79 artifact execution behavior has not been re-profiled here.
- Matrix row scan and column scan require outputfield checks; `scan_count_cols_2` actually ends in force_count_rows. It cannot be validated by rowcount alone.
- Unsorted grouped range has10,001 groups, not10,000: key0 has999rows, key10000 has1row, middle keys1000rows each.
- Many-aggregation rows/cols need all33outputs, not one sample field. Filter/unfilter needs entry-struct logical validity checks distinct from missing entry fields.
- `rows_is_transition` actually computes `is_snp`; preserve actual upstream expression in the oracle.
- Range1000intervals exactly tile0..10M; union multiplicity2 and join multiplicity1 must be verified independent of physical partitioning.
- Bounded take/collect gives an entire returned result oracle but does not prove consumption beyond that bounded domain. Display is additionally truncated by columns.
- Write outputs should be reopened via public readers or parsed asVCF/TSV. Whole compressed-file hashes can differ across formats, BGZF layout, metadata, or timestamps; only canonical logical output is comparable. Cryptographic stream digest is evidence with a collision assumption, not literal element-by-element mathematical proof.

## Reusable implementation plan

1. Freeze existing benchmark source/fixtures and introduce an untimed observer at public action boundaries. Capture returned scalar/smallobjects, schema/receiver identity and output paths; do not retain whole large Arrow/rowbuffers. Preserve multiple actions perbenchmark (sexQC has an intermediate aggregation). Stop the timing/telemetry first, then validate before worker tempcleanup and report timed-success separately from validation-success.
2. Supply a canonical typed result codec: explicit signed integer widths, strings/arrays/struct field order, logical nulls, NaN/infinities/signedzero policy and ordered versus multiset semantics. Integer closed-form checks remain exact; floating tolerances must be perstatistic, not a blanket approximate-equality switch.
3. Implement complete scalar/returnedprefix and deterministic range oracles first. These are smalloutputs; use fullresult comparison. Count manifests come from immutable input data, not the count under test.
4. For38field/order pipelines, use perrow closed-form mismatch counts when available (ranges/scans/joins); for realgenomic/statistical outputs produce one shared Hail oracle on the identical frozen fixture and compare allintended fields with schema/row/entry counts plus streamed logical digest/tolerance summaries. A scalarcount alone remains a deliberately limited validator. Fullvalidation can cost another scan ormore; keep it untimed and account for it in scan ETA.
5. Parse/reopen each output before deletion, verifying keys/globals/entry masks/full logical values; use boundedbuffer streaming, not bounded-prefix hashes presented as fullfile validation.

## Unresolved contracts and blockers

- Random-sex QC must capture/check the actual generated sex assignment or use a common immutable assignment; separately initialized backends are not assumed to share RNG sequences. Existing fixturegeneration contains randomness too, so validate immutable manifest identity rather than regenerate expected values independently.
- String take ordering ties, downsample representative choices, grouped take firstrow, and unordered collect need documented equivalence classes. Exact arbitrary order equality would reject valid executions; count/schema-only would accept wrong values. These remain unresolved until contract review.
- Large100Mzero arrays and250Mdownsampling cannot be fully collected on the driver. Shape and formula validators can be distributed, but force-count alone proves neither.
- Public force-count action observation plus a postaction validator does not itself prove the timed optimizer retained every field; fullconsumption claims require the public action contract and actual execution evidence, separately.
- The intentionally lazy Python-construction case is supported as a planning benchmark. Adding a timed action changes the benchmark. Its final object/schema must be explicitly observed.
- The old worker cleans output paths before reporting passed; without a lifecycle change exported-file validation is impossible.

## Work estimate (implementation, not scan runtime)

A reusable action observer/typed smallresult codec and39count+13scalar+6bounded-result validators: roughly6–12 engineer-hours, contingent on public action interception review. Eightfile-output adapters and5display/2host/1planning observations: another4–8hours. The38computed/order pipelines and nondeterministic contracts: roughly8–20hours, with genomic/statistical oracle generation time additional and not estimated from source. These are implementation ranges, not a promised completion ETA; a broad scan should explicitly label limited versus fullvalidators until everycase passes. No tests should be silently removed to shorten this work.
