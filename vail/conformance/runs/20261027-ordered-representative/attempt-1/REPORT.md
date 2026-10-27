# H091 one controlled duplicate-conversion profile — complete, no feature recommendation

The new duration-driven monitor completed its one requested3second CPU sample successfully. All complete4096×4matrix outputs matched the independent last-index/payload formula. The actual sample is dominated by sorting/grouping/take work, while reported ArrayAgg state/evaluate frames are small and cannot be assigned uniquely to the redundant duplicate collect rather than genuine later cell collections. **Defer H091 implementation on this diagnostic**: the source pattern remains a valid bounded-state hypothesis, but this run does not establish material isolated collect-last CPU or allocated/retained-byte cost. It does not prove the issue is universally solved or negligible. No LastValue code, ordinal rewrite, additional profile/query variant, rescale or benchmark comparison was attempted.

## Original controlled setup and full correctness

Exact reviewed6177c16a5, normalLinuxnative4761/prod517/maina35-equivalent,69frontend and all configuration/script assets passed. Public262144row/16partition native source generation, writes, domain/range/sum/schema/filehash checks were outside profile/action timing; setup1.919411s. Original generated fixture lock is copied into `raw/fixture-lock.json` and preserved in archive, including all file hashes/schema and source counts. This fixes the off-tree receipt omission from the earlier H085 attempt without recovering or rerunning it.

Source mapping has4096row groups×4columns×16duplicate rounds,64-byte payload suffix. The actual query explicitly sorts by source_index before conversion; that sorting cost is intentionally included. Every one of the4096rows and four column keys is checked, and every entry's value equals `r+4096*c+15*16384` with exact string payload. Full outputs are retained for confirmation and all ten sample-process actions, not checksums/truncated previews. The four global column keys are attached to localized rows and checked there; its evaluation/copying is part of this public workload, not duplicate-only state cost. This controlled repeated-coordinate source is not a real cohort or Hail comparison.

Confirmation action0.462496s wall/0.760539core-seconds. Ten sample-process actions range approximately0.3606–0.4353seconds wall/0.5592–0.7088CPU. These repetitions in one process are descriptive, not independent trials or a speedup comparison. Action timing includes public collect plus Python validation/sort/encoding; stdout serialization follows action timing but remains in the process/profile. CPU/RSS are not isolated native-aggregate work or allocated-byte counts.

## One valid bounded-duration sample

First action completed at monotonic55.708761489. Popen-before observation55.742743139; sampler exit0 observed58.990521348, within t0+5 (3.281760seconds aftert0). Immediate launch closure58.990523108. Query decision/final exit0, sampler decision/final exit0, complete action stream. Ten actions completed, stdout18,259,429bytes below64MiB. The fixed successful perf command requests cpu-clock:u99Hz DWARF with sleep3; these host observations are not exact kernel recording endpoints. Perf reports **395 samples, zero lost**,4.932MB recorded. This is low-resolution attribution, not a precise percentage estimate or confidence interval.

| Sampled symbol/family | Inclusive % | Flat % | Interpretation |
|---|---:|---:|---|
| SortExec async root | 27.85 | 0.00 | Includes explicit source sort and other pipeline sorts; overlapping wrappers |
| Arrow take | 14.68 | 0.76 | Multiple necessary movements; not all removable |
| GroupsAccumulatorAdapter update | 12.91 | 1.27 | Multiple grouped reductions, not isolated duplicate collect |
| GroupsAccumulatorAdapter merge | 12.15 | 0.00 | Overlaps descendants |
| TakeAccumulator merge | 8.86 | 0.00 | Existing first row/column metadata reductions; out of scope |
| Arrow concat | 2.03 | 0.00 | May include genuine later collections |
| ArrayAggGroups state/evaluate | 1.01 each | 0.00 | Overlapping; cannot uniquely bind to duplicate or needed later collect |
| Python EvalFrame | see full report | 4.56 | Python output/validation included |

Inclusive percentages overlap and must not be summed as recoverable time. Async/unresolved stacks and one sampled CPU process limit attribution. No isolated allocation/retention measurement, superseded-owner release proof, or native LastValue order/null/emptyStruct/view equivalence is established. A source-level O(duplicates) state pattern does not justify a performance acceptance without the remaining proof. Do not modify necessary later_CELLS collect or metadata take merely because it appears in this profile. Broader H031/H091 remain open/deferred.

## Cleanup and original receipts

Positive setup/confirmation/query/sampler/command process-group cleanup; captured guest4→temporary2→restored/verified4. Monitor max processRSS620,572,672bytes includes runtime/query/output work, not allocator bytes. Existing6GiBRSS/10saction/60sprocess/5slaunch+sample deadline/16spostwarm/64MiBfullstdout/10GiBfreefloor honored; no fixed action-count exhaustion. Provider1800sSTOP verified beforeboot, guest28minute guard. Original archive downloaded/hash verified, proactiveSTOP and provider independently described **TERMINATED**, host error:null/scope_verified:true.

`host/receipts.tar.gz` contains original perf.data and all guest receipts. `raw/` omits only duplicate uncompressed perf.data; original bytes remain archived. SHA256.json authenticates all originals and this report. This successful diagnostic validates this execution of the new monitor, not unexecuted failure/signal/LinuxRSS paths. No additional execution or production change is requested.
