# H047 actual current public regression path — one diagnostic

The fixed real-data regression runs through ordinary DataFusion partial hash aggregation and the native **generic** LinregGroupsAccumulator, not the anticipated shared-predictor coordinate path. CPU stacks show PartialHashAggregateStream → LinregGroupsAccumulator::update_batch/update_layout → factor_linreg_flat → Householder application. No query was changed to force a different path.

The current scratch allocation is measurable but modest: `MemBuffer::new` accounts for **1.91% inclusive / 0.08% flat** samples. One visible caller chain is factor_linreg_flat → MemBuffer::new (1.81%) → Rust alloc (1.73%) → posix_memalign (1.71%). These nested percentages overlap. There is no allocated-byte count or observed dynamic StackReq/alignment measurement, and the whole 9.91% factor function is not an allocation opportunity. Source-proposed q_coeff/shared QR workspace is not established as this workload's dominant bottleneck. Do not implement the originally anticipated shared-predictor workspace on this evidence. A root decision is needed whether the modest generic scratch setup warrants a separately scoped change; no change is recommended by default ahead of the larger data movement.

| Actual sampled symbol | Inclusive % | Flat % |
|---|---:|---:|
| DataFusion UnnestStream::poll_next | 38.74 | 0.50 |
| LinregGroupsAccumulator::update_layout | 18.59 | 6.62 |
| factor_linreg_flat | 9.91 | 4.88 |
| MemBuffer::new | 1.91 | 0.08 |
| predictor_row | 1.79 | 1.79 |
| Arrow take_list UInt64/Int32 | see full report | 8.24 |
| Arrow take_bytes UInt64 | 3.99 | 2.08 |

Unnest/take movement is the larger visible general opportunity; this diagnostic does not authorize expanding H089, H051, H012 or changing a query/operator. Inclusive entries are overlapping attribution, not additive recoverable time. Some Rust inline/async call roots remain unresolved; lack of a symbol above reporting thresholds does not prove its absence. We cannot infer the share-predictors flag's admission failure reason merely from the source/public expression; the actual generic path is the evidence.

## Full outputs and scope

Exactly one confirmation and one sample process ran, with the unchanged DP-zero-imputation model. Complete 2,535 distinct column keys and all ten fit fields were retained for every action. None, NaN, +Inf, -Inf and signed finite zero have distinct lossless representations; finite values are stored as hex. Sample actions compare full domain/shape/classes and finite values with relative1e-8/absolute1e-10 tolerance. The observed fits are real-data consistency results, not an independent numerical oracle. The separate tiny8×3 public API smoke has independent analytic beta validation.

Confirmation action: **4.271028s wall / 23.109476 core-seconds**, process peak1,430,888,448 bytes. Sample process actions: warm1.915085s/31.374841CPU, then1.449631s/29.883140CPU,1.388838s/29.458517CPU,1.415279s/28.536922CPU. Sampling/control overhead and warm caches differ; these are diagnostics, not independent throughput trials or a comparison. Monitored sample-process RSS max2,373,877,760 bytes includes fixture/query construction, retained runtime state, result extraction, Python validation, sorting, full serialization and every action; no allocation or scratch-capacity claim follows from it.

The timed action includes collect(), Python full-fit validation, encoding and sorting; stdout JSON serialization occurs after its wall/core measurements but remains inside the profile's process/window. No individual Python/JSON symbol exceeds the flat report's0.5% threshold; this is not proof of zero Python work. Do not assign all sample-process CPU or RSS to regression math. The initial process/import/preparation work is outside action timing; normal-user whole-process CPU sampling can include boundary work.

## Provenance, limits and cleanup

Exact executable08080c6177b5089cc5b3e32f3b59280ee0946b4d; native4761b8ec4e70325e922bb330987ef87c7a9f50bfba4f93f9a69c0c761337244c, accepted production517/main2d. Six script/configuration asset hashes plus packaged ASSETS manifest,69frontend hashes, original fixture metadata and DPInt32 preflight passed. No build, install, query variant, retry, hardware perf event or production change.

CPU-clock:u99Hz DWARF one3second recording: **6,193 samples, zero reported lost samples**. Four actions in sample process,4.299seconds from first to last completion,4.520seconds sampler elapsed including final action/cleanup; requested record is exactly sleep3. Budgets60sexternal/10saction/6GiBRSS/10GiBdisk/6spostfirst/12actions unchanged. Original perf_event_paranoid4 captured, temporarily2, restored and verified4. Confirmation/query/sampler/privileged commands all have positive owned-group cleanup. Download archive SHA verified; host result error:null/scope_verified:true, proactive providerSTOP and independently described **TERMINATED**. Provider1800sSTOP verified before boot, guest28minute shutdown guard. Initial SSH connection refusal during boot preceded any command/query and only boot readiness was retried, not the workload.

`host/receipts.tar.gz` contains the exact original perf.data (49.969MB uncompressed) and all guest receipts. `raw/` is the original unpacked evidence excluding only that duplicate perf.data. SHA256.json authenticates all packaged files. No further VM or implementation requested; all resources released.
