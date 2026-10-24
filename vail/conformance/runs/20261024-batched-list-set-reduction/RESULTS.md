# Conservative batched row-set reduction gate

Candidate production a5f0ca7b2194fbf2782672a9af857ac06e13e26e is based on main ca59e075706a76f83c94310508f2b50fb9ebdd5a. It selects ordinary DataFusion array_distinct once at query preparation for recursively eligible ordered, nonfloating logical types, preserving the declared Set label. Other types retain the existing path. No custom kernel or SIMD is introduced. Both normal wheel artifacts were built in the same stable checkout/target with identical release/native/kache settings and installed through uv into independent environments; see provenance.json and build logs.

Both the baseline and candidate passed all 420 public Python cases, including 15 new independently specified set semantics cases. There was no additional Rust feature-graph build: this change only lowers expressions to an existing DataFusion function. The broad gate includes the previously merged native reduction, ordering, storage, projection and coordinate surfaces. Per-list Call collection is an explicitly preserved preexisting unsupported UInt16 planning capability, not newly implemented by this change.

## Bounded resource trial

Fixed seed 30020261024, balanced shuffled AB/BA process order, 24 independent processes, 304 total queries. DP has eight pairs, 16 queries/process with four warmups; the ordered-struct and unchanged-floating controls each have two pairs. No build, tests, profiling or other owned runtime workload overlapped these measurements. All queries and all process peaks are retained, with no outlier selection. Primary statistic is paired process mean latency/CPU; the DP bootstrap interval resamples the eight process pairs, not individual correlated queries. Two-pair control intervals have little inferential strength and are descriptive only.

| Public query | Baseline mean wall | Candidate mean wall | Candidate/baseline wall | CPU seconds baseline → candidate | Process peak RSS baseline → candidate |
|---|---:|---:|---:|---:|---:|
| Row set of DP, real 63M-entry matrix | 236.46 ms | 219.63 ms | 0.9289 | 2.6830 → 2.4803 | 1.5667 → 1.5656 GB |
| Row set of ordered struct(value, column), 4096 × 2500 cells | 2.3687 s | 0.1837 s | 0.07753 | 2.6310 → 0.4417 | 1.0670 → 1.0934 GB |
| Floating DP fallback control | 240.38 ms | 238.27 ms | 0.9912 | 2.7845 → 2.7817 | 1.5375 → 1.5624 GB |

DP paired wall ratio 95% bootstrap interval [0.9241, 0.9337]; CPU ratio 0.9245, interval [0.9207, 0.9288]. Process peak RSS is unchanged within observed variability (paired ratio 0.9999, interval [0.9609, 1.0361]). Floating fallback CPU ratio 0.9990; no meaningful control speedup is claimed.

The high-cardinality struct control deliberately includes the column index in the value to exercise distinct ordered structural values. Its 9,306,112 present entries are independently verified with baseline aggregate_entries(agg.count()); the matrix contains filtered cells. It improves wall by about 12.9x and CPU by about 6.0x, but costs 26.4 MB / 2.5% higher mean process peak RSS on this fixture. This cost is disclosed, not called a memory optimization. RSS includes the whole process and cannot establish live allocation counts, bytes moved, or a universal capacity bound. DF still encodes a whole visible child batch; unusually large batches remain a resource limitation requiring normal query batch sizing.

## Actual profile confirmation

Separate three-second software samples on the DP pipeline show DistinctArrayAggAccumulator, ListAggregate and SetAccumulator in the baseline, all absent in the candidate; general_array_distinct appears only in the candidate. RowConverter remains as expected, now inside the batch library kernel. Frame-presence counts are string occurrences, not normalized CPU shares. Sampled timings are separate from the resource trial. No PMU, arithmetic saturation, SIMD utilization or memory-bandwidth claim is made.

## Harness corrections, retained without repeating measurements

The first resource driver used a row name `s` that collides with the real fixture's column key. It failed before executing any query; the traceback is retained. The corrected field is row_distinct_values. After all eight DP pairs completed, the first wide candidate process returned 9,306,112 rather than the authored expected 10,240,000. The latter incorrectly counted filtered cells. An independent baseline aggregate_entries(agg.count()) returned exactly 9,306,112. Only that oracle was corrected, and the driver resumed using every already-recorded process, including the first wide candidate process; no DP pair or candidate result was discarded or rerun. The original mismatch log and protocol are retained.

## Scope remaining

H003 remains partial: collect identity was previously merged; this candidate addresses eligible per-list collect_as_set. Floating/opaque/unordered fallback, generic take/ordered reducers, broader initializer preparation and descriptor-preservation gaps remain open. Do not claim the entire H003 backlog is closed. Source/test review was clear before building; final resource/evidence acceptance and main merge belong to the parent reviewer.
