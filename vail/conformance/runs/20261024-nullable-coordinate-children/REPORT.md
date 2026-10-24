# Nullable coordinate child preparation

Frozen source/test `795cf99ada749e237c1b11b7ea0241e838d9de1b` (production c2d06e02), base scalar-broadcast main4bd968e8. Normal installed candidate native SHA256 `f1f4afe9873ad4a7a869e848938d6704b361427655c37648ee8126adf62ead6a`; immutable baseline `9b577e3cca73600e204258c3ce47ce3f98054ba6125c785a743c61fab2fc0bc3`. Both match their normally built wheel members. Source fingerprints, wheel hashes, paths and changed-source touching are recorded. Experimental Mean source was fully restored before compilation. Normal16job native release build took1m15s; no binary copies. This isolated base excludes later encoded pushdown/session reuse; no additive speedup claim is made.

Both coordinate single and bundle adapters previously materialized an Arrow array slice per defined owning list and concatenated all children when any outer list was missing. The new common helper keeps one existing prepared reducer invocation per batch. After existing equal-width validation, checked width×defined-count equaling the physical child span proves null lists hide no payload: borrow that span directly, even for fragmented masks. Otherwise Arrow MutableArrayData binds the original child once and copies coalesced defined ranges in stable order. No per-list ArrayRef/header, gap-sized bitmap, selected-index vector or per-run reducer preparation is introduced. Arbitrary hidden children are omitted. Range-copy fallback still copies selected payload; views/dictionaries can retain owners, so this does not promise compact ownership.

## Correctness

Unchanged baseline:11new public cases pass. Exact normal candidate:46focused cases pass; full broader gate378passed,0failed. Cases include fragmented zero spans, hidden65537-element invalid gaps, nonzero sliced offsets, Int32/Int64/Float32/Float64, single and bundled filtered reducers, all-null inputs, width0, mismatched defined widths, nested parent/child logical nulls, floating cancellation, NaN and signed zero. Math, states, merge protocol and list order are unchanged.

## Matched native matrix workload

Existing profile25 native matrix,24885rows×2535columns (63083475logicalcells), AD arrays with6,144,940missing values. Public biallelic row filtering makes global coordinate AD reductions valid for equal-width arrays. Clean alternating ABBA processes,4iterations each, discard iteration0:6warmed observations/version/query. OPENBLAS_NUM_THREADS=1 in both. No builds, tests, profiling, analysis, provenance hashing or export ran during these measurements. Every output matches; all iterations are retained.

| Public query | Main wall | Candidate wall | Speedup | Main CPU | Candidate CPU | CPU reduction | Main process peak RSS | Candidate peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Coordinate SUM(AD) |1.3217s|0.5525s|2.39×|27.086s|12.878s|52.45%|6068MiB|3691MiB|
| Coordinate SUM / COUNTWHERE / MIN bundle |5.5258s|0.6350s|8.70×|126.996s|14.147s|88.86%|23869MiB|3803MiB|

RSS is Linux ru_maxrss, KiB divided by1024, including startup/input and earlier iterations; CPU seconds sum process threads, not CPU percentage. These are medians, not single best iterations. The native input and public predicates are identical between versions.

Physical-layout controls use public Arrow Table.parallelize inputs,16partitions,524288lists of width16 (huge-gap case has64lists). Eight warmed samples/version/mode: all-valid wall4.69→4.64ms, no observed regression; zero-span null lists27.75→4.21ms; fragmented positive hidden spans8.02→6.57ms; long valid runs7.16→5.63ms. The huge-gap control selects512children from8,389,648physical children and remains~3ms/~235MiB process RSS in both. These short controls verify representation behavior/resource bounds; the large native matrix is the throughput evidence. Complete CPU, RSS, fault data and dimensions are in summary.json/control JSONL.

## Profile attribution and limits

Separate99Hz software cpu-clock profiles identify baseline Coordinates::update_batch at63.20%inclusive CPU (candidate3.64%) and Arrow concat at12.37%inclusive (primitive concat1.43%flat). Int32Array::slice2.22%flat and ScalarBuffer::new1.63%flat disappear from the candidate profile; primitive concat is0.06%flat from other pipeline paths. Kernel osq_lock22.21%flat becomes unsampled, consistent with greatly reduced allocation/page-fault pressure. Some upstream list concatenation remains; this does not claim removal of all concatenation or a DRAM-bandwidth measurement. Profile captures include startup/warmup, so flat percentages are descriptive, distinct from matched warmed timings.

Two exploratory routes are retained honestly: unfiltered global AD coordinate SUM correctly rejected mixed multiallelic lengths2/4; per-variant row AD SUM is valid but routes through MatrixTranspose (10.16%flat), not the H050 adapter. Neither supplies H050 throughput evidence. Baseline valid single profile reports0lost samples/4out-of-order events; the raw warning is retained.

Cloud run root `/home/danielking/h050-nullable-coordinate-runs`; stable source/target unchanged location. Raw profile SHA receipts are recorded separately; large bundle raw trace remains remote rather than consuming laptop disk. Final current-main composition and required reviewer acceptance remain pending before merge.

Verified downloaded small raw-single archive: `/private/tmp/h050-raw-single-profiles.tar.gz`,28MiB, SHA256 `d17be9bc9fb383f42cf63aa9b116d638f9b9272e0c39b735d0e8df165e8aa4c2`. The1.5GB bundle trace is retained remotely with recorded SHA256; no large local raw download.
