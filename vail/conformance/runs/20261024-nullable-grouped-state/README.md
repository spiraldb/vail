# Nullable grouped accumulator: first public runtime proof

Not merge-ready: this receipt covers the Rust public target only. Native wheel,
Python, H093 Reader/Writer and resource gates remain pending.

The complete-list API keeps scalar Count non-null U64 while nullable declared
outer groups produce nullable results. Missing groups are excluded from actual
scalar states and restored using genuine nullable-index Vortex Take. Opaque
flush pairs states/mappings and resets both. Present scalar nulls remain null;
all-valid batches use the original finalized array without expanded ranks/Take.
The existing list_sum helper migrates to the new additive API; no planner or
source capability eligibility is changed. Legacy scalar/grouped APIs remain.

Exact assembly d3cee402 retains H0932cbfa705 plus reviewed grouped396d8393.
The single authorized existing native-only encoded_projection test graph built
in317.52seconds under600second/1.1GiB constraints. Initial free1,420,840,960;
minimum1,414,852,608; final1,959,337,984bytes. No retry or source fix occurred.

The entire target passed20/20 (11existing+9new), zero ignored/filtered,0.13s.
New public cases cover Count missing/empty/NaN options; declared nullable with
all-valid batch; scalar Count non-null zero; raw Boolean SUM missing/empty before
normalization; present SUM empty/all-null vs missing; all-missing/zero rows;
fixed-size sliced logical ancestor nulls; paired multi-batch flush/reset; wrong
declared schema rejection; combined Mean Sum/Count state correspondence.

Input canonicalization and mixed-group child kernels may still decode retained
owners. Skipping absent metadata is not a codec selectivity proof. There is no
claimed native entry-count speedup or selected-owner bound from this API change.


## Exact combined native and reader gates completed

The same production source passed the H093 public storage Reader/Writer target:
2/2,0ignored/filtered,0.05s; its native-only graph built241.15seconds under the
separate300second budget. This exercises real native writes and reads across
sliced parent/child nulls and dictionary/null-child schema normalization.

A NORMAL maturin release wheel then built333.63seconds and installed normally
into a NEW owned environment; no compiled native transplantation. Imported
native SHA20f0a0fa53828ecbee9f586516f91b70caf0203fe77d1c5952f7f4b3d13a058d
was verified before and after the exact existing420Python public gate:420passed,
0failed,11.31s. The frozen H093741fc15f normal native remains unchanged as the
performance baseline. All combined correctness gates are green:22Rust+420Python.

Current-main merge14648174 adds ONLY upstream evidence documents relative to
the tested source; production/test source hashes are checked unchanged. No
new production, planner eligibility or decoder change is included by that merge.

## Existing all-valid ListSum resource control

Real stored500M entries (200Krows x2500cols), public
aggregate_entries(agg.sum(DP)), identical immutable unfiltered native snapshot.
Computed-source translation invokes fill_null(list_sum(elements),0). An actual
separate3second software sample confirms ListSum::execute -> list_sum_impl ->
ListGroupedAccumulator<SumV2>::accumulate_list -> existing grouped kernel frames.
This demonstrates the control exercised the migrated API, rather than a bypass.

ABBA uses two independent processes per artifact; each records12warm actions,
with the first2discarded for20retained within-process observations/artifact.
These are not20independent process replications. All values20969995813 match an
independent formula. Build/tests/profiling did not overlap unprofiled timings.

| Metric | Frozen H093741fc15f | Combined20f0a0fa | Candidate/baseline |
|---|---:|---:|---:|
| Median wall |52.205ms|51.838ms|0.993|
| Median core-seconds |0.604709|0.610163|1.009|
| Maximum process RSS |802.03MB|787.61MB|0.982|

This is effectively flat performance, not a speedup claim: wall-0.7%,CPU+0.9%
are small relative to process variation. Process-wide RSS includes fixture and
runtime allocations and is not attributed solely to the wrapper. No hardware
saturation, SIMD or memory-bandwidth percentage is inferred from software samples.

## Scope and remaining review

The new full-list API fixes its missing-group contract. The old element-only
Count API remains unchanged and does not gain nullable-group support; its earlier
panic receipt is not silently reclassified as fixed. No new native count-only
pushdown or universal selected-owner bound is claimed. All-missing/zero groups
skip range/child evaluation AFTER input canonical acquisition; a codec may do
work during canonicalization. Mixed retained owners can still materialize through
existing child kernels. Independent final review and any specifically requested
owner-resource probe remain pending before merge.
