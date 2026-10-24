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
