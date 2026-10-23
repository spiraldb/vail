# Local CSE in mapped struct bodies

Candidate `b9b12189`, baseline reviewed main `7aad07b5`. Both use normal
Rust 1.98 release builds with kache; separate worktree targets and installed
environments. Native and source identities are recorded in provenance.json.

## Exact many-column query, three alternating pairs

The original upstream `many_aggs` builds33 named column annotations on the
cached profile25 matrix. Every query returned2535 columns. No query or input
substitution, frontend monkeypatch, SO copy, concurrency change or boolean
accumulator change is used in this comparison.

| Metric | Baseline median [min,max] | Local CSE median [min,max] | Change |
|---|---:|---:|---:|
| Query wall seconds |5.840 [5.468,5.850]|4.872 [4.009,4.881]|−16.6%|
| Query core-seconds |61.850 [58.116,61.936]|49.745 [48.494,50.602]|−19.6%|
| Process peak RSS, decimalGB |10.728 [10.626,10.785]|10.987 [10.873,11.075]|+2.4%|

Fresh processes, engines/variants serial, same32-way execution setting.
Wall/rusage are scoped to the action; maxRSS is the process lifetime peak.
No own build/profile overlaps these six samples. Ambient laptop activity was
not controlled, so these are diagnostic comparisons. Do not compare their
absolute values to older Hail measurements or claim a new Hail ratio.

## What is shared

The native expression compiler analyzes strict Struct projection regions,
interning shallow expression properties and child classes. Repeated safe
arithmetic and list lengths are bound with the existing no-case Switch lexical
binder. Each intermediate is evaluated once per body invocation and its Arrow
result is reused. The shared result is not cached across invocations/actions.

Conditional branches, unknown/fallible operations and nested lambda scopes are
opaque. Division/modulo require a positive integer literal divisor and safe
operands. Array indexing and casts remain conservative barriers. This is a
limited local optimization, not general CSE of every operation.

Scalar-row aligned maps with row/global captures deliberately retain their
existing route: the current lexical binder could otherwise materialize those
scalars as repeated arrays. No row/column metadata alignment change is part of
this experiment. Ordinary array-map lowering already uses DataFusion's physical
array_transform; its directly compiled body does not receive logical CSE.

## Profile evidence

Both actual original workloads were profiled separately at200Hz. Durable
symbolized profiles and threadCPU-weighted summaries are included. The sampled
process CPU totals were62.569s before and51.311s after, including initialization.
The list-length caller subtree fell19.535→8.695 sampled core-seconds; numeric
kernel self samples fell2.747→1.497s. Inclusive timings overlap and must not be
added. These profiles support the mechanism; the unprofiled N3 samples above
are the latency/CPU comparison.

Remaining costs include boolean→UInt8 conversion, boolean bitmap construction
and per-bit access. Their absolute sampled costs do not materially improve.
The lexical binder has15.755s inclusive CPU (including the expressions it runs)
but no observed self samples; neither local-CSE planning nor structural class
construction appears in sampled execution. No setup/configuration was added
inside entry/row loops; compilation and schemas remain prepared beforehand.

Keeping shared intermediates live longer raises peak memory modestly. This
does not remove all redundant work or address boolean accumulation traffic.

## Validation and review

Exact final-artifact127 Python public tests pass, covering lazy failures,
random contexts, repeated actions, captures, sparse/null entries, coordinate
reducers and composite callbacks. Two new native tests pass through public
Expr→lower→evaluate: an unused enclosing variable with the generated-name
prefix, and a96-level nested struct projection with explicit expected values.
No private helper/plan assertions or mocked execution.

Required reviewer source/test clearance received for `b9b12189`; initial
quadratic nested-struct analysis and binder-name collision findings were fixed.
Final performance acceptance requested with this evidence before any main merge.
