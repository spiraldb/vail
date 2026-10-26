# Reader bridge resource boundary (source-only, not measured)

Checkpoint58944219 is array/layout wiring only. It changes neither native
capability admission nor the translator. The new reader request is a genuine
Bool element projection; original-schema raw/different-body siblings use the
original reader expression. No selected-owner or IO bound is claimed yet.

## Public file IO experiment

Use the public Vortex writer to store fixed-width lists of nullable structs.
Eight unrelated, incompressible primitive fields make the value domain materially
larger than the struct validity bitmap. Generate and write before timing. Keep
rows, child cardinality, logical validity seed and checksum fixed. Independently
calculate per-list counts from the seed, without evaluating the tested expression.

A wrapper around public VortexReadAt counts actual requested byte ranges and
completed read bytes. Each query uses a fresh opened file and reader so neither
session/segment caches nor previous projection output can explain omitted IO.
Report footer/open bytes separately from scan bytes, coalesced requested bytes
separately from unique covered ranges, cold/warm process windows, CPU/wall/RSS.
Do not assert private reader names, cache hits, or exact byte thresholds in unit
tests. Record actual produced layout tree as provenance, not as a behavioral
oracle. Require a real Struct layout with separately addressable validity to
claim payload omission. A Flat layout is an explicitly different boundary.

Compare raw full read, direct list-element validity map, Sum(Map), repeated same
map, raw-value sibling and distinct validity paths. The last two must retain
correct original values/types; they need not skip IO. The sum of per-row results
is checked outside the scan, preserving ordinary downstream aggregation.

## Narrow native certificate (not implemented)

The future certificate is distinct from DenseI32Sum and ListLength. It binds
snapshot identity, exact file identity/schema and selected partitions, list path,
struct presence path, prepared Bool output type and operation kind. Admit only:
whole effective source row coverage without predicates/deletes/range pruning;
all logically valid outer lists; checked dense offsets from0 to exact child
cardinality; exact corresponding row domains at every nested struct; bounded
non-null Bool validity layouts (audited bitmaps/constants/chunks) at every logical
ancestor and target. Dictionaries, unknown encodings, missing outer groups,
sparse retained-owner selections and nullable parents before the list decline
before computed payload work. The immutable prepared expression must remain tied
to its operation proof, including residual result schema and missing/empty
semantics. No arbitrary Vortex expression entry is introduced.

This proves work proportional to the full effective list child domain plus its
validity metadata, not arbitrary sparse query selections. Supporting nullable
outer lists or sparse children requires a separate exact-range/codec proof.

## Adversarial public controls before native admission

Missing outer lists with orphan payload, distant visible ranges, nullable struct
ancestors, zero-length lists, nonzero list slices, dictionary values larger than
visible codes, unknown Pco payload/mask encodings and schema changes must either
produce correct ordinary fallback or be proved by a new bounded operation.
A post-canonical Pco experiment cannot certify this pre-decode route.

Fresh/reused tiny and many-small-file actions must disclose metadata preparation
latency/read-byte/RSS costs. Existing cumulative charged metadata estimate and
outstanding eight-file window remain unchanged; neither is a strict parsed-heap
cap. No global cache, new terminal count operator, native matrix benchmark
matcher, fake null structs or Boolean Count kernel is permitted.
