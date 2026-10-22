# Per-row captured dictionary lookups inside list maps

The upstream optimized coverage query computes entry mean and a per-row counter,
then maps 100 queries over that dictionary, scans the counts and computes nine
suffix sums. Its October 22 cloud result was 49.109s/1026.72 core-seconds versus
Hail 5.640s/66.11. The actual 250000×1000 randomly generated native fixture is not
available locally; no full fixture was regenerated.

`representative_query.py` preserves that query shape using deterministic
20000×1000 range inputs, 32 partitions. It is an attribution probe, not the full
upstream benchmark. The profile runs were contended as explicitly permitted by
the user and do not establish a controlled wall-time speedup or Hail ratio.

Before profile uses the already normally built combined integration artifact
`/tmp/throughput-integration-site`, native SHA256
`3c9a3edf8a6d94a8fb707190cf949ed122dfcbd7b2509571adb35b5a5791332a`
from reviewed 850f/merged 701d. It predates later independent native/import fixes.
No compiled library was copied or preserved for this work. Before query returned
20000 rows; process query delta 2.283929s wall / 23.685805 CPU, high-water RSS
16230612992bytes (whole process).

The 1000 Hz profile has 9046 samples. 7062 of 7127 samples under DataFusion's
GroupValues interner descend from dictionaryLookup, only 65 from counter's
per-list grouping. Leading raw-sample leaf attributions (CPU-weighted figures below): GroupValues intern 36.8%, hash table
accounted insertion 16.8%, rehash 14.5%. ArrayFold is 1.5% inclusive and was not changed.
The cause is captured dictionaries expanded for every mapped key, followed by a
fresh grouping index over the repeated copies. This turns 100 probes into indexing
roughly 100 copies of each owner's dictionary.

The focused rewrite handles unconditional mapped `.get`, `.contains`/found or
lookup-value projections with callback-invariant plain captured dictionaries.
Existing DataFusion array_transform computes the query-key list. The dictionary
is indexed once; all queries use owner IDs to probe that shared per-batch native
DataFusion grouping index. Existing lazy default evaluation follows the lookup.
Computed dictionaries, callback-dependent defaults and nested conditional lookups
retain the original path. No custom hash/equality/reduction algorithm is added.

Synthetic result binder names avoid active scope/schema/referenced/bound names.
The public native regression deliberately collides an outer lambda's name and
independently checks the nested Arrow result. Python public tests cover multiple
row dictionaries, empty/missing lists/dictionaries, missing/repeated keys and
values, row-dependent key/default captures, string keys, and lazy default errors.

An initial adjacent grouped-aggregation batching patch was dropped because the
counter-only profile did not demonstrate a CPU/RSS improvement; final production
changes are limited to mapped dictionary lookup.

## Final validation and attribution

Production revision `5a6bfd32` (formatting-only child of reviewed `281d28fa`),
normal worktree-local release/kache build, native SHA256
`ef629561d216755c41a7c2d31b9c7413adfefeca2293ef96397ddc87e9025f84`.
No RUSTC_WRAPPER override, shared target directory or compiled-library cache
workaround was used. Test-only `1d9b93bd` adds three-partition sliced list input
and mapped membership coverage. Six focused public Python suites: 50 passed;
updated mapped-lookup tests: 3 passed.

The final representative query returned 20000 rows. Query process delta:
1.062731s wall / 4.074359 CPU, whole-process high-water RSS 843923456 bytes.
Both before/after captures ran with contention and different baseline sources;
these are attribution/resource diagnostics, not a controlled speedup claim.

`analyze_profile.py` weights each sampled stack by its recorded threadCPUDelta,
not wall sampling counts (which include blocked threads). Lookup inclusive CPU
falls from 86.464% to 4.178%; GroupValues interning beneath Lookup from 77.920%
to 2.843%. The remaining native grouping belongs principally to the actual
counter, not repeated dictionary construction. There is no ArrayFold change.
The symbolicated profiles embed saved symbol names; unknown frames remain
unknown rather than invented source-line attribution.

Public native lower/evaluate lexical-collision regression: 1 passed under
`cargo +1.98.0 test --release -p vail-execution --lib
public_mapped_dictionary_lookup_preserves_colliding_outer_binding`.
All source/test review findings were closed before final evidence submission.
Original full-cloud fixture and timings remain unchanged; no VM was restarted.
