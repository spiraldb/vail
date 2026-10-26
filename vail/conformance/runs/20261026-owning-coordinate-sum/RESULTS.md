# Owner-preserving global integer coordinate SUM

Production source: `00a8d7d7fdefb8d2c324fc33c1ec6c58706f02ec`, based on
main `dc13b054`. This is a correctness/resource-test candidate, with no cloud
throughput comparison yet. Main has not been updated by this agent.

Only a complete global `aggregate_entries(agg.array_agg(lambda x: agg.sum(x),
mt.integer_array_field))` enters this route. Mixed aggregates, computed bodies,
captures, lookups, noninteger arrays, initializers and unsupported options keep
the complete original route. The original core and full cells demand remain
upstream; this does not add source pruning or claim IO/decode savings.

The adapter resolves the exact ordinary Struct path against the final argument
schema, borrows its List field, selects observed owners, and copies only selected
primitive spans. It never gathers outer Structs/Lists or unused sibling fields.
Zero masks use a const all-valid path; one is borrowed. Multiple masks are packed
into reusable selected-domain storage and combined with Arrow's whole-buffer,
offset-zero bit operation. Width/span traversal consumes one validity mask.
Existing Coordinates owns arithmetic, state, merges and finalization. Unknown
width remains distinct from a defined empty array.

## Runtime evidence

- Exact corrected production: **8/8 public Rust tests pass**. These cover the
  combined excluded outer gap + included missing inner array with growing hidden
  primitive span + growing unused sibling, allocated bytes through the public
  accumulator operation, sliced/null ancestors, word boundaries, wrapping,
  empty/missing partitions, repeated updates, partial merges and ragged errors.
- **435/435 public Python tests pass**: 431 existing cases plus four new public
  cases covering filtered entries, missing/empty/null values, widening, mixed and
  computed/captured fallback, cross-partition widths and a native roundtrip.
- Normal wheel build, kache, nice10/jobs2, native CPU flags. Fresh wheel-installed
  environment; no PYTHONPATH or copied/transplanted native library.
- Installed native SHA256:
  `2db480cce22ebefec0fc20629529728e4036f9c7263cef5fb31897664840ac85`.
- Wheel SHA256:
  `517f4aaa09beaea4039c77afc24a5606a0a393bf0f426c95e2d64e385bfcc15f`.
- `receipts/provenance.json` records exact source, all changed-file hashes and
  installed frontend hashes. Native/wheel files remain in the owned local gate
  directory; they are not checked into Git.

The private admission/physical-plan receipt is explicitly diagnostic, not a test
oracle. On the corrected artifact it shows the adapter under ordinary DF
partial/final AggregateExec, without entry Unnest. Public output tests, rather
than plan names, determine correctness.

## Preserved adverse evidence

The first compile exposed an ExprError-to-DataFusionError conversion omission;
`3dcb6461` fixed it. That revision passed five Rust tests and four public Python
cases on a normal artifact; those earlier receipts are not substituted for the
corrected artifact.

Reviewer found a real pinned Arrow edge in `5338851d`: in-place AND over short
unaligned destination spans can clear following selected bits. Its initial
seven-test pass lacked the necessary three-short-span case. A normal wheel build
of that revision was stopped via its verified owned process group; its partial
log is retained and does not establish runtime acceptance.

Test-only counterfactual `c01cfc69`, retaining 533 production, demonstrably fails
with `[33]` instead of independently expected `[66]`. `00a8d7d7` packs each mask
completely before one offset-zero AND; the same regression and all other tests
pass. The failing counterfactual log is retained. No Arrow library patch or
custom bitmap kernel is bundled here.

The allocation test excludes fixture construction and query preparation, and
bounds consumer allocation growth when hidden child/sibling spans grow. It does
not establish flat whole-process RSS, IO savings or a wall/CPU speedup. Input
backing storage still exists and may be borrowed during an update.

## Review and remaining gate

Required reviewer source/resource clearance covers exact `00a8d7d7`; the earlier
533 blocker is closed by the exact failing/passing regression. Final evidence
review and matched cloud resource measurements remain before performance/main
acceptance. `CLOUD-PROPOSAL.md` is unrun and requires root authorization.
