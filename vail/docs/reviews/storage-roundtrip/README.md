# Native storage round-trip correctness

Development branch: `dk/storage-roundtrip-correctness`, based on main
`471f8479b0544fd6d0f27abf1c05c1f027906594`. Code candidate:
`9ee5dffb229617e3954bd2bda623439810fb1643`. Source review is clear; matching runtime validation is pending;
main has not been updated.

## Confirmed defects and implementation

The matching main native extension SHA256 is
`83a0b3ad91b4224cc13e00435d3bd89e75e3f5fc3a971311c458371837f23aa5`.
The two retained baseline failures reproduce: nested dictionary/list writing
raises `ListLayoutStrategy needs at least one chunk`, and a present one-field
struct with a missing child reads back as a missing parent. The saved baseline
log and checksum are in this directory.

The list writer now accepts a typed stream containing no chunks and returns a
zero-row typed layout, as its flat/chunked counterparts already do. It uses
bounded one-chunk lookahead and replays nonempty/error input unchanged. It does
not insert dummy values, collect the stream or change the stored format.

The struct issue is in Vortex expression partitioning: the parent-validity
reference can become a child-validity reference when a single-field expression
is pushed into its child reader. The reader now represents parent validity as
an independent typed Boolean dependency backed by the existing validity child.
Its internal name cannot collide with user fields. Root/field/projection masks
use that dependency before partitioning. The unconditional mask after evaluation
is removed so non-strict null checks and null-filling retain their semantics.
Stored data already has the parent bitmap; this reader correction needs no
rewrite or format-version change.

## Boundary evidence

The phase-labelled native regression passes Arrow conversion and fails the raw
Vortex-file root projection before the struct-reader fix: expected parent bits
`[true, true, false]`, actual `[true, false, false]`. This locates the loss before
Iceberg, DataFusion and Python decoding. The fixed reader passes root/select and
non-strict projection checks. Final direct-field/filter additions are included
in the subsequent full native validation, whose results will be recorded here.

Public regressions cover nested empty/all-missing collections, Arrow type
metadata, one/multi-field structs, independent nested parent/child validity,
empty structs, internal-name collisions, sliced Arrow buffers, multiple input
batches and key filters. They assert values/types at public boundaries rather
than physical chunk or execution-partition layouts.

`patches/vortex-layout-vail.patch` records all differences from published
vortex-layout0.86.1, preserving earlier maintained canonical-storage changes.
Only the list writer and struct reader are newly changed from the main parent;
there is no parallel Vail storage codec or Python/Rust serialization bridge.

## Additional Arrow input defect

The first matching wheel (native SHA256
`6affa417f80255c189710c8a924209ae9c9f39b7fc28c4278dfd7998653821b4`)
passes both original failures and 25 focused cases. A newly added sliced nested
struct test panics during Arrow C Stream import, before storage is reached.
The exact main native reproduces the same panic. PyArrow validates the input
and its own C Stream round-trip successfully; the bug is in arrow-rs conversion.

`ArrayData::slice` in Arrow59.2 slices struct children while retaining the parent
offset; conversion to nested concrete structs applies that offset again. The
published Arrow60 crate contains the corresponding narrow correction. The
backport is implemented in development commit `1bcb5aac`; matching validation
is pending. The workspace remains pinned to59.2; there is no Python input
normalization, row copy or serialization workaround. Final matching validation
will include this additional general input fix.

The nullable nested-field pruning scope is assessed separately in
`pruning-scope.md`; value correctness does not certify payload-read avoidance.

## Validation gate

Matching release wheel, complete local Python/native validation and required
exact-revision source review are pending. The standing reviewer is
`codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`. No main update may precede
review or bypass findings. Concurrent file-aligned reader work is separate.

## Source review disposition

The required reviewer cleared production source and public behavioral tests at
`9ee5dffb229617e3954bd2bda623439810fb1643`. The one test finding was addressed:
the touched internal `TestSegments`/layout assertion was removed, with typed-empty
acceptance retained at persisted public table boundaries. The follow-up also
adds public Arrow slicing checks for an already-offset parent and subsequent
relative/empty slices. The reviewer separately verified the Arrow59.2/60 archive
provenance and exact narrow backport. This is source clearance only; matching
runtime evidence and final integration approval remain required.
