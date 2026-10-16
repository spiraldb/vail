# Native storage round-trip correctness

Development branch: `dk/storage-roundtrip-correctness`, based on main
`471f8479b0544fd6d0f27abf1c05c1f027906594`. Initial source-reviewed code candidate:
`9ee5dffb229617e3954bd2bda623439810fb1643`. Rebased candidate `e79f57db`
is on main `5275ae78`; the reviewer verified all three code/test patches are
identical after rebase. Source review is clear; matching Vail native/Python validation is complete;
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
non-strict projection checks. Final direct-field/filter additions pass in the complete native validation
reported below.

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
backport was implemented in development commit `1bcb5aac`; matching validation
now passes in the rebased candidate. The workspace remains pinned to59.2; there is no Python input
normalization, row copy or serialization workaround. Final matching validation
includes this additional general input fix.

The nullable nested-field pruning scope is assessed separately in
`pruning-scope.md`; value correctness does not certify payload-read avoidance.

## Validation gate

Matching release wheel and complete Vail Python/native validation are complete.
Final exact-evidence integration approval is pending. The standing reviewer is
`codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f`. No main update may precede
review or bypass findings. Concurrent file-aligned reader work is separate.

## Source review disposition

The required reviewer cleared production source and public behavioral tests at
`9ee5dffb229617e3954bd2bda623439810fb1643`. The one test finding was addressed:
the touched internal `TestSegments`/layout assertion was removed, with typed-empty
acceptance retained at persisted public table boundaries. The follow-up also
adds public Arrow slicing checks for an already-offset parent and subsequent
relative/empty slices. The reviewer separately verified the Arrow59.2/60 archive
provenance and exact narrow backport. This was source clearance only; matching runtime evidence is now recorded
below, with final integration approval still required.

## Matching rebased wheel

Wheel2 was built from `e79f57db` on main `5275ae78` in 9m43s. The immutable
package is `/private/tmp/vail-storage-package2`; native SHA256 is
`c601aee1b1cc02ab70ec08a5d74c21f432fed367cb3a8d62f3d0286875867deb`.
All 26 focused cases pass in 3.27s, including both original storage failures
and the sliced nested-struct C Stream input. `wheel2.json` and
`source2-hashes.json` identify the artifact and build inputs. Broader suite results appear below; focused success alone does not certify the
rest of the codebase.

## Broader matching validation

The complete native scope (execution/table/storage/ordering/VCF, library and
integration tests) passes: **269 tests across 45 binaries**, each under a 120s
external timeout. The tests include the direct Vortex root/field/filter and
non-strict projection checks, Arrow repeated/offset-parent/empty slices and C
Data import, plus existing file-avoidance pruning controls.

Maintained Arrow-data library tests pass **43/43**. The attempted extra
Vortex-layout library suite did not execute: Cargo rejects dev-dependency tests
for that patched dependency because it is not a workspace member. This is
recorded in `evidence/maintained2`; it is not counted as passing. Vail's native
integration tests do exercise the modified reader/writer through real files.

The full upstream run completed all **56 eligible modules**: 453 of 1468 original
definitions pass, 617 fail or are setup-blocked, and 398 are collection-blocked.
Parameter cases are 480 passing and 1212 failing. There were no timeouts or native
crashes. All 1468 definition statuses match the earlier corrected `upstream20`
run exactly; this establishes status equivalence, not universal compatibility.
One failure gets further: `scan._prev_nonnull` now works and its assertion is
blocked by missing `_values_similar`. Other comparable first-error lines are
unchanged after address/path normalization; old collection errors do not carry
comparable first-error detail. Raw logs and comparison limits are retained.

The upstream harness keeps function-scoped randomness reset, records 97 omitted
structural IR assertions, and preserves the explicit typecheck/Hadoop/HailFS
exclusions. The dedicated randomness module passes 6/6. Whole-suite failures and
collection blockers remain; these fixes do not claim all random-using tests pass.

The first full local run had 1800 passes and three stale assertions requiring
persisted writer partition identity. Commit `d3c09d55` removes those and two
coincidentally passing assertions of the same unsupported contract. Values,
types, counts, key ordering, file counts and pruning checks remain. The reviewer
cleared that test-only delta. The final full local rerun passes **1803 tests**, with **1 skipped** and
**19 subtests passed**, in 61.98s. `evidence/python3-full.json` records the exact
command, native hash and final test source hash.

Final integration review is requested against the committed evidence candidate.
Main remains `5275ae78` until approval is recorded.

Raw test logs are retained byte-for-byte, including pytest trailing whitespace.
Source/test diffs have no whitespace errors.
