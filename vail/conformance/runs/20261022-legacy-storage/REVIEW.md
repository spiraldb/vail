# Required review disposition

Reviewer: Codex chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f`
([reviewer](codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f)).
Date: 2026-10-22.

Initial candidate `d5339a5a542876bcd6942edfdae6eeccf9a15760` was **not approved**.
The reviewer required public-API tests, complete field/schema assertions,
linear bitmap access, less avoidable byte/field parsing work, and representative
resource evidence.

Source/test revision `0c68a827571cdda28f5c1333410cf20d8bef8003` received scoped
source/test clearance. Disposition of findings:

- Internal matrix-state and private decoder-helper checks were replaced by
  complete datasets read and inspected through public APIs. The full corpus
  asserts independently specified version-specific schemas and exact field
  sets; dropped payload/global fields fail the tests.
- A separate live Hail writer/readback oracle supplies retained native datasets
  and expected values, schemas and keys, covering more varied values and axis
  behavior than the historical all-values corpus alone.
- Nullable bitmaps use borrowed byte views and constant-time byte/bit indexing.
  Field parsing uses a seen-name set. Primitive reads use bounds-checked
  `unpack_from` instead of copying bytes on every numeric/varint read.
- Recursive wire/logical encoding checks establish the input type preconditions
  for compiled localization and Arrow conversion. Matrix conversion releases
  original entry arrays incrementally and reuses the rows list.
- Fresh native build provenance, complete public test results, and separate
  process wall/CPU/peak-memory results are retained in this evidence directory.
  Documentation explicitly bounds the scale claim and describes overlapping
  eager in-memory representations.

Final candidate `7e73f2cd4b8430d059fa146f774381e7309bc3d8` was **approved for
main** after the reviewer verified it only added five validation-evidence files
to the source/test-cleared revision. The reviewer inspected the build receipt,
141-test public suite result, resource measurements, and documented limits.
There are no unresolved review findings. The reviewer ran no builds or tests;
validation was performed by the implementing chat against the recorded fresh
artifact.

The final approval covers this explicitly eager, local migration reader and
its recorded compatibility/resource evidence. It is not a claim of universal
format coverage, a general memory bound, or production throughput at cohort
scale. This receipt adds no production or test source changes.
