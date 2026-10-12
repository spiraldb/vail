# Array.flatmap verification

Array.flatmap constructs ArrayMap plus ArrayFlatten scalar IR. Execution lowers
to DataFusion array_transform and flatten. A physical-expression adapter compacts
null inner list spans using DataFusion's Arrow utility before flattening; missing
callback arrays contribute zero elements, while missing individual elements stay
missing. Flattening removes exactly one array level and preserves element order.

Recorded results:

- 133 Python tests and 19 subtests passed, including six new flatmap tests.
- 42 Rust tests passed, including a new sliced Arrow regression with nonempty
  hidden values under a null inner list.
- 9 recorder self-tests passed.
- 4 native conformance fixtures passed (source-derived expectations).
- Selected upstream Hail cases: 94 passed, 1 failed. Tests.test_annotate now
  passes flatmap construction and stops at the unimplemented array.filter method.
- 4 Hail oracle fixtures unavailable due to missing hailtop.version.

No source drift or extension integrity failure. Coverage includes lazy callback
execution, callback construction exactly once, nested lambda/row/global captures,
missing and empty arrays, type/source rejection, nested output types, order,
slicing, and persistence round-trips. These are the Vail suites and selected
upstream cases, not the entire Hail test universe. See summary.json and adjacent
logs for commands and detailed results.
