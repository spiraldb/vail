# Array.map verification

Implemented typed ArrayMap and LambdaVariable expression IR with lexical scope
validation, lowered to DataFusion array_transform / LambdaExpr. Python callbacks
construct the body once. Nested maps capture outer lambda variables, row fields,
local globals and independently captured globals.

Recorded suite (source hashes and extension fingerprint stable):

- 127 Python tests and 19 subtests passed, including seven new map tests.
- 41 Rust tests passed.
- 9 recorder self-tests passed.
- 4 native conformance fixtures passed (source-derived expectations).
- Selected upstream Hail cases: 94 passed, 1 failed. Tests.test_annotate now
  proceeds past map and fails at the unimplemented flatmap method.
- 4 Hail oracle fixtures unavailable because hailtop.version cannot be imported.

Map coverage includes missing/empty arrays, missing elements, type-changing
bodies, nested variable-length arrays, lexical captures, singleton globals,
callback construction count, rejected escaped variables and malformed native IR,
lazy execution, and writing/reading mapped data. Two additional interactive
checks passed for mapping globals and capturing across three nested maps.

This is the Vail suite and its selected upstream cases, not the entire Hail test
universe. Full logs and machine-readable counts are in summary.json and the
adjacent log files.
