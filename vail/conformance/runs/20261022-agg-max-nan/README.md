# Floating-point agg.max NaN compatibility

Candidate: `1e451c07` on base `c8cf283ae8be0339822031251772493dd4f6feb6`.

Hail reference: `hail/hail/hail/src/is/hail/expr/ir/functions/UtilFunctions.scala`
`nanmax` and `agg/MonoidAggregator.scala` ComparisonMonoid. A non-NaN wins
over either sign of NaN; all-NaN yields NaN; empty/all-missing yields missing.

The frontend composes two native MAX reducers, coalescing a NaN-nullified
maximum with a raw maximum. DataFusion's existing isnan scalar UDF supplies
the predicate. This covers the public grouped, local-array, coordinate,
MatrixTable, and exclusive scan paths through the same aggregate expression.
Integer and scalar max retain their existing paths. Signed negative NaN min
behavior is an existing issue outside this max change.

Validation uses the release wheel built from this checkout with Rust 1.98.0,
`maturin build --release --locked`, Python 3.12.12, and absolute checkout
PYTHONPATH. The native extension is extracted from that wheel into the ignored
local package directory before testing.

An initial preflight failed because scalar isnan had not been included in the
expression validator's native-UDF type inference branch. Revision `1e451c07`
fixes that integration issue. The reviewer also requested strict zip checks to
prevent truncated coordinate/scan outputs from passing; revision `0d02d81f`
addresses that finding.

Final validation on the corrected release artifact: **177 passed** in 18.37s
across the eight focused aggregation/scan modules listed in validation.json.
Original Hail `expr/test_expr.py::Tests.test_agg_minmax`: **1 passed** with its
behavioral assertions unchanged. The temporary upstream adapter provides Vail
as Hail; no filesystem/backend/IR-only or linear algebra tests were selected.
Raw build, pytest, and upstream results are stored alongside this document.
Native SHA-256: `72934f1db9427a971e49930e46b31d6575ea38e0c837fb7a432f31388bf12448`.

Required reviewer thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact
source revision `1e451c0762760d97dd505cd648b3112061dadfb6` against unchanged
main base `c8cf283ae8be0339822031251772493dd4f6feb6` after inspecting the
matching validation evidence. All findings are closed (see review.json).
The reviewer authorized an accurate evidence/receipt-only child without new
runtime validation and scoped local integration, preserving unrelated files.
