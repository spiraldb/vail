# Total aggregation regression results

The extension build and all available suites passed. Source hashes and the native
extension remained unchanged during the suites; no integrity failure was detected.

| Suite | Passed | Failed | Unavailable |
| --- | ---: | ---: | ---: |
| Rust workspace | 50 | 0 | 0 |
| Python and conformance harness | 166 (+19 subtests) | 0 | 0 |
| Recorder self-tests | 9 | 0 | 0 |
| Native conformance fixtures | 4 | 0 | 0 |
| Selected original upstream Table cases | 98 | 0 | 0 |
| Hail oracle fixtures | 0 | 0 | 4 |

The four oracle fixtures cannot initialize Hail because `hailtop.version` is
missing. They are not passes. The selected upstream cases use original Hail test
bodies against Vail; this is not execution of the complete upstream test suite.

The 16 new Python aggregation tests passed. The upstream allowlist gained
`test_aggregate1`, `test_aggregate_ir`, and `test_aggregate_localize_false`, taking
its passing case count from 95 to 98. Rust coverage gained eight accumulator tests.

Implementation and current limits are documented in
[Total aggregation](../../../docs/TOTAL_AGGREGATION.md). Detailed outcomes,
commands, timings, and counts are in [summary.json](summary.json), with per-suite
logs alongside it.
