# Grouped aggregation verification

All available suites passed, with no source or extension drift.

| Suite | Passed | Failed |
| --- | ---: | ---: |
| Python tests | 179 (+19 subtests) | 0 |
| Rust workspace | 50 | 0 |
| Recorder tests | 9 | 0 |
| Selected original upstream Table cases | 101 | 0 |
| Native conformance fixtures | 4 | 0 |

Four Hail oracle fixtures remain unavailable because `hailtop.version` is missing.
The upstream count is selected original test bodies run against Vail, not the
complete Hail suite. Thirteen new Python tests cover grouped aggregation and its
localized Struct hash compatibility. Three original upstream cases were added.

The earlier grouped-aggregation run had one incorrect test expectation:
unsupported ordering raises RuntimeError, not ValueError. The corrected test and
full rerun pass. No implementation changes occurred between those runs.

See [implementation and limits](../../../docs/GROUPED_AGGREGATION.md) and
[recorded commands and outcomes](summary.json).
