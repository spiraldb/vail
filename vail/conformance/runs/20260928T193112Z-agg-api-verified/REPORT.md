# Aggregator API verification

Command: `CARGO_TARGET_DIR=target/integration uv run --no-sync python scripts/run_all_tests.py --label agg-api-verified`

| Suite | Passed | Other |
| --- | ---: | --- |
| runner_self_tests | 9 | — |
| build_extension | 0 | — |
| rust_workspace | 54 | — |
| python_tests | 230 | 19 passing subtests |
| native_conformance | 4 | — |
| upstream_table_cases | 101 | — |
| hail_oracle | 0 | 4 unavailable |

No failed assertions or infrastructure failures. No source drift or native-extension changes during the suites.

All 33 public Hail aggregator exports and keyword parameter names are checked against the upstream source. Twelve original Hail aggregator bodies run inside the Python suite; they are not extra cases to add to its count.

The four Hail oracle fixtures remain unavailable because the checkout cannot import hailtop.version. The selected upstream and native source-derived fixtures do not establish complete Hail parity.

Known compatibility gaps include raw CDF/compaction metadata, KLL int64 range and k constraints, expression-valued initialization/options, and dynamic call_stats allele initialization on empty input. See ../../../docs/AGGREGATOR_INVENTORY.md and the numeric/genomic research notes.
