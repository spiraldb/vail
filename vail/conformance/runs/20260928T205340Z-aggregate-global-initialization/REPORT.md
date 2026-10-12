# Aggregate globals initialization verification

Command: `CARGO_TARGET_DIR=target/integration uv run --no-sync python scripts/run_all_tests.py --label aggregate-global-initialization`

| Suite | Passed | Other |
| --- | ---: | --- |
| runner_self_tests | 9 | — |
| build_extension | 0 | — |
| rust_workspace | 58 | — |
| python_tests | 241 | 19 passing subtests |
| native_conformance | 4 | — |
| upstream_table_cases | 101 | — |
| hail_oracle | 0 | 4 unavailable |

No assertion failures, infrastructure failures, source drift, or native-extension changes during suites.

The new Python tests assert Hail-derived behavior for expression-valued histogram bounds/bin counts, quantiles, fold initialization and call_stats allele counts. They cover empty and filtered scopes, grouped aggregation, local/captured globals, lexical array lambdas, missing parameters, a lazy aggregate dependency, and deleted/restored global files across actions.

Two previously incorrect semantic expectations were corrected: call_stats AF is missing when AN=0, and the exact median of [0,1,2,3] is 2 under Hail's rank convention.

Expected behavior comes from checked-out Hail source, not a live differential run. Four oracle fixtures remain unavailable because hailtop.version cannot be imported. See ../../../docs/AGGREGATE_INITIALIZATION.md for implementation and source references.
