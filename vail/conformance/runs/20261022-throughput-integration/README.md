Combined four throughput fixes, production source 850f89382f8e1848a344e9a4103165942c7862d4 against main15583b71. All merges clean; no manual resolutions.

Rust1.98 release, worktree-local target, kache configuration unchanged. Wheel SHA256 4c2b2d6884cc4554cb4efeaf2b0106b39d224bc04b5ab20d947aa32c6592e6b6; native SHA256 3c9a3edf8a6d94a8fb707190cf949ed122dfcbd7b2509571adb35b5a5791332a.

Matching extracted wheel passed178 tests in21.10s: test_agg_numeric, test_bind_columnar, test_grouped_collect_nested, test_matrix_layout, test_vcf, test_agg_max_nan, test_array_agg_coordinate_callbacks, test_array_agg_coordinate_registry. PYTHONPATH=/tmp/throughput-integration-site, CPython3.12. Logs included. Separate component tests also passed as recorded by reviewers.

No new throughput claims: local performance gate deferred by user; full same-VM Hail/Vail benchmark remains outstanding.
