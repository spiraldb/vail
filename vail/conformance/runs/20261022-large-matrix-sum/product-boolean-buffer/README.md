# Direct product / Boolean coordinates and numeric state buffer reuse

Material source: edf384f7. Comment-only invariant documentation: 13195009. Public Boolean accumulator protocol test: d45ed7ad9ddd8d8e5bffb07f44e7e5359134bdfd. Native SHA256: c86322df4a256acd378d49f413a5bf328bb555036ec677c18e0e4c338deb1434.

Normal `maturin develop --release --locked`, Rust 1.98.0, worktree-local vail/target, configured kache wrapper preserved. Build passed (50.10s).

Matching Python invocation: `python -m pytest -q python/tests/test_coordinate_product_boolean.py python/tests/test_matrix_column_coordinate_callbacks.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_array_agg_coordinate_dag.py python/tests/test_matrix_array_agg.py python/tests/test_matrix_entry_sum.py python/tests/test_agg_numeric.py python/tests/test_globals.py`: 128 passed.

Native invocation: `cargo test --release --locked --manifest-path vail/Cargo.toml -p vail-execution --test aggregate_factories coordinate_`, Rust 1.98.0 and macOS dynamic_lookup linker flags: 6 passed. Covers published numeric/Boolean snapshots and native Boolean partial-state restoration.

The exact fresh stock-PyPI Hail comparison and 1 kHz before/after profiles are recorded separately in ../../20261023-table-array-sum. That evidence uses this same normal native artifact. Hail/Vail medians: 0.6571/0.04719 wall seconds, 7.7047/0.5753 core-seconds (13.9x/13.4x). This is the exact upstream table array SUM query, not a representative replacement.

Remaining reducers (min/max/stddev and structured/ordered designs) are queued; this receipt does not claim their completion.
