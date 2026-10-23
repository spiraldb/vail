# Hail VDS read routing

Production/test candidate: `460adffd`, based on frozen main `e79cf6ff`.
Branch: `dk/vds-legacy-routing`. Main and cloud benchmark sources are unchanged.

A VDS contains `reference_data` and `variant_data` MatrixTables. Each component
now uses `read_matrix_table` when `_vail_matrix.json` exists, otherwise the
existing Rust `MatrixTable.read_legacy` scan. There is no Python row/entry decoder
or new row-loop machinery. Existing LEN/END derivation, `_drop_end`, and max
reference-block-length sidecar handling are shared across both formats.
`intervals` and `n_partitions` remain explicitly unsupported, as before.

## Validation

Dedicated virtualenv `/private/tmp/vail-vds-routing-env`; normally built with
`maturin develop --release --locked`, global `rustc-wrapper = "kache"`, release
debug disabled. Build completed in 4m20s. No native artifact copying/reuse.
`provenance.json` records asserted source/native origins and SHA256.

- Four new public tests pass (7.04s). They read the real Hail-written 1kg VDS,
  compare every sparse row/entry to the checked-in independently extracted Hail
  oracle, preserve missing entry structs, compare a Vail-native roundtrip, check
  five samples/81 variant rows/max-block-length sidecar, exercise `_drop_end`,
  and retain explicit planning-option errors.
- Existing local VDS suites: 26 pass, six fail. Five fail in existing dense,
  sparse-conversion/combine execution with `ScalarSubqueryExpr evaluated before
  the subquery was executed`; one fails with duplicate projection field
  `locus`. These failing tests do not invoke the changed `read_vds` path.
- Exact 28 upstream cases formerly blocked by the storage guard: three pass,
  25 fail. No remaining failure has the removed storage guard. Detailed results
  are in `upstream-summary.json` and the raw JSONL/log.
- The 25 downstream failures comprise 20 missing APIs, three scalar-subquery
  execution errors, one `KeyError: 'op'`, and one 30-second per-test timeout.
  Missing API counts: `filter_intervals` seven; `sample_qc` five; one each of
  `write_variant_datasets`, `segment_reference_blocks`, `interval_coverage`,
  `read_dense_mt`, `truncate_reference_blocks`, `experimental`, `split_multi`,
  and `export_vcf`. These are exposed by successfully reading the source VDS;
  implementing them is outside this routing fix.

The upstream runner retains behavioral assertions and records structural
adaptations. Its isolated initial attempt had a missing `typing_extensions`
collection dependency; the reported rerun installed fresh test dependencies and
collected/executed all 28 selected cases. No benchmark/performance claim is made.
