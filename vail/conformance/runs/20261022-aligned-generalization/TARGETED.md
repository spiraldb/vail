# Targeted global-capturing entry filters

User authorized implementation, required review and main integration after the
broader study. This implementation changes only `MatrixTable.filter_entries`
when its predicate references a matrix global AND passes existing aligned-map
eligibility. Lookups, aggregates, randomness and column-position expressions
retain their prior path. Entry-only/row-only/column-only filters and entry
projection eligibility are unchanged. No native or public API changes.

The predicate's selected entry value is passed to the existing aligned mapper
with `preserve_cell=True`. Its outer presence guard preserves filtered slots;
missing predicates remove entries for both keep modes. Existing row-index
key projection is preserved. The selected global fields are bound independently
instead of capturing an enclosing globals column.

## Exact validation provenance

Production source revision: `84c9526238a93f73963058a8194af159e1445650`.
Current integration baseline: `8892034b6257ea85d4939184010e7104ecd6c807`.
Both isolated packages contain otherwise identical current-source Python code.
Native SHA256:
`68254f5ee21c520edc7e3be33a2d74b496c7a41c477d2408106a7262bda8e12b`.
No Rust/crate/Cargo changes occurred between the previously validated main
integration artifact and this baseline; no new native build was necessary.
Full native and routing hashes are in each benchmark environment record.

Eight suites: new global-filter tests, full matrix_table, matrix layout, aligned
maps, VDS, VDS functions, optimizer contracts and Arrow metadata. Result:
**203 passed, 1 baseline-existing failure** in100.54s. All10new tests passed.
They exercise both keep modes, missing globals, phased calls/nested metadata,
repeated/reordered columns, empty axes, prefiltered slots, defined empty entry
structs and lookup fallback. Tests inspect only public behavior.

The failure is `test_make_table_filter_stats_and_explicit_blockers`, which
expects `m.cache()` to raise `NotImplementedError`. It fails identically against
unchanged main8892034b (2.07s): cache no longer raises. Both candidate and
baseline logs are committed. This unrelated cache test was left unchanged.
The reviewer confirmed the reproduced baseline failure does not itself block
this scoped filter change.

Run command (substitute one of the two recorded PYTHONPATHs):

```sh
PYTHONPATH=/Users/danielking/.codex/worktrees/vds-holes-experiment/packages/targetedglobal \
/Users/danielking/projects/vail/vail/.venv/bin/python -m pytest -q --import-mode=importlib \
 vail/python/tests/test_matrix_global_filter.py vail/python/tests/test_matrix_table.py \
 vail/python/tests/test_matrix_layout.py vail/python/tests/test_aligned_runend_experiment.py \
 vail/python/tests/test_vds.py vail/python/tests/test_vds_functions.py \
 vail/python/tests/test_kernel_optimizer_contract.py vail/python/tests/test_arrow_type_metadata.py
```

## Actual narrow implementation benchmark

Same materialized fixture4096×128:524288entry positions, with prefiltered slots.
Independent exact formula oracles check every result. Three fresh processes per
variant, one warmup plus three measured actions each, serial execution with
alternating order. Thus9measured actions per table cell. Native executable is
identical; routing module hashes differ. RSS is process-wide cumulative peak,
not memory attributed to just one physical expression. Timings include aggregate
action planning/execution/decoding; initial fixture read and expression building
are excluded and recorded separately. Shared host, no controlled hardware claim.

| Workload | Existing ms | Targeted ms | Ratio | Peak RSS MiB existing / targeted |
|---|---:|---:|---:|---:|
| Global-capturing filter |176.79|18.42|9.60×|4411 /778|
| Row + global-capturing filter |174.93|22.84|7.66×|4516 /946|
| Row-only filter control |21.51|21.92|0.98×|821 /825|
| Entry-only filter control |20.82|19.15|1.09×|903 /906|
| Entry-only select control |35.00|34.39|1.02×|1836 /1839|

The small differences on unchanged paths illustrate host noise; they are not
claimed improvements/regressions. This study supports the narrow dispatch only.
It does not isolate run-end encoding from capture pruning, does not establish a
Hail win, and does not justify replacing all general array lambdas. See raw
`targeted*.jsonl`, `targeted-summary.json`, `targeted_benchmark.py` and provenance.

## Review and integration record

Required reviewer thread: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Exact84c95262 received scoped source/test clearance; no new source findings.
The final documentation/evidence candidate is submitted for main approval.
Final reviewed revision and disposition are recorded in the main integration
commit, which must follow approval. No additional production changes after
source84c95262 are included in this evidence update.
