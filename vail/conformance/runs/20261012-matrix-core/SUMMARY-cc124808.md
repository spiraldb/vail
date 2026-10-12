# MatrixTable upstream coverage after cc124808

Base: `cc124808e23ed03b8c299f2af1bbf85033c80020`, with the row-aligned MatrixTable changes applied. The native extension was rebuilt from this base before running tests.

## Coverage

- All 214 definitions in the pinned upstream MatrixTable file were attempted in isolated subprocesses.
- The previously untested 200: **62 pass, 138 fail or are blocked**.
- The previous selection of 14: **12 pass, 2 are blocked**.
- Total: **74 passing definitions, 140 nonpassing definitions**.
- Expanding the three parametrized definitions produces **229 variants: 74 pass, 155 fail or are blocked**.
- No timeouts or worker crashes occurred. Native panic exceptions are counted separately below.
- The complete Vail Python suite: **829 pass, 1 skip, 19 subtests pass**; this includes all 46 MatrixTable regression cases.

These are original extracted assertion bodies, with original class helpers, shared helpers, fixtures and parameter generators. Hail imports are replaced by Vail; backend decorators/initialization are omitted; temporary paths are local. This is not full upstream pytest collection or a differential Hail oracle run. IR/Scala-dependent assertions are explicitly blocked, never counted as passes.

## First observed blocker across the 140 nonpassing definitions

| Category | Definitions | Examples |
| --- | ---: | --- |
| VCF reader missing | 40 | `self.get_mt()` now calls its original `hl.import_vcf` helper |
| Hail backend dependencies | 30 | 28 IR/Env requirements; 2 Scala codec imports |
| Other shared/Matrix APIs missing | 24 | `empty_array`, `query_matrix_table_rows`, `experimental`, `Table.add_index`, NDArray `reshape`, `Table.sample` |
| Randomness | 19 | `rand_int64`, `rand_norm`, `rand_bool`, `rand_unif` |
| Expression/validation failures | 8 | empty field names, string type specifications, aggregate lowering, interval-key validation, union schema ordering, partitioned writes, comparison coercion, Matrix interval filtering |
| Genetics methods | 6 | `balding_nichols_model` |
| Assertion mismatches | 5 | column aggregation order, indexed-read partition counts, display formatting, and two expected-error-message mismatches |
| Native panic exceptions | 4 | empty structs in distinct/union and interval lookup paths |
| Dictionary literal inference | 2 | non-string keys inferred as struct fields |
| Legacy Hail storage formats | 2 | existing `.mt` and `.ht` fixtures lack Vail descriptors |

Counts stop at the first blocker. For example, a VCF-backed test may require additional missing features after its reader is implemented. Many backend-dependent randomness tests stop before reaching their random identity assertions.

### Assertion mismatches

- `test_aggregate_cols_order`: keyed column aggregation does not use the expected public-key order after reordered column selection and checkpoint.
- `test_indexed_read`: the first requested read reports one partition instead of two; later assertions are not reached.
- `test_show_header`: the basic display does not return Hail's expected formatted string.
- `test_semi_anti_join_rows` and `test_semi_anti_join_cols`: invalid-key rejection text does not match Hail's expected regex.

### Remaining native defect

The four affected upstream definitions are `test_union_cols_distinct`, `test_distinct_by_row`, `test_distinct_by_col`, and `test_interval_product_join`. They raise the zero-field Arrow StructArray constructor panic. A pure Table reproducer still fails on this base:

```python
import vail as hl

t = hl.range_table(4).annotate(value=hl.struct())
t = t.key_by(k=t.idx // 2)
t.distinct().count()  # should return 2; raises PanicException
```

The original scan-backed grouping and partition-array empty-struct extraction reproducers now both pass. Thus the earlier fixes work, but empty structs remain unsupported in additional native paths. No Table fix was added by this MatrixTable task.

## Reproduction

```sh
PYTHONPATH=vail/python:vail /Users/danielking/projects/vail/vail/.venv/bin/python \
  vail/conformance/upstream_matrix_cases.py \
  --source /Users/danielking/projects/vail/hail/hail/python/test/hail/matrixtable/test_matrix_table.py \
  --selection all --jobs 2 \
  --output vail/conformance/runs/20261012-matrix-core/upstream-cc124808.json
```

The JSON contains original-source and frontend fingerprints, base commit, native binary hash, individual variant outcomes, traces, automated categories and reviewed categories. Historical reports remain available separately.
