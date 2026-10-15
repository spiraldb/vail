# Variant QC port

The vendored Hail Python `variant_qc` body is ported in `python/vail/qc.py`.
Its ambient `Ref('n_cols')` becomes Vail's lazy matrix-scoped
`matrix_num_cols` expression, constructed by `mt._num_cols()` and resolved
from the owning plan's ordered global column array during lowering.

Also supplied the scalar HWE expression needed by the copied method, backed by
the existing native HWE kernel. The shared kernel now follows Hail's
Levene–Haldane recurrence and summation order. This fixes the exact upstream
assertions `0.7` (two-sided) and `0.7000000000000001` (one-sided), rather than
relaxing them. Struct selection and the expression-only `parse_variant` helper
allow the copied body and original upstream input setup to run.

Final upstream original-body checks, with every body assertion retained:

- `methods/test_qc.py::Tests.test_variant_qc`: passed (`final-original.json`).
- `methods/test_qc.py::Tests.test_variant_qc_alleles_field`: passed
  (`final-alleles-field.json`).
- `expr/test_expr.py::Tests.test_hardy_weinberg_test`: passed
  (`final-scalar-hwe.json`).

These use the existing isolated-body adapter: imports, fixtures, and backend
selection target native Vail. The QC module's unused VEP configuration imports
remain unavailable and are recorded in the JSON setup notes; these three tests
do not use them. No assertion or test body was changed. Earlier JSON files in
this directory record the initial missing parser and floating-point failures.

Native HWE unit tests: **2 passed** (`native-hwe.log`), including exact QC
floating-point values, published examples, and aggregate ploidy/merge behavior.

New local regressions cover construction without an eager count, expression
source validation, column filtering, empty row and column sets, row/entry and
aggregate contexts, missing versus filtered entries, optional DP/GQ summaries,
multiallelic HWE missingness, scalar HWE broadcasting/globals/missing inputs,
and parser/struct selection behavior.

Full local Python suite plus both conformance harness test files:
**1,260 passed, 1 skipped, 19 subtests passed**, in 310.30 seconds
(`full-python.log`). Command, from `vail/`:

```sh
PYTHONPATH=python:conformance .venv/bin/python -m pytest python/tests conformance/test_harness.py conformance/test_upstream_backend.py -q
```

Final focused QC regressions: **8 passed** (`focused.log`). The final rerun
checks actual values from entry annotations, rather than a count that an
optimizer could satisfy without evaluating those annotations.

Targeted Rust formatting checks and `git diff --check` also passed.
