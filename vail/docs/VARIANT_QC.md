# Variant QC

`vail.variant_qc(mt, name='variant_qc')` ports the Python expression implementation
from the vendored Hail `methods/qc.py` (MIT; see `licenses/HAIL.txt`). It computes
call statistics, missing and filtered entry counts, call rate, biallelic HWE
statistics, and optional DP/GQ summaries.

The ambient Hail `Ref('n_cols')` is replaced by `mt._num_cols()`. This returns a
Vail expression with IR `{"op": "matrix_num_cols"}` and the owning matrix as its
expression source. It has global scope and resolves during matrix lowering to
the length of that plan's ordered global column array. Construction executes no
query. Normal source checks reject its use with a different matrix.

Column filters change this count; entry filters do not. Empty row sets preserve
it. Consequently `n_filtered = int64(mt._num_cols()) - agg.count()` distinguishes
filtered entry slots from existing entries with missing GT values. The expression
also works in entry annotations and matrix aggregate inputs.

`hardy_weinberg_test(n_hom_ref, n_het, n_hom_var, one_sided=False)` exposes the
shared native HWE kernel as a scalar expression. The kernel uses Hail's
Levene–Haldane probability recurrence and mid-p summation order; scalar and call
aggregate forms share it. This preserves the upstream QC test's exact floating
point assertions. Multiallelic HWE fields are missing, as in the copied method.
