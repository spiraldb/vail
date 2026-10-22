# Branch simplification audit integration

Source: `afb156d3bf19abf6c7387ddd05b6dbae9c5a7c53`, based on
`e6e2b953d41c61900b25c0f3064c13d38970ae94`.

This combines reviewed Python-only components:

- `7f2f917c`: variant QC uses direct Call predicates and fixed Call comparisons
  instead of reconstructing allele lists. Exact HWE diploid categories retain
  phasing, missingness, multiallelic and filtered-entry behavior.
- `bf5df954`: native absolute value and text-export null substitutions.
- `d5525916`: strict entry inputs rely on propagated null validity instead of
  redundant conditional wrappers.

There are no Rust changes. Setup remains expression construction outside
row/entry loops. No new custom kernel or semantic input validation is added.

## Correctness validation

Normal Rust 1.98 release build using kache and a worktree-local Cargo target
finished in 6m46s. A subsequent normal incremental maturin build packaged the
frozen combined source. No compiled library was manually copied or reused as a
build-cache workaround. The wheel was installed normally with uv in a dedicated
CPython 3.12 environment.

Wheel SHA256: `0ce56e1a7456933f3635cea5b499f14fa37256be48ff1612e3be0e0e250a4885`.
Installed native SHA256:
`59c30b4f8ae085289647ce7990c9f247dcb4f5c2c5590733f7bb839c9de25632`.

105 public Python tests passed in 11.63 seconds. Test files:

- test_variant_qc.py
- test_call_value.py
- test_genotype_index_call.py
- test_native_branch_substitutions.py
- test_matrix_strict_entry_inputs.py
- test_matrix_entry_sum.py
- test_matrix_predicate_reductions.py
- test_matrix_array_agg.py
- test_matrix_layout.py

See build.log and tests.log. The new QC oracle checks public complete QC results
for phased/unphased diploids, haploids, zero-ploidy, missing and filtered entries,
and multiallelic genotypes.

The required reviewer cleared exact combined source/tests. Final integration
approval was requested with the matching artifact evidence. Root alone merges.
The user waived local performance gating; no new measured speedup is claimed.
The active cloud benchmark remains on its earlier frozen source.
