# Current upstream randomness audit

Reran the same 131-definition upstream randomness selection after sampling,
lexical binding, and logical nested aggregation changes.

| Mode | Passed | Failed/blocked variants | Worker blockers |
| --- | ---: | ---: | ---: |
| Original assertions | 30 | 93 | 8 |
| Structural IR assertions omitted | 110 | 13 | 8 |

All six definitions in upstream test_randomness.py pass in both modes.
These are definition counts: every declared variant must pass for a definition
to count as passing. These isolated-body runs adapt imports, decorators,
initialization, and fixtures, and are not an unmodified Hail runtime run.

The original-assertion run is in ../20261014-randomness-current-original.
Exactly 80 additional definitions pass after omitting assert_contains_node
calls in the Table/MatrixTable modules and the diagnostic print(t._tir).
All other test-body assertions remain. Missing Hail IR classes account for
the structural coverage gap; behavioral passes do not establish IR equivalence.

The eight workers stop at the unsupported Hail JVM choose_backend import in
test_statgen.py. This blocks four model definitions and four regression
definitions before their bodies run, so they are not counted as passes here.
Four original model bodies were separately verified in the sampling/model
follow-up. The fixed phased-genotype golden mismatch remains separately known.

The 13 remaining failed/blocked definitions involve experimental/plotting APIs,
direct Hail stream IR construction, variant_qc, encoding flags, pc_relate,
BGEN/GEN/PLINK import, UTF-16 string semantics, maximal_independent_set, and
worker exception formatting. Per-definition traces are in the numbered files.

Compared with the preceding IR-omitted audit, passes increased from 104 to 110.
No selected behavioral definition regressed. The selection covers direct
rand_*, shuffle, and sampling calls and same-module callers; it does not prove
discovery of every possible indirect randomness consumer.

results.json, summary.json, and run-audit.py preserve the outcomes, tested
native-library hash, and reproduction script. Run the script from vail/ with
the Vail Python package on PYTHONPATH.
