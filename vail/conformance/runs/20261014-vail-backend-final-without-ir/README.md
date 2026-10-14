# Explicit Vail test backend

Vail's native backend now identifies itself as `vail`, exposes an explicit
capability set, and supplies choose_backend through the compatibility import.
Requesting Spark/local/batch raises instead of presenting Vail as another engine.
The shared upstream backend adapter manages the native session initialization,
owned temporary paths, teardown, and Vail timeout selection. Both pytest and
isolated-body runners use it. Hail query-backend test markers name Vail.

Class-level API configuration is adapted without modifying test method bodies:
available APIs retain their actual implementation, while unavailable API
references become callables that raise NotImplementedError when used. This
allows unrelated model tests in the regression test class to collect, while
regression tests actually execute and report their missing APIs. Both runners
record unsupported operations separately from behavior and harness errors.

## Fresh results

The same 131-definition upstream randomness selection:

| Mode | Passed | Failed | Worker/setup blockers |
| --- | ---: | ---: | ---: |
| Original assertions retained | 34 | 97 | 0 |
| Structural IR assertions omitted | 114 | 17 | 0 |

The eight previously import-blocked tests all execute: four Balding–Nichols
model definitions pass, and four regression definitions fail on unsupported
linear/logistic/Poisson regression APIs. The dedicated randomness module passes
all six definitions. The 80-definition structural-IR gap remains.

The 17 remaining behavioral-selection failures have these first failure causes:

| Cause | Definitions |
| --- | ---: |
| Missing public APIs (listed below) | 13 |
| Direct Hail stream IR construction (`ir.NA`) | 1 |
| Hail encoding flags (`_with_flags`) | 1 |
| Hail UTF-16 string behavior in PLINK export test | 1 |
| Worker exception stack-trace formatting | 1 |

Missing APIs: experimental full outer join and ROC plotting (two tests),
variant_qc, pc_relate, import_bgen, import_gen, import_plink,
maximal_independent_set, plotting, plus four linear/logistic/Poisson regression
tests. Unsupported cases remain failures; they are not counted as passes or
silently skipped. These are first blockers, not proof that fixing one missing
API suffices to pass a test.

Normal pytest independently runs the formerly blocked test definitions and
the dedicated randomness module: **10 passed, 4 failed**, with all four failures
categorized unsupported. Event streams and logs are in
../20261014-vail-backend-final-pytest. Local harness, simulation and randomness
validation: **156 passed**; see validation.log.

Counts are definition counts, requiring every variant to pass. Imports,
decorators, fixtures, class configuration, and legacy sample paths remain
explicit test adaptations. Existing Hail flag decorators are still omitted;
this adapter does not implement Hail's compiler or execution flags. The
IR-omitted run also removes assert_contains_node calls and diagnostic
print(t._tir), preserving the other assertions. Neither run is an unmodified
Hail-runtime suite, nor a census of every possible indirect randomness consumer.
The previously observed fixed phased-genotype reference mismatch is outside
this 131-definition selection and remains unresolved.

See results.json for individual variants, summary.json for counts,
source-sha256.json for implementation hashes, and run-audit.py for reproduction.
Original-assertion results are in ../20261014-vail-backend-final-original.
