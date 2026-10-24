# Native repeated-reduction investigation

Final baseline: `ec909af4963717a6d7a38b853ed16cd00b25450a`.
Production/test source: `91044731` on `dk/native-reduction-cse`.
Native SHA256: `786c8115a053f97b28da181647eb7f9736be57f6d48e2d984dc4edaf9a792c00`.
Main is not updated by this agent. Review is required before integration.

## Result

The fix makes equivalent bound callbacks have the same semantic scalar-UDF
identity, allowing DataFusion's existing CSE to share them. It introduces no
second CSE pass or matrix/benchmark-specific rewrite. Free captures, actual
arguments, Arrow types, volatility, error/RNG barriers and executable expressions
are unchanged. The original program remains available for semantic inspection;
only its comparison key uses canonical lexical names.

Benefits are modest, not an explanation of all remaining row-aggregation CPU.
Final warmed native-input measurements on 500 million sparse entry slots:

| Pipeline | Baseline wall | Candidate wall | Baseline CPU | Candidate CPU |
|---|---:|---:|---:|---:|
| Retained filtered entries plus count/statistics | 0.698 s | 0.633 s | 9.017 s | 8.057 s |
| Twelve row fractions over different thresholds | 4.653 s | 4.466 s | 62.080 s | 59.353 s |
| Four row fractions | 1.399 s | 1.419 s | 18.443 s | 18.599 s |
| Plain entry sum control | 0.152 s | 0.148 s | 1.766 s | 1.820 s |

Thus the filter-feature pipeline saves about 9% wall and 11% CPU; twelve
fractions save about 4%; four fractions are effectively flat. The control's
wall/CPU differences have opposite signs and are small. Do not claim a broad
throughput improvement or a new Hail speedup. Earlier 80M and 500M exploratory
campaigns and a reversed-order filter confirmation are retained separately;
headline numbers above use only `evidence/final500m/` and the final native SHA.

## DataFusion integration and source findings

Pinned DataFusion **55.0.0** `common_subexpr_eliminate.rs` uses exact expression
identity, rejects volatile nodes, and tracks conditional evaluation of CASE,
short-circuit Boolean operations and UDF-declared conditional arguments. We
improve those existing contracts instead of writing a custom CSE optimizer.

Vail's `logical_kernel` already exposes source/global inputs as ordinary UDF
arguments and includes actual Arrow input types in identity. Previously its
remaining expression identity also included arbitrary callback parameter names.
`ArrayMap`, `ArrayFilter`, `ArrayPredicate`, folds and lexical switches could
therefore distinguish alpha-equivalent programs.

The correction opts **only logical scalar semantic kernels** into lexical
identity. Aggregate program identities remain unchanged. Free variables retain
their names, canonical bound names cannot capture them, and nested shadowing
uses the existing `Expr::child_binders` lexical edge contract. Memoization keys
include only relevant free bindings and lexical depth, avoiding multiplication
of visits under irrelevant enclosing scopes. Digest equality is followed by
full structural equality; hashes never establish semantic equivalence alone.

The exact fraction producer is `matrix_list_aggregates.py::reduce_row_predicate`:
each sibling fraction builds the same present-entry map with a fresh callback.
Actual final physical diagnostics show **four presence-denominator kernels
becoming one**, and **two equivalent filter kernels becoming one**. These are
diagnostic receipts, not private-plan assertions in the public regression tests.

The denominator was not a major hotspot: existing
`list_aggregate/direct.rs` already counts Boolean predicates with bitmap
`true_count`. Distinct predicate maps still dominate these fraction pipelines.

## Planning cost and profiles

An initial unbounded normalization regressed the existing retained coordinate
callback-DAG test from 5.74 s to 8.27/8.32 s. macOS sampling identified the added
summary/normalization work. That candidate is superseded.

Final optional canonicalization has a general work budget: at most 1,024 unique
input nodes and 4,096 context rewrites. Larger programs keep the original exact
identity, safely missing some CSE opportunities. No value/effect semantics are
weakened. This avoids amplifying preparation of generated programs. Budgets
count nodes/contexts, not literal payload bytes.

Final DAG call time is **5.78 s**, matching the 5.74 s baseline. The final
post-fix sample has no alpha-normalization frames in its hot path. Sampling
windows overlap different compilation phases and are not a complete disjoint
CPU accounting; timings and source analysis supply the accompanying evidence.

## Validation and provenance

- Final normal kache/maturin release build: `budget-build.log`.
- **90 public tests pass**, including independent threshold/filter/capture/
  missingness/fold/error/retained-DAG contracts plus existing array, bind,
  coordinate and scalar-extrema suites: `evidence/budget-final-public-tests.log`.
- The same 90 tests pass on the exact baseline.
- **5 selected RNG-scope tests pass**, with 36 unrelated tests deselected:
  `evidence/budget-random-scope-tests.log`.
- Initial missing-fixture failures in `test_bind.py` were environmental. The
  exact upstream source was supplied without production changes, then the full
  suite passed. Source SHA256 is recorded in `provenance.json`.
- Native module origin, exact source fingerprints, CPU model, build settings and
  baseline artifact receipt are saved. Normal builds used an exclusively
  released target and a dedicated environment; compiled native artifacts were
  never manually copied or cached.

`measure.py` prepares stored native sparse-entry matrices outside timing. Final
shape is 200,000 by 2,500 (500 million slots). Four/twelve fraction pipelines
reduce every result field to a checksum. The filter-feature pipeline retains
filtered entry records and independently computes their count, then sums their
values/counts. Expected results come from independent Python formulas over the
periodic input, including filtered entries and missing values. Every run checks
the result. Input manifests and file SHA256 receipts are retained.

Each fresh worker performs one warmup and three measured iterations, with no
build/test/profile work overlapping final timings. Timings include reading and
query planning; imports and the discarded warmup are outside the final median.
CPU is process/thread RUSAGE_SELF, and RSS is the maximum process RSS (macOS
bytes converted to GiB). These are local warmed throughput measurements with
uncontrolled OS cache/frequency; there is no fresh Hail comparison.

One extra confirmation campaign overlapped a 0.4-second input-manifest hashing
read. It is retained under `discarded-confirmation-metadata-overlap` and excluded
from the summary. A clean reversed-order confirmation was then run. Original
pre-budget native receipts remain identifiable by their different artifact SHA;
only `final500m` is the final-source performance gate.

## Remaining work

This fixes identity, not arbitrary reducer fusion. DataFusion still cannot share
an expression hidden inside two **different** opaque kernels. Naively extracting
inputs can defeat existing map/reduce fusion and add intermediate arrays.

A stronger next target is the explicit plain-row-fraction exclusion from the
existing multi-field coordinate aggregation route in `matrixtable.py`: it exists
to retain the row fraction's empty-input NaN contract. Reusing that existing
fusion route while preserving all empty/filter/missing semantics deserves
profiling before inventing another aggregation implementation.
