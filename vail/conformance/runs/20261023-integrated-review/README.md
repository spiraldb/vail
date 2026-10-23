# Reviewed compatibility and count integration

Integrated source revision: `79bc04b90168d9d19a49ad1e9e596135098d1dc1`.

Includes VDS legacy routing, Python API conveniences, nested numeric coercion and
Mapping inference, matrix entries force count, ordering-demand optimization, and
persisted benchmark exclusions. All source branches start at frozen benchmark
revision `e79cf6ff`. No cloud artifact or benchmark source was changed.

## Validation

- Normal isolated `maturin develop --release --locked`, kache, Python 3.12.12,
  two Cargo jobs, `target-cpu=native`: 21m38s. Native SHA and module paths in
  `provenance.json`. The final material followup is Python-only; native source is
  unchanged from the recorded build. No native artifact was copied or cached manually.
- Combined 15-file public suite: **309 passed, 13 failed** in 16.99 seconds.
  The exact 13 failures reproduce on frozen main's native environment; see
  `matrix-frozen-baseline.log`, provenance, and machine-readable comparison.
  There are **no new failures** in that suite.
- Integration/aggregate public suites: **67 passed** in 0.71 seconds.
- Real benchmark-policy CLI subprocess tests: **5 passed** in 0.498 seconds.
- The numeric branch's two known broader-suite failures were independently
  reproduced on frozen main: unstable long-array correlation and dictionary-metadata
  cast for empty-struct grouped keys; see `frozen-baseline-failures.log`.

The initial combined run exposed five failures relative to frozen main. Four
were internal aggregate record constructors that relied on Python dictionaries
being treated as structs. They now explicitly construct native struct expressions
(stats, call_stats, related genomics records, corr/linreg/downsample and ordered
take). One fixture now explicitly constructs column structs. Public regressions
cover record result types and values, nested numeric dictionary/struct rename,
automatic frozendict inference/pickling, and ordered collect/head/scan around count.

Ordering tests use public actions and values, not private rewrite helpers or
physical operator names. Collection aggregation asserts multiplicity because its
ordering is unspecified; table collect/head/scan assert observable ordering.

Combined Rust public integration checks also passed: `consumer_actions` (6),
`legacy_expansion_counts` (7), and `logical_order` (1). Normal kache release build
completed in 21m15s for this feature graph; test outcomes are in `rust-tests.log`.
The nullable shared-union regression preserves multiplicity across repeated
public table occurrences; no private optimizer helper is called by these tests.
