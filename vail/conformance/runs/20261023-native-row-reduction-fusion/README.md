# Shared native row-reduction contexts

Source base ec909af. This development candidate admits plain named row fractions
into the existing shared row-list context path and restores their existing empty
NaN identity after reduction using ordinary coalesce. It does not introduce a
new aggregate kernel or claim one primitive pass across every reducer.

## Source correction

The existing named row aggregate bundle shares filtered/projected entry contexts
and retained reducer DAGs. Its primitive reducers still execute separately. The
collection-group multi reducer similarly shares grouping but invokes DataFusion
GroupsAccumulator::update_batch separately for each reducer. Reusing it with a
constant grouping key would add hashing, owner buffers and gathers; no evidence
justifies that route. Existing Boolean count reductions use Arrow bitmaps.

## Baseline diagnostic

`diagnostic.py` runs public queries against the normally built baseline at
/private/tmp/vail-export-join-integration-env. `baseline-diagnostic.log` retains
all samples. Native sparse input contains filtered entries and missing values.
No build or other timed workload overlapped this diagnostic.

80M entries, four fractions, excluding first sample per mode:

| Shape | Wall seconds | CPU seconds |
|---|---:|---:|
| Four independent named fields | 0.3323–0.3373 | 2.590–2.596 |
| One struct containing four fields | 0.1282–0.1283 | 0.963–0.969 |

The struct form is approximately 2.6 times faster. Empty public direct fractions
return NaN while struct fractions return missing; the candidate explicitly
preserves the direct contract without changing the nested contract. This is a
diagnostic opportunity, not yet a validated candidate speedup.

Eligibility already excludes RNG, lookups, position-sensitive callbacks, scans,
nested explode/fold/array aggregation and unsupported aggregate options. Existing
agg_filter scope lowering remains in charge of predicate/error demand. Only
unwrapped plain named fractions receive the empty identity restoration. Public
tests cover sparse/empty/mixed numeric reductions and dead errors on filtered
entries. Candidate execution validation and larger timings are still pending a
normal build; the shared local Cargo target is currently owned by the integration
agent. No main changes have been made.

A second baseline diagnostic extends to 500M entries (baseline-500m-diagnostic.log):
independent named fractions take 1.697–1.940 seconds and 19.17–19.60 CPU seconds;
shared struct fractions take 0.644–0.658 seconds and 7.05–7.10 CPU seconds.
All numerical outputs match. The five new public semantic tests pass against the
unchanged baseline (0.31 seconds); this establishes the oracle before candidate
execution, including the guarded invalid cast on filtered entries.
