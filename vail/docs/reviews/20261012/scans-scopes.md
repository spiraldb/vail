# Scan scope review

Compared scan implementation and related expression typing/state code against baseline `7689173`, with attention to nested scopes, prefix boundaries, and accumulator snapshots.

## Findings

- **P2 — Nested scan folds could collide with a user field and fail planning.** The fold lowering originally named lexical values `__vail_scan_variable_{i}` and appended them to the input schema without checking existing row/global fields. A user field with that name (for example `__vail_scan_variable_0`) therefore produced `invalid expression: duplicate struct field "__vail_scan_variable_0"` for `scan.array_agg(lambda item: scan.fold(... item + table.__vail_scan_variable_0 ...), table.a)`. The fix in [nested_scan.rs](/Users/danielking/projects/vail/vail/crates/vail-table/src/nested_scan.rs:187) checks each candidate against the evolving schema and resolves lexical aliases back by their exact generated names at line 220. Regression coverage is in [test_scan.py](/Users/danielking/projects/vail/vail/python/tests/test_scan.py:333), including references to both the lexical array variable and the colliding row field.

## Verification

The regression first reproduced the duplicate-field error against the stale native extension. After the extension timestamp advanced to the rebuilt binary, `test_nested_scan_fold_internal_names_do_not_shadow_user_fields` passed (1 passed in 10.41s).

Before this collision was identified, focused tests passed (`4 passed, 20 deselected`) for explode/filter, nested group_by/array_agg, current-row projection, and fold initialization/partition merge. A set-valued `scan.explode` smoke case passed for populated, empty, and missing sets. The exclusive row frame is `UNBOUNDED PRECEDING` through `1 PRECEDING`; the partition carry uses preceding partition summaries, with an inclusive array-scope validation window. `GroupsAccumulatorAdapter::evaluate(EmitTo::All)` is followed by state restoration via its merge API, with a dedicated Rust test covering repeated snapshots and state restoration. No build or full suite was run.
