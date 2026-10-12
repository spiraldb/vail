# Interval endpoint layout verification

Intervals now store `start, start_exclusive, end, end_inclusive`; public constructors/accessors and Hail endpoint ordering retain their semantics. Initial arrays, expression construction, globals, Arrow localization, string formatting, interval-read bounds, and key encoding use the new layout.

- Full Python suite: **643 passed, 1 skipped, 19 subtests passed**, zero failures. [Log](python.log).
- Rust table: **44 passed**, zero failures. [Log](rust-table.log).
- Rust ordering/execution: **62 passed**, zero failures. [Log](rust-ordering-execution.log).
- Vortex comparison audit: **12 passed**, zero failures. [Log](vortex-audit.log).

New regressions assert Arrow field order, public flag accessors, missing parents, ordering of inclusive long intervals before exclusive short intervals, and read/write round trips. All six Vortex comparison operators are compared with the Hail endpoint tuple order across a matrix of points and flags. General missing-value/float semantics remain governed by existing documented policies.

The physical interval schema changed. Existing files written with the old interval layout need rewriting; this change does not implement automatic migration. Canonical key bytes preserve existing endpoint semantics.
