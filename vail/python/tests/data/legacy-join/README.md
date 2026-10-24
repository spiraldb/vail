# Hail partition-join fixture

`hail-join-fixtures.tar.gz` contains genuine Hail 0.2.139 tables, not an imitation
writer. `generate.py` is the exact generation script (its output directory is
specific to the profiling VM). `expected.json` records Hail's own public join
and collect results. The archive's `manifest.json` authenticates the input files
and oracle with SHA-256 hashes. The tests use public Vail read, join, count,
filter, annotate, and collect APIs.

Inputs cover mismatched partition ranges, duplicate/missing keys, both signed
Int64 endpoints, an empty input, and keyed value projections. Each join kind
is checked for complete row multiplicity as well as force-count cardinality.
