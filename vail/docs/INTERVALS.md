# Interval support

`tinterval(point_type)`, `Interval`, and `interval(...)` support all current Vail point types: booleans, integers, floats, strings, arrays, sets, dictionaries, tuples, structs, calls, ndarrays, and nested intervals. Explicit point types allow missing endpoints. A present interval with a missing endpoint remains distinct from a missing interval.

The logical type encoding is `{"interval": point_type}`. Arrow and Vortex store a struct with typed start and end children and two boolean inclusivity children. Reserved child names retain the interval identity through DataFusion; persisted logical type descriptors also guide key encoding when Iceberg renames nested fields.

Python constructs lazy expression IR. Rust lowers `contains`, `overlaps`, and direct interval comparisons into DataFusion comparisons, boolean expressions, and `CASE`. A typed canonical key expression supplies the point ordering shared with sorting, grouping, and fragment bounds. `CASE` preserves Hail's boundary evaluation order: a missing first boundary yields missing, while a false first boundary skips the second boundary. Arrow struct construction and projection use the existing native expression infrastructure.

Intervals work in row and global expressions, grouping, key sorting, comparison, type expansion, and table write/read. `expand_types` emits `start`, `end`, `includesStart`, and `includesEnd`, matching Hail. Interval point values are retained as their original types. Interval-specific joins and Python interval-restricted read options remain part of the existing unfinished Table API work.

Tests cover every supported point type, endpoint inclusivity, missing intervals and endpoints, grouped keys, global values, direct predicates, union/distinct, explode, type expansion, a three-file Vortex/Iceberg roundtrip, and an interval-key range selection spanning two data files. Boundary expectations were checked against the pinned Hail source/runtime. Hail does not define ndarray ordering; intervals of ndarrays use Vail's existing ndarray key order.

`Locus` and reference genomes are deferred in [OPEN_ISSUES.md](OPEN_ISSUES.md).

## Verification (2026-09-29)

- `python -m pytest python/tests conformance/test_harness.py -q`: 302 tests and 19 subtests passed.
- `cargo test -q -p vail-execution -p vail-python -p vail-table -p vail-storage -p vail-ordering`: 69 tests passed.
- `cargo check -q -p vail-python` and `git diff --check` passed.
