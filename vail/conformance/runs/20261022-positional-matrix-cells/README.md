# Positional matrix cells

Matrix cells now contain only `value`. Their list position associates each
entry with shared column metadata; no column ID is stored or materialized per
cell. IDs occur once per shared column record. A null entry struct represents
absence; a defined struct with null fields remains present. No presence flag
was introduced.

The change covers Python matrix constructors/transforms, native VCF/BGEN/PLINK
readers, aligned entry maps, and sparse-to-dense conversion. New snapshots use
version 4. Version 3 snapshots remain readable through a projection that drops
repeated cell IDs while preserving nullable entry payloads.

An audited `character_length` expression can also borrow shared column metadata
rather than copying its payload once per row. The allocation regression checks
that increasing shared payload width does not cause proportional copies.

Validation uses an optimized native build on the Apple M3 Max laptop:

- 807 Python tests pass across 44 matrix/importer-related files, including
  physical schema checks, filtered entries, valid all-null/empty structs,
  repeated/permuted columns, and old/new persistence.
- 9 Rust layout tests, 5 allocation tests, 5 aligned-map unit tests, and the native VCF layout test pass.
  One manual throughput benchmark is ignored.
- 23 broader test cases were explicitly excluded only after all 23 also failed
  against the old implementation. Their baseline logs and classification are
  included. These cover existing scalar subquery/kernel errors and a stale
  unsupported-operation expectation. This change does not fix those failures.

The baseline used for failure classification is the earlier legacy worktree,
not an exact rebuild of the tested main base; see provenance.json. No claim is
made that the entire repository test suite is green. The initial interrupted
broad run and final passing run are both retained.

This change proves removal of the redundant per-cell array. Legacy Hail read
performance comparisons are tracked separately and must be rerun on this layout.
