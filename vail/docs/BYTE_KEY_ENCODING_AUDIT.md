# Byte-key encoding removal audit

Status: implementation removed; final optimized-wheel Python validation passed (1,026 tests, one skip). Custom byte encodings must not implement value equality, ordering, grouping, joining, sorting, or partition-bound comparison. Use typed Arrow/DataFusion operations; reject unsupported dictionary/set keys. Arrow IPC for storing typed bounds is serialization, never a lexicographic comparison representation.

| Area | Sites found | Owner / replacement |
|---|---|---|
| Execution ordering and expressions | ordering.rs CanonicalKeyExpr/NullableCanonicalKeyExpr; logical_expr.rs encoded and hex key UDFs; interval and fallback binary expressions; execution/lib.rs physical_ordering | Matrix performance agent: native typed expressions and Arrow comparators |
| Join planning and execution | join_interval.rs encode_prefix/scan_bounds; joins.rs; lookup.rs; zip_join.rs | Join agent: typed source bounds, typed min/max discovery, native join keys |
| Grouping, scans, comparison and plan adapters | logical_aggregation.rs; aggregation.rs; logical.rs; nested_scan.rs; comparison.rs; plan.rs | Parent: remove encoded fallback and ensure unsupported types error |
| Collection expressions | Python collections.py/array_methods.py canonical_key helpers | Matrix performance agent: native/library equality, no encoded string keys |
| Storage and fragment selection | fragment_index.rs KeyType/KeyValue::Canonical; local_table.rs; storage/lib.rs; interval_read.rs | Interval agent: typed persisted bounds, Arrow comparison, versioned metadata |
| Ordering crate and tests | vail-ordering encoding functions and comparison tests | Remove after consumers migrate; preserve unrelated Call codec and type validation |

The custom execution ordering module and ordering-crate encoders have been removed. Joins, grouping, scans, collection expressions, and fragment-bound comparisons now use typed values. Persisted bounds use Arrow IPC only for transport; readers decode and cache typed values before comparison. A runtime-source search for `canonical_key`, `encode_key`, `NullableCanonical`, and `CanonicalKey` found no remaining implementations. Ordinary Binary data and DataFusion’s internal implementation remain outside this audit.

Verification required: supported primitive/nested key behavior, missing values, partition overlaps, boundary inclusivity, duplicate joins, grouping, collection equality, and explicit Set/Dictionary ordering/key rejection. Search all runtime sources after migration to ensure no custom encoder consumers remain; built-in DataFusion internals and ordinary Binary data are outside this custom-encoding purge.

## Validation so far

- Integrated Rust library tests: 103 passed; focused storage roundtrip tests: 9 passed; Vortex comparison audit: 12 passed. The subsequent Arrow list-sum kernel has its own focused Rust contract test.
- Join, lookup, and partition checks: 77 passed. Scan checks: 31 passed. Grouped/composed aggregate checks: 21 passed. Collection/native-policy checks: 59 passed.
- Review fixes cover empty-struct grouping, unordered set equality, nested set membership, and public missing interval endpoint handling. Local interval checks: 40 passed; source-rooted upstream Locus checks: 10 passed.
- The last broad Python run reached 972 passing tests, one skip, and five failures before stopping. Those failures received focused fixes; the final complete rerun is still required. Partition-selection semantics are being checked against stock Hail before acceptance.
- Targeted optimized measurements: 1M-row inner join 0.0709 seconds versus fresh Hail 0.2163 seconds; 1M-row prefix scan 0.201827 seconds. Matrix entry-sum profiling and the final full benchmark rerun are pending. Development-build timings are not used for comparisons.
- Follow-up review confirmed stock Hail's dense partition numbering after key filtering. Corrected both operation orders, with missing-file regressions proving unrelated fragments are not opened: storage/index checks 11 passed, combined storage/interval checks 70 passed, exact upstream Matrix interval-partition check passed. Provider intersection checks distinguish a malformed comparison (error) from a valid disjoint intersection (empty).
- Matrix entry annotation and positional contexts now use DataFusion `arrays_zip` rather than capturing and repeatedly indexing the owning cell list. Focused public Matrix checks passed 9 tests, including reordered columns and empty entry lists. Matched optimized entry-sum measurements improved from 0.149875 to 0.116710 seconds for 100K × 10 and from 0.536261 to 0.337611 seconds for 25K × 100; the wide case still trails Hail.
- Final complete Python suite: 1,026 passed, one skipped, 19 subtests, zero failures/errors in 62.08 seconds. All 42 installed Python modules hash-match workspace sources; native extension SHA-256 `0ea9e6446a0592417a865faf1876eb849dd65d2e92864c196a0d58c61ce9c44b`. See `../conformance/runs/20261013-final-python-release/STATUS.md`.
