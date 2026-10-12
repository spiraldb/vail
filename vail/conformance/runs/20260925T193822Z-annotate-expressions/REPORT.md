# test_annotate expressions

The unchanged Hail Tests.test_annotate passes. All 95 selected upstream Table
cases now pass; none fail. This is not the entire Hail test universe.

Implemented the remaining expressions it exercises:

- Array.filter: typed predicate IR, lexical scope checking, shared map/filter
  lambda lowering, and native DataFusion array_filter execution. Missing
  predicates discard elements; order and outer missingness are preserved.
- Set literals: distinct recursive set type, canonical sorted/unique Arrow list
  input, Python set localization, globals, and Viceberg type round-tripping.
  A reserved list element field name preserves the logical set type through
  DataFusion paths that discard arbitrary field metadata.

Three Luna agents handled filter implementation, Python set codecs/tests, and
independent type/metadata review. Parent review consolidated duplicated lambda
lowering, corrected test expectations, fixed nested-set decoding, and repaired
set type loss through scalar conversion and DataFusion joins.

Recorded verification:

- 150 Python tests and 19 subtests passed.
- 42 Rust tests passed.
- 9 recorder self-tests passed.
- 4 native conformance fixtures passed (source-derived expectations).
- 95 selected upstream cases passed, including test_annotate unchanged.
- 4 Hail oracle fixtures unavailable because hailtop.version is missing.
- No source drift, extension change during tests, or assertion failures.

Scope limits: general set operations are not complete. Array/struct set members
cannot yet localize as hashable Python objects. An additional exploratory check
found direct floating set expression equality inherits DataFusion's distinction
between signed zeros; this is not covered by test_annotate and remains a Hail
compatibility gap. Table._same's float comparison and set literal deduplication
already treat signed zeros equally. Passing the selected suite does not establish
full set or Table API parity.

Full commands, hashes, outputs and timings are stored beside this report.
