# Review of the uncommitted Vail implementation

Three GPT-6 Luna reviewers examined execution/planning, storage/ordering, and the Python API/Arrow bridge against outer repository HEAD `d87680c`, including untracked implementation files. The review was read-only. This was a broad sampled review, not exhaustive verification of every changed line.

## Findings

| Priority | Finding | Evidence | Recommended remedy |
| --- | --- | --- | --- |
| P1 | Standard deviation suffers catastrophic cancellation. | Runtime: `[1e12, 1e12+1, 1e12+2]` produces NaN instead of approximately 0.816497. | Reuse DataFusion's stable population standard deviation accumulator, preserving Hail empty-result semantics and scan state merging. |
| P1 | Tuple arity mismatch silently changes data. | Runtime: one element supplied for a two-element tuple becomes `(1, None)`; serialization uses truncating `zip`. | Validate exact tuple width and recursively validate members before Arrow conversion. |
| P2 | Indexed reads open every candidate file for each output partition. | Runtime: selecting partition 0 fails after deleting only partition 1's file. | Intersect indexed candidate ranges with the requested execution partition before opening files. |
| P2 | `head(n)` fully scans sources with unknown partition counts during planning. | Code-confirmed: legacy preparation calls `partition_counts` before constructing limits. | Stream ordered input through a lazy limit rather than pre-counting every row. |
| P2 | Arrow struct names collide with Vail tuple encoding. | Runtime: a legitimate struct field `__vail_tuple_0` is inferred as a tuple and an explicit struct schema is rejected. | Discriminate physical encodings with metadata, rather than field names alone. |
| P2 | Scalar `eval` accepts an empty row-indexed struct. | Runtime: `eval(t.row)` returns `Struct()` for a zero-field row. | Check expression row scope, not just referenced column names. |

## Design clarification

Expression-valued `filter_intervals` eagerly evaluates interval inputs. The Python reviewer confirmed this behavior, but primary review checked upstream Hail `hail/python/hail/methods/misc.py:415`, which also calls `hl.eval(intervals)`. This is not a Hail parity regression. Making it fully lazy would be a deliberate change in implementation/behavior to meet the desired lazy architecture.

## Validation and limits

The execution reviewer ran the focused scan suite: **19 tests passed**. Other checks used small reproductions or source inspection; no full suite or native rebuild was run for this review. Intentional divergences and deferred features were excluded. Findings have not been fixed as part of this review.

Detailed evidence: [execution](execution.md), [storage](storage.md), [Python and Arrow](python.md).
