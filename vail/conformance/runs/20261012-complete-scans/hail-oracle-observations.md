# Hail runtime observations

Verified against the locally built Hail JVM runtime with Java 21 and Spark
`local[2]` on 2026-10-12.

- `scan.array_sum` over `[[1, 2], [3]]`, split across two partitions, raises
  `HailException: mismatched lengths in ArrayElementsAggregator`. This includes
  a mismatch in the final input row/partition. The same holds for
  `[[1, 2], [3], [4, 5]]`.
- Scan scopes with no aggregation in their bodies are rejected by Hail;
  `scan.group_by(idx % 2, int32(42))` and `scan.array_agg(lambda x: x + 1, [idx])`
  did not produce valid results. Vail retains its requirement for an aggregate
  in a nested scan body.
- Hail statistics retain a cancellation-prone sum/sum-of-squares formula.
  On `[1e12, 1e12 + 1, 1e12 + 2]`, Hail total `stats` returned stdev `0.0`.
  Its exclusive scan stdev values were `[NaN, 0.0, NaN]`. The mathematical total
  stdev is approximately `0.816497`. A stable variance algorithm would be a
  deliberate numerical semantics change; it was not introduced in this work.

Floating-point reduction order can further affect these results across engines
and partition layouts. The prior broad review's mathematical-accuracy finding
should not be read as evidence that Hail uses a stable variance algorithm.
