# Matrix PL dosage / array arithmetic

Source candidate: `56b23d53ca99a91a177b858dd5efd0f5a73adedc` (Rust change `5af02ef6`).

The upstream `test_matrix_table_array_arithmetic` benchmark filters the existing `profile25_mt` to biallelic rows and computes `hl.pl_dosage(mt.PL)` for every entry. It is not a benchmark of arbitrary array operators.

Two general changes remove incidental list work:

* `hl.bind` is encoded in the frontend as `map([tuple(arguments)], callback)[0]`. The physical compiler recognizes this exact singleton construction and evaluates its binding and callback without temporary lists, flattening or gathering.
* Nonrandom scalar operands of elementwise array arithmetic are bound outside the element lambda. Previously `probabilities / sum(probabilities)` captured the probability lists and recomputed their sum inside the lambda. Missing/empty arrays retain lazy evaluation and do not evaluate the scalar operand.

No custom numeric reduction or PL-dosage kernel was added. Existing DataFusion kernels evaluate the callback.

## Correctness and artifacts

Six new public tests pass on the prior wheel as semantic oracles. The exact candidate wheel passes 86 public tests covering bindings, nested scopes, missing/empty lists, structs, CASE laziness, array arithmetic/map/methods, genotype likelihoods, stream aggregate captures and aggregate type inference.

Candidate release wheel SHA256: `7cf54966fc6d52c31b523e420ba97e54ccb3fc2b1483649ecb4d2950309fa3d3`.
Native extension SHA256: `8f6f07458ad85a4a53e10645411d6f9707eb836f02c9fa9c6111e00000faa1a0`.
Built with Rust 1.98.0 and kache. Python source checked byte-for-byte against the wheel.

## Measurement status

Waiting for unrelated laptop builds to finish; no timing or profile claim yet.
The prepared driver uses the existing `/tmp/entries-profile25.mt` fixture, warms the query once, then measures wall time and process user+system CPU around a second forced evaluation. Reported peak RSS is process-wide high-water memory, including warmup, rather than a sampled query-only peak.
