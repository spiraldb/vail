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


## Profile diagnosis (October 22, before Arrow-literal matcher fix)

The earlier candidate's semantic tests passed, but the intended singleton-binding optimization **did not activate for Python expressions**. Python stores literal indices as `RuntimeLiteral(ScalarValue::Int32(Some(0)))`; the matcher only recognized the older JSON `Literal` representation. The scalar-arithmetic hoist therefore introduced additional temporary singleton lists without removing them. Candidate `ec147f0b` fixes this exact representation mismatch; post-fix validation is pending.

A 1 kHz samply capture of the actual existing `profile25_mt` dosage query on the validated `56b23d53` build confirms the cost. The driver includes startup and two evaluations (warmup plus forced query). Other laptop work was active, as explicitly authorized; this capture is for attribution, **not a wall-time comparison**. CPU proportions below weight each sample by `threadCPUDelta` (100.59 sampled CPU seconds total), rather than counting collapsed blocked-thread samples as CPU work.

| Symbol | Self CPU share |
|---|---:|
| Arrow `build_extend_null_bits` closure 1 | 10.25% |
| `_platform_memmove` | 9.47% |
| Arrow `MutableArrayData::try_extend` | 8.46% |
| Arrow `try_extend_offsets<i32>` | 4.24% |
| Arrow primitive f64 extend | 2.99% |
| Arrow struct extend | 2.72% |
| Arrow bit-mask copying | 2.64% |

DataFusion `make_array_inner` accounts for **27.66% inclusive** CPU, whereas power calculation accounts for **4.64% inclusive**. Inclusive figures overlap children and must not be added to self figures. No `ScopedSwitch` frame appears in this capture. Copy stacks identify singleton-array construction, list `take`, lambda capture expansion and CASE batch filtering as their callers. This explains why correctness tests alone were insufficient to validate the intended optimization.

Raw, saved-symbol and canonicalized symbolicated profiles are retained. Unknown native addresses remain unknown; no source lines or inline frames were invented. `analyze.py` reproduces CPU-weighted attribution.
