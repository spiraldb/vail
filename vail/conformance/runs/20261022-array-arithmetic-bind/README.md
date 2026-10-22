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


## Verified follow-up profiles

Production `ec147f0b` was built normally using Rust 1.98.0 release, kache and this worktree's own Cargo target. No compiled extension was copied or saved as a build-cache workaround. The normal editable build generated native SHA256 `8c6a8cbe178f34db4efa2a2f599706b603a3fba5d2aa5bfd6a0d4c654d45e889`. The exact build passed 86 Python tests. Its fresh 1 kHz `profile-after` capture contains **no samples in `make_array_inner` or list `take`**, and does execute `ScopedSwitch`. This demonstrates that the intended singleton-list removal now actually activates.

The second small change, production `29caf51d`, expresses PL dosage directly as `(p1 + 2*p2) / (p0 + p1 + p2)` using unnormalized probabilities and native DataFusion arithmetic. It avoids producing a normalized list merely to extract two values. Strict scalar addition naturally propagates a missing probability at any position. The probability transform no longer adds an unnecessary explicit null guard around operators that already propagate nulls. Cache bounds, negative-index wraparound, wrong-length errors and underflow-to-NaN behavior are preserved. This Python-only change uses the normally built, unchanged Rust extension above. The combined candidate passes **91 Python tests**, including all missing positions, cache boundaries, malformed lengths and extreme underflow.

The `profile-dosage` capture confirms that **`array_sum` and `array_any_match` also disappear**, alongside singleton `make_array` and list `take`. The same existing fixture has 2,535 columns and 24,054 biallelic rows: 60,976,890 entry evaluations per query. Each capture contains startup plus two query evaluations. Other laptop work was active; these profiles establish code-path removal and CPU attribution, not a controlled runtime comparison or a new Hail/Vail ratio.

| CPU attribution | Broken matcher | Arrow matcher fixed | Direct dosage |
|---|---:|---:|---:|
| `make_array_inner`, inclusive | 27.66% | no samples | no samples |
| `array_sum`, self | 1.33% | 2.45% | no samples |
| Arrow `filter_struct`, inclusive | 6.75% | 11.60% | 6.66% |
| CASE result merge, inclusive | — | 5.55% | 2.07% |
| native power, inclusive | 4.64% | 8.15% | 13.53% |

These are proportions with different denominators; an increased share need not mean increased absolute work. Inclusive rows overlap and must not be summed. The profile files retain raw captures and saved symbols; symbolicated captures canonicalize native function identities. The profiling driver intentionally does not rebuild fixtures or rerun Hail.

### Why CASE still copies

Before direct dosage, CASE sites came from probability-null masking, cache lower-bound errors and negative-index wrapping, the scalar-arithmetic empty-array guard, normalization's `coalesce`, dosage array-null masking, dosage arity checking, and dosage missing-element masking. `gp_dosage` has its own guards but is not called by this benchmark. Zero denominators use native floating division, with no separate CASE guard.

DataFusion 55's `case.rs` projects the union of referenced columns (lines 1046 onward), then `filter_record_batch` filters those referenced columns for selected branches (346 onward; selected branch at 853). A referenced probability list/struct is still a nested Arrow array. Filtering copies its relevant children and validity bits, visible as `filter_struct -> filter_array -> MutableArrayData -> build_extend_null_bits` / primitive extend. `ResultBuilder::finish` merges branch results back into original positions. We do not claim DataFusion copies every unused column: it prunes unused columns before this filtering.

Direct dosage removes the normalization, empty-array arithmetic, list-missingness and redundant null guards; cache index/error semantics and the three-likelihood arity guard remain. This capture has function symbols but no inline/source-site attribution, so its aggregate CASE cost cannot honestly be assigned to individual Python guards.


Final public Rust semantic regression also passed on the normal isolated release target: **1 passed** (Arrow-scalar and JSON-IR zero-index construction forms, null-containing batch evaluation). No physical-plan topology assertion is used. Full build/test logs are retained.
