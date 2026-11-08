# Same-program scalar tiling: rejected

Diagnostic source `529f5f2b4cc918a3c94229f9baeb980658c834eb` evaluates the **same prepared public Vail expression** either over the whole input batch or in contiguous 2,048-row slices. Tiled timing includes preallocating and assembling the final Float64 output. There is no new runtime operator, bind, evaluated-value cache or default policy. All source owners remain retained. A row limit is not a byte/element bound for arbitrary variable-length lists.

The total fixed-three-element fixtures cover a mapped probability list with several scalar index consumers, a mapped polynomial list with scalar consumers, and a cheap no-list arithmetic control. Fixtures, compilation and independent full-result validation are outside timing. Each case/size has four fresh workers per schedule in balanced whole,tiled,tiled,whole,whole,tiled,tiled,whole order, three warmups and five evaluation intervals of at least 20 ms. CPU is unmeasured.

| Program | Rows | Whole evaluation | Tiled evaluation | Tiled change |
|---|---:|---:|---:|---:|
| Probability / weighted ratio | 8,192 | .4746 ms | .5059 ms | +6.588% |
| Probability / weighted ratio | 1,048,576 | 61.457 ms | 66.331 ms | +7.931% |
| Polynomial / scalar sum | 8,192 | .2641 ms | .2926 ms | +10.793% |
| Polynomial / scalar sum | 1,048,576 | 34.007 ms | 38.290 ms | +12.595% |
| Cheap column arithmetic | 8,192 | .002415 ms | .004101 ms | +69.775% |
| Cheap column arithmetic | 1,048,576 | .2818 ms | .5209 ms | +84.848% |

All adverse results are retained. This rejects default tiling: it preserves the same intermediate operations and adds scheduling/assembly work. These observations do not establish a Hail or actual PL-dosage comparison, a default eligibility proof, memory reduction, source-owner release, unique CPU attribution or completion of shared probability-list/index fusion.

Every worker evaluates both schedules outside timing and checks exact whole-versus-tiled valid-value bits and nulls. Polynomial and cheap results also match an independent bit-exact oracle. The independent nonlinear probability reference uses a fixed eight-epsilon absolute/relative bound; observed maximum difference was one ULP. Its reference domain is finite nonnegative inputs no larger than 90, with a strictly positive finite denominator. The comparison rejects NaN/Inf actual values against those finite references, and expected-null positions are checked separately.

An initial whole-program trial failed the original bit-exact independent probability assertion by one ULP before timed samples. It is preserved as a validation failure, not a tiling regression or timed cohort. The difference was not attributed to a particular kernel. Requiring exact whole/tiled behavior plus a tight independent mathematical check changes diagnostic validation only; it grants no approximate production semantics. A preceding malformed polynomial constructor compile error is also preserved.

All 48 final workers validated, exited zero and had owned groups absent; source and executable hashes stayed unchanged. No builds/tests overlapped timing. Full cold preparation/first-evaluation observations are retained; faster first evaluation in some large cases does not cancel the repeated-evaluation regressions above.

Archive `scalar-tiling-evidence.zip`: SHA-256 `83711fe88aad58bf416ee6702210976fe259d5195a445d9b4c719841efcbbe8d`, 74,743 bytes, 160 payloads. Exact membership and every payload length/hash were verified. It contains source, controllers, failures, build logs, pins, complete observations/oracle summaries and cleanup receipts; no compiled artifacts or environments. Private evidence: `/private/tmp/vail-general-list-20261108/scalar-tiling-final` and the retained failed `/scalar-tiling` attempt. Independent review cleared the diagnostic/source/raw evidence and rejection; no production tiling was admitted.
