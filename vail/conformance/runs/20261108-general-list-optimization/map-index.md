# Eliminate a mapped list before one indexed read

Production candidate `30783876b24484cc80549d175faf15bab765f676` rewrites `map(xs, x => f(x))[i]` to `f(xs[i])` when the source is an ordinary canonical primitive array, the index is a nonnegative literal and `f` is a total, strict, null-propagating expression with exactly one use of `x`. It uses existing expressions and kernels, without a new bind operator, value cache, interpreter or field-name specialization.

The finite proof admits primitive literals, negation/not, addition/subtraction/multiplication and comparisons. Missingness tests, multiple uses, captures, casts, division/power, lazy branches, errors and unknown operations retain the original map. Logical sets also retain that path even though their storage is list-shaped. Exactly one strict use preserves index-bounds demand, including multiplication by zero and null literals. Missing arrays propagate through the original indexed read without a CASE or selected-row gather.

Preparation certifies the existing DAG before allocating a replacement, counts use edges rather than unique variables, bounds the proof to 64 unique nodes/depth and rebuilds only the sole parameter path. Other expression Arcs are reused. Existing singleton-map/index bind normalization runs first and is unchanged. This does not fuse PL dosage's fallible, shared probability-list consumers.

## Public expression measurements

The same public Rust example is compiled normally against accepted main `ec0cca2ca` and the candidate. Five expression shapes at four sizes each use four fresh processes/version in balanced B,C,C,B,B,C,C,B order: 160 workers. Full independent Arrow value/null oracles precede timing. Five repeated-evaluation intervals per worker last at least 20 ms; preparation and first evaluation are separate. CPU is unmeasured.

| Expression | 1 × 3 | 64 × 3 | 64 × 128 | 1 × 65,536 |
|---|---:|---:|---:|---:|
| Arithmetic map then one index | 1.91× | 2.42× | 5.78× | 26.47× |
| Identity map then one index | 2.53× | 3.00× | 4.69× | 2.46× |
| Indexed map inside a real outer map | 1.39× | 1.79× | 3.70× | 14.54× |

These are repeated expression-evaluation speedups, not Hail or split-multi speedups. Warmed targeted preparation improves 29.5–45% for direct selected/identity maps and 6.5–18.1% for the nested case. Adverse observations remain: nested 1×3 cold preparation plus first evaluation rises 275.771→302.625 µs (+9.738%); shared 64×128 fallback warmed preparation rises 17.667→19.604 µs (+10.961%); shared 64×3 cold-plus-first rises 280.457→295.125 µs (+5.230%). Missingness-fallback evaluation rises .09–2.01%; shared fallback evaluation changes −.51% to +1.80%. No noise waiver removes these costs.

An earlier implementation rebuilt before declining and had larger preparation costs. Its separate 160-worker cohort is retained rather than presented as evidence for final source.

## Real installed-wheel controls

Normal maturin-installed candidate and accepted FloatFold wheels run the unchanged upstream Hail benchmark bodies on Vail's real native 1,000-row × 2,535-sample fixture with four input partitions. Eight balanced fresh workers per case use two warmups and five timed original force-count calls. These are medians of process medians, not a new Hail-engine or full-corpus comparison.

| Workload | Before wall | After wall | Wall change | Before CPU | After CPU | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Entry filter | 49.485 ms | 49.847 ms | +.732% | .193379 s | .193770 s | +.202% |
| Filter/unfilter | 106.738 ms | 106.785 ms | +.044% | .406020 s | .405400 s | −.153% |
| Array arithmetic / PL dosage | 55.306 ms | 55.526 ms | +.397% | .213455 s | .213743 s | +.135% |
| Split multi HTS | 162.706 ms | 162.387 ms | −.196% | .429688 s | .422913 s | −1.576% |

All adverse controls are retained. These controls do not establish actual-workload benefit or attribution to this rule. All 32 workers / 160 samples passed complete Arrow values and metadata outside timing; split also matched the established full reference. All process groups exited and all 13 input files passed final full-content hashes. Original force-count actions do not independently prove every payload field is forced.

CPU is whole-worker process CPU across threads; RAYON4/BLAS1 does not cap the native runtime's 32 Tokio workers. Peak RSS includes import/validation, and the sampled 6 GiB worker guard is not a hard process-tree limit. No timed RSS improvement is claimed.

## Correctness and allocation

Exact final source passes 137 Rust tests and its normal installed wheel passes 527 Python tests plus 10 subtests. The sole excluded correlation allocation test is the previously reproduced baseline failure. Public tests cover both lowering routes, missing lists/elements, slices/empty inputs, null ancestors, promotion/wrapping, retained owners, repeated reads, bounds/effect preservation, IEEE operand-order bits and logical-set fallback.

With fixtures, preparation and warmup excluded, growing a mapped child from 1,024 to 131,072 values leaves evaluation allocation at 1,680 bytes for a direct index and 2,793 bytes inside a real outer map. The same test against accepted source checks both full results before failing its resource guard: 36,415→4,197,951 and 37,398→4,198,934 bytes. All seven semantic tests, including the logical-set case, pass that baseline. This is allocation volume, not zero allocation, input-owner release, peak RSS or arbitrary list-byte bounds.

The first IEEE test's initial bit-exact NaN reference failed on both candidate and accepted source. With runtime-opaque oracle operands, the corrected same test passes both versions while retaining exact bits, without a tolerance or production change; no unique compiler cause is claimed. The preparation revision exposed a candidate-introduced logical-set error; the final rule explicitly requires logical arrays and the public regression passes. All failures and exact source-restoration receipts are retained.

## Evidence

`map-index-evidence.zip`: SHA-256 `aa95c17b8695ec7d4d1f2c4b102b016c7bd2c67f1314e74d734e80d5e4a7f2f1`, 950,157 bytes, 1,270 payloads. Exact member set and every payload hash/length verify. It contains initial/revised source snapshots, failures, raw micro/control workers, normal build/wheel identities, full oracle identities, gate logs, counterfactuals, source restoration, final input hashes and cleanup receipts. Binaries, environments, wheels and large Arrow references are excluded; identities are retained. Private roots: `/private/tmp/vail-general-list-20261108/map-index` and `map-index-revision`.

No cloud work, `.so` copying, host-settings change, push or PR was used. Independent exact review and explicit performance admission precede main integration.
