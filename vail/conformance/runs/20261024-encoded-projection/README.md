# Encoded projection prototype: first measurements

This prototype is **not merge-ready**. Its first measurement regressed; these
results are retained rather than replaced by an expected improvement.

Production revision `386c990029dca0552770fd40fd2e372800a2deda`, based on approved
main `0eec407731c5b39829316f8edf3f463e544b6888`. Normal kache/maturin release
build, native CPU, two build jobs, incremental disabled; build took 14m27s.
Candidate native SHA256
`f6dae80baaabbfc9e84d93641cd766f221e588c88f306b0f33b279c5ad36fd4e`.
Baseline is the normally built H078 assembly `2da14077`, production-equivalent
to main `0eec407`, native SHA256
`69a06f57d61746e815cd843bb58a08efb912d214bb3c6da650d7c62e05d825bf`.

Same existing synthetic native fixture: 200,000 rows × 2,500 columns, 500M
entry slots, 32 source partitions, 500,686,160 stored Vortex bytes including
column metadata files. Input and OS cache were warm. One warmup and three
measured queries each, sequential environments, no builds/tests/exports during
measurements. Every action returned the independent expected sum 20,969,995,813.

| Median | Main baseline | First prototype |
|---|---:|---:|
| Wall seconds | 0.076326 | 0.132074 |
| CPU seconds | 0.911501 | 1.575259 |
| Maximum process RSS bytes | 910,114,816 | 1,081,491,456 |

The prototype took 1.73× as long and used 1.73× the CPU seconds. This small first
comparison establishes a regression, not a final throughput claim. Raw actions
and artifact provenance are in the adjacent JSONL files.

A separate eight-second Time Profiler recording sampled the computed source.
Numeric dictionary take was 21.85% of leaf CPU samples, Boolean take 23.22%,
generic masked grouped SUM 11.08%, and bitmap iteration 11.03%. These are sampled
CPU fractions, not hardware utilization. Caller stacks split numeric takes
between Arrow conversion of retained original entry columns and canonicalization
inside scalar Mask beneath the grouped accumulator. The intended dictionary
grouped reduction was not the hot implementation. The source exposed unused
passthrough columns across an aggregate boundary that retains its input schema;
it therefore computed the reduction while also decoding original entries.

The first public Python gate had 54 passes and two failures in new wide-schema
native tests: Utf8View physical strings did not match the declared Utf8 schema.
The existing normalizer was missing at the computed output boundary. The
earlier release Rust runtime gate passed five tests before the final resource
refinements; that earlier run is not claimed as an exact-revision final gate.

Raw trace `/private/tmp/encoded-pushdown-first.trace`; exported stack XML
`/private/tmp/encoded-pushdown-first-stacks.xml`, SHA256
`e71692c11c0950bb6b06a0f3d8112cd0675099239093c0f035db8f664dc869c4`.
The workload JSONL SHA256 is
`c4385b07f2ec0671858f774276d7bfa3ef3937465f29a93f703b0e6a27ddd7ae`.

Subsequent fixes require fresh builds, semantic checks and measurements. Review
also requires clipping visible child spans before ancestor extraction/masking,
preserving session batch/entry budgets, and resolving underparallelized sources.

## Second prototype, still slower

Production source `3959714d`, native SHA256
`75858264238d9df730bbbcba1a55529a414e26f1f88e87d8da006f9801bac15a`.
Normal follow-up build took 5m34s. Repeated the same warmup + three action
comparison in quiet conditions: baseline median 81.724ms / 0.891919 CPU seconds;
candidate 99.898ms / 1.135964 CPU seconds. The prototype improved versus its
first version but still took 1.22× wall time and 1.27× CPU time versus main.
Every action returned the exact expected sum. Public Python gate: 56 passed,
including the two formerly failing wide-schema cases and native string
passthrough. This does not establish final resource acceptance.

Second profile: Boolean take 15.74%, numeric take 14.83%, generic masked grouped
SUM 14.38%, bitmap iteration 14.90% of leaf CPU samples. Unused Arrow conversion
was removed. The dictionary grouped kernel still did not become the hot path.
Pinned MaskReduceAdaptor declines reduction unless its Boolean mask is concrete
Bool/Constant; encoded logical masks therefore prevented the Dict rule. The next
prototype changes mask execution order to resolve only that bitmap first and
reapply the existing encoding-specific mask rules. No improvement from that
next change is claimed before its own build, correctness checks and timings.

## Third prototype: encoding preserved, kernel still slower

Production source `a2f343dd` (test/evidence HEAD `12629a5c`), native SHA256
`1557c16866ec8d9bfe5e164cc4f6b7eb1966092ccb7365f0f8f23f60be93ab7d`.
Normal build took 5m23s. Same quiet warmup + three actions: baseline median
75.921ms / 0.898115 CPU seconds; candidate 101.917ms / 1.207583 CPU seconds,
1.34× slower in both. Exact independent sum passed every action.

Current Time Profiler establishes that the encoding-preservation fix works:
61.56% of leaf CPU samples are in the new dictionary reduction. Dictionary
gathers and the generic masked grouped reducer disappeared from the top
stacks. Thus avoiding expansion alone was insufficient to beat main.

A read-only public Vortex API diagnostic inspected one existing file: DP has
102 dictionary values with bitpacked u16 codes. Field extraction yields
Mask(Dict, encoded Boolean AND); one execution step after the mask-order fix
returns Dict(masked codes, shared dictionary values). Diagnostic output is
retained, with no production hooks or implementation-dependent test assertions.
The example was built with normal Cargo/kache and the exact standard maturin
CPU/link flags; this reused its dependency cache normally (3.60s build), without
copying any native artifact.

Actual ARM assembly showed hidden Mask enum decisions and bounds checks in
the per-code loop, plus bit-at-a-time validity extraction. The next version
binds concrete borrowed bitmap views per batch and reuses the library's
word-wise BitIndexIterator. Its improvement remains unclaimed until measured.

## Concrete bitmap views, nullable records, and confirmation

Typed borrowed bitmap views plus library set-bit iteration (`b905a505`, native
SHA997001b8...) reduced the fourth prototype median to89.790ms /1.062947CPU
seconds, versus76.623ms /0.892587CPU main: still slower. All exact sums passed.

Cache-sized nullable dictionary records (`ff28d46d`, native SHA256
`2dec41217f7c722202ac103beecfc6473e3445bb09d4fe10d01727af6674ca89`)
reached approximate parity. A quiet 12-action confirmation measured main
75.592ms /0.901650CPU seconds and candidate74.403ms /0.893676CPU seconds.
Wall ranges overlap72–80ms; this does not establish a meaningful speedup.
The preparation path is limited to16KiB and at least four present codes per
dictionary slot; large/weak-reuse dictionaries retain borrowed bitmap lookup.
The cache/amortization policy still requires broader resource validation.

A public-API single-thread CPU experiment uses40.96Mslots, independent exact
sums, two warmups and five samples per cardinality/density class. Card102
measured19.37ms with all-valid codes, but33.95ms at99%valid and31.33ms at91%
valid. Card3000 measured27.17ms,43.51ms,40.34ms respectively. Lower densities
50%/5% measured18.38ms/4.58ms (card102),23.18ms/4.99ms (card3000).
Thus bit-index work is a general dense-mask problem, independently of the
periodic native fixture. The next candidate streams valid runs for dense code
masks with the existing library iterator; its benefit is not yet measured.

No prototype in this report is approved for merging. Existing resource-review
blockers remain: visible-child clipping before parent-mask execution, session
batch caps, and underparallelized source handling.
