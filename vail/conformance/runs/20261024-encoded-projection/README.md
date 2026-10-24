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


## Current resource fixes and reproducible confirmation (3ad301e3)

Current normal native SHA256 is
`bd7dcca1a2d34820fdb8f763b063bc6dd57663a076709e53af3a4916e914ec59`.
The candidate passes 8 public Rust integration tests and 56 Python tests,
including logical ancestor nulls, independently sliced masks, public batch caps,
and empty/null list groups retaining a large hidden child owner. No physical
plan-name assertions are used. Session batch splitting now uses the existing
BatchSplitStream, and list child clipping precedes ancestor-mask evaluation.

Warm, isolated 12-action confirmation on the M3 Max (exact sum checked each
query), comparing normal H078 main artifact SHA69a06f57...:

| 500M-slot native matrix | Main median wall / CPU | Pushdown median wall / CPU | Wall speedup |
| --- | --- | --- | --- |
| Defined entries, nullable DP | 72.389 ms / 0.863420 s | 61.546 ms / 0.744743 s | 1.176x |
| Filtered entries, nullable DP | 76.125 ms / 0.903811 s | 74.738 ms / 0.898885 s | 1.019x (parity) |

Both use 32 source partitions. Earlier three-action measurements were noisier;
these confirmations supersede them. The unfiltered fixture is written through
public unfilter_entries().write, with the identical DP sum 20,969,995,813.

A public writer also prepared a 500M-slot table with exactly one source
partition: 500,000 rows x 1,000 list elements, nullable Int32 DP. Query:
`t.aggregate(hl.agg.sum(hl.sum(t.items.map(lambda e: hl.int64(e.DP)))))`.
Three-action preliminary medians: main331.186ms /0.507719CPU s, candidate
296.375ms /0.355398CPU s (1.117x wall,1.429x CPU). Exact sum23,075,009,474.
Both underutilize the machine (approximately1.2–1.5 concurrent cores). This is
an unresolved general scan-work scheduling opportunity. Current storage row
ranges select files but slice after whole-file decode; naive additional source
lanes would duplicate decode and are not acceptable.

Public hidden-owner resource experiment: two visible list rows with128 child
values, two logical ancestor masks, and null/empty rows retaining hidden owner
spans. Owner sizes10K/1M/40M measured query medians33.8/28.7/15.3microseconds;
40M owner with8192 visible children measured26.3microseconds. Preparation was
excluded. Tiny-time variation precludes comparing those individual medians,
but query cost does not grow with the retained backing owner. Source is
`examples/bench_list_clip.rs`, no private instrumentation or counters.

The proposed >=90%-valid run traversal was REJECTED and removed. Although it
slightly helped the periodic native fixture, randomized91%-valid masks became
1.40–1.52x slower for cardinalities3000/102. The raw rejected experiment is
retained rather than selecting an unvalidated density threshold. Current
mixed-null traversal uses the existing word-wise set-bit iterator.

No current prototype is approved for merging. Final profiles, resource review,
and exact-revision review remain required. Broader pushdown roadmap is tracked
by the parent in investigations/VORTEX_PUSHDOWN_QUEUE.md.


## Current steady profile, assembly, and actual CPU counters

Current Time Profiler and CPU Counters traces are retained under
`/private/tmp/encoded-resource-final{,-counters}.trace`; file checksums accompany
this report. Parsing excludes the first4seconds (startup/warmup/pause).
Steady dictionary reducer is38.1% of leaf CPU samples. Allocator, file reads,
bitpacking and registry cleanup are other substantial costs. In particular,
read_vortex_task currently constructs a default VortexSession PER FILE, which
rebuilds/drops registry state. Reusing session state at an appropriate runtime
scope is a general source-confirmed opportunity, not implemented here.

The actual hottest ARM inner loop loads a u16 code, bounds-checks its dictionary
index, loads a cache-resident value/validity record, sign-extends and accumulates
into i64. It does not construct an expanded numeric child or Int64 entry array.
No handwritten SIMD. The compiler emitted scalar dependent indexed loads;
there is no evidence this is approaching arithmetic-unit capacity.

Actual Apple CPU Counters steady cycle-weighted categories: Useful42.76%,
Instruction Processing Bottleneck46.33%, Instruction Delivery Bottleneck6.20%,
Discarded4.71%. These are instruction-bandwidth bottleneck categories, NOT
arithmetic or SIMD utilization. Processing includes dependency/memory stalls;
these counters do not measure DRAM bandwidth or establish ALU/FPU saturation.
The exposed categories give no basis to claim the scan is hardware-limited.
The independent one-source experiment further demonstrates an unresolved
parallel-resource gap. Source scheduling and per-file session setup deserve
attention before treating this prototype as a final optimized scan.


## Re-review exposed two additional resource failures

The initial hidden-owner experiment selected contiguous rows plus null/empty
hidden ranges. It did NOT establish bounds for distant valid child ranges or
oversized retained dictionaries. Review correctly requested these additional
cases. The first follow-up reused ListView selected-range rebuild and bounded
the small-list take-index allocation by visible valid size, then gathered
oversized dictionaries before primitive reduction. Its9Rust and56Python tests
passed, but the public resource experiment STILL FAILED:128 visible values
across distant owners10K/1M/40M took approximately30/139/4431microseconds.
Oversized primitive/bitpacked dictionary variants had similar owner scaling.

Actual Time Profiler of that failed probe attributes98.6% of steady leaf CPU to
primitive compare_slice_constant inside Dict FillNullKernel. Existing take
kernels zero nullable indices with fill_null(0); Dict FillNull searched EVERY
retained dictionary value for0 before filling a tiny selected code array.
This is a separate general library callee bug. Follow-up source now guards
retained dictionary cardinality greater than logical array length BEFORE that
search, canonicalizes only selected rows, and reuses primitive fill-null.
Its build/probe results are pending; these blockers are not claimed closed.

Primitive and bitpacked dictionaries have selective take (bitpacked's whole
array crossover is bounded by8times selected count). Pco has page-aware slice
but no selective TakeExecute kernel in the pinned source; generic take can
still canonicalize its retained compression block. The resource proof must
account for that capability rather than assuming arbitrary take is bounded.
No universal encoding-independent resource guarantee is claimed.


## Follow-up 266ab06f: demonstrated bounds, remaining Pco blocker

Normal artifact SHA256
`4d763b48c46c26bf9093f30c1e291fc003007aabbf81a71957b0155c71e246d4`
passes9public Rust and56Python tests. The Dict FillNull guard removes the
observed hidden-owner scan. With128visible values across distant owner sizes
10K/1M/40M, median query cost is31.96/29.96/29.88microseconds. Oversized
primitive dictionaries:31.33/31.38/29.17microseconds. Oversized bitpacked
dictionaries:38.92/38.75/38.75microseconds.40samples after5warmups; independent
exact sums; input preparation excluded.

Fresh12-action native unfiltered500M confirmation: main73.337ms /0.843319CPU s,
candidate62.515ms /0.728825CPU s (1.173xwall,1.157xCPU). This remains BEFORE
physical capability preflight/fallback, so is not the final merge measurement.

A separate public Pco probe confirms the remaining general capability BLOCKER:
128visible dictionary codes against10K/1M/10M retained nullable Pco values cost
91.6microseconds/4.07ms/40.62ms. Exact sums pass, resource scaling fails. Pinned
Pco has no selective take kernel; even its page-aware slice executes/ranks the
unsliced validity bitmap. A generic take call is not a resource proof.

The next design preserves original typed expressions and uses conservative
metadata preflight. Only a small proven closed operation/layout composition
receives a typed prepared certificate. Unknown/missing capability must choose
the original Vail source plus ordinary DataFusion projection BEFORE encoded
payload evaluation or partial output. No codec-name blacklist or inner-kernel
None is accepted as a substitute. This design remains under review/development;
no merge approval or universal boundedness is claimed.

## Conservative physical eligibility and ordinary fallback (in progress)

Source b9543b87 adds a metadata-only, closed dense-list capability proof before
computed execution. It binds immutable snapshot/file/operand/partition identity,
requires constant-step zero-based offsets exactly covering their child, proves
ancestor validity from constant true metadata, and admits only bounded primitive,
patch-free bitpacked, constant, or small-dictionary descendants. Dictionary
preparation has a fixed64KiB cap including decoder scratch and validity. Unknown
encodings (including Pco), variable/hidden/sparse coverage and unsupported schema
changes retain the original typed expression and ordinary DataFusion source plus
ProjectionExec before any computed payload evaluation. This is a narrow supported
subset, not a promise that the ordinary reader is selected-value-bounded.

The first public Python gate found51passes and6failures, preserved at
`/private/tmp/encoded-certified-initial-python-gate.log`. The ordinary fallback
mistakenly supplied the table globals schema to the logical lowerer even though
its physical input is row-only. This skipped leading source columns when globals
were present. Production df408be6 fixes that row-only lowering; the translator's
closed syntax rejects Global/binding-dependent expressions before this route.
Public tests now cover actual nonempty globals, aliases/key order and a separate
global-dependent arithmetic reduction that must retain its ordinary planner.
The exact restored-lock build and final gate/measurements are pending; earlier
266ab06f speedups above must not be attributed to the certified revision.
