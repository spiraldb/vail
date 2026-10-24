# Native list-length projection

Candidate source `0f2d75c4` includes current main `6656e247`. The normally built
native artifact `5b681dc5…a433e` was built at `adfd8a34`; the subsequent commit
adds only two public runtime tests. Source and receipt digests are in
`manifest.json`. Main was not modified by this task.

## Change and eligible domain

Generic `ArrayLength(Column(List))` can now be evaluated by Vortex's existing
ListLayout offsets-and-validity projection before Arrow conversion. DataFusion
still owns relational operators, filters, ordering, aliases and global SUM. No
entry values are inspected to compute length, and no new entry loop or SIMD
implementation was introduced. The result remains nullable Int32; global SUM
still uses DataFusion's Int64 accumulator.

This is a closed capability, separate from the existing dense Int32 list-field
SUM proof. Eligible native full-file operands have validated list/ancestor
logical validity, U64 non-null Sequence offsets, checked rows+1 and width/end
arithmetic, and width <= Int32::MAX. Nonzero element offsets, null elements,
and retained element owners are allowed: the proof reads only the element
row-count descriptor and never traverses element payload/codec trees. Variable
or unsupported shapes, later pruning, deletes, unsupported bindings and unsafe
coverage retain the original typed DataFusion route. No benchmark/schema names
or query-specific rewrites are used.

The proof cache key includes operand operation and paths, and a prepared
projection binds the exact expression and result schema to its certificate.
SUM's operand-domain rules are unchanged.

## Metadata preparation

The first implementation exposed excessive many-file startup work. Preparation
now uses one real positional reader per file, reuses checked metadata slices
from its already-read initial buffer, and runs footer/tree validation on Tokio
workers. The ordered outstanding window, including completed but unconsumed
results, is at most eight files. Ordinary file outcomes are consumed in source
order; a task panic/join failure is reported immediately. JoinSet aborts active
tasks on early fallback/error/cancellation, but an already-dispatched blocking
read may finish.

One shared cumulative 16 MiB budget charges raw read lengths before requesting
their allocations and charges metadata estimates after parsing. It is **not a
strict peak-heap cap**: opening/parsing a footer precedes its approximate-size
charge, and that estimate does not cover every library allocation. At budget
exhaustion, reservation winners can depend on scheduling; certification versus
safe ordinary fallback is therefore not claimed to be deterministic. The byte
counter is never reset per file, and no global data/footer cache is introduced.

Only an already-proven Constant segment <=256 bytes may be requested whole for
scalar metadata reuse; larger constants use a <=16-byte positional scalar
read. Unknown large Flat payloads are not read to save calls. Initial-buffer
sharing clones a real BufferHandle without copying its bytes, and the per-file
cache is dropped after preparation. There is no await or IO under its mutex.

## Matched laptop results

M3 Max; normal release/kache builds, native CPU flags, jobs2, no overlapping
builds/tests/timed workloads. Baseline is exact main `669af5b0` (before the
disjoint correlation change in `6656e247`), native `9ee229e8…db8b`. Candidate
includes both. Each row uses serial A/B/B/A independent processes with 24 timed
actions per process, 48 observations per artifact. Every action checks an exact
integer result; one untimed warmup precedes the observations.

“Fresh” means a new public read/snapshot/proof cache for each action in an
already-warm process. Public source opening occurs before the timed aggregate;
physical planning, capability preparation and execution are timed. OS file
caches are warm. These are not cold-disk or Python-import measurements.

| Query | Baseline wall | Candidate wall | Baseline core-ms | Candidate core-ms |
|---|---:|---:|---:|---:|
| SUM(length), 200K lists x2500 elements (500M elements) |50.603 ms|6.446 ms|571.53|54.24|
| Fresh128 tiny files |8.002 ms|8.787 ms|47.98|61.02|
| Same128-file source, proof cached |8.239 ms|5.034 ms|49.56|37.19|
| Fresh one tiny file |1.374 ms|1.455 ms|1.397|1.486|

The large shape query is **7.85x faster**, with **90.5% fewer core-seconds**.
It does not decode 500M elements: avoiding that work is the point. The native
fixture has 200K rows, 2500 elements/row and **56 physical files**, 418 MiB; it was
written once through public native APIs and reused unchanged.

The remaining tradeoff is explicit: fresh128-file startup is **0.786 ms (9.8%)
slower and uses27.2% more core-seconds** for this nearly payload-free query.
Tiny one-file startup adds80 microseconds. Cached many-file execution is1.64x
faster. This is not a claim that every small fresh read improves.

Failed/intermediate approaches are preserved: serial preflight cost21.121 ms;
eight overlapping IO futures plus Constant reuse reduced it to14.412 ms;
reusing exact initial-read tree slices reached11.296 ms in a matched control;
bounded worker parsing reduced it to8.787 ms. Worker scheduling spends extra
CPU to remove the planning-thread latency bottleneck. It is not hidden by a
file-count/size threshold or expanded eligibility.

## Resource and profile evidence

Public ArrowReader scan metrics over the same56-file fixture report
443,113,832 ->3,669,960 logically requested file bytes (120.7x reduction) for
ordinary versus computed length projection. The computed requests are exactly
56 initial65,535-byte footer reads; no element payload segment is requested.
This is an **adapter scan boundary**, excluding LocalRead capability preflight,
and it counts requested ranges, not physical disk/cache misses. Native timings
above include preflight, which reads footers again;120.7x must not be presented
as complete-query physical disk savings.

A public Vortex kernel probe builds real Pco element owners of10K/1M/10M values,
then requests128 visible non-null elements across lists with a nonzero start
and a missing middle list. Length evaluation medians are2.542/2.500/2.500 us.
This demonstrates the library length kernel avoids element-owner decoding;
it does not prove a persisted LocalRead certificate. The ordinary public native
producer clips retained-prefix owners, so those produced files are explicitly
not counted as hidden-owner native coverage. The actual native nullable-list
and variable/nested-list fallback cases are covered by public tests.

Process high-water RSS for the large query is776.05 ->360.91 MB. On tiny
many-file queries the candidate process is about45 MB larger than the baseline
process; different normally built native artifacts/import footprints prevent
attributing that entire difference to this metadata code. Across300 fresh128-
file actions, baseline RSS high-water is311.25 ->315.24 MB and candidate357.17
->360.40 MB. Candidate increments100->200 and200->300 are344 KB and115 KB;
there is no assertion that16 MiB is a heap limit or that this proves unlimited
run memory behavior.

Software samples show ordinary length reads traversing List/Struct/Int32 Arrow
conversion; the computed path avoids element conversion. The final many-file
sample places prepare_file/layout validation under Tokio worker threads rather
than the physical-planning/main thread. These are stack samples, not arithmetic,
SIMD, bandwidth or hardware-saturation counters. No handwritten numeric kernel changed: the optimization removes payload work
and uses existing Vortex length kernels. This evidence does not establish that
every remaining library loop is at its hardware limit.

## Validation

- 405 public Python tests pass, including six new length cases covering empty,
 fixed/variable lists, missing parent lists, null elements, nested payloads,
 aliases, filtering/head/order, globals, composite aggregates and independent
 many-file reads.
- 30 public Rust tests pass:11 encoded projection (including current-thread and
 multi-thread Tokio length queries),2 replaced/concurrent polling runtime reads,
 7 numeric aggregate and10 coordinate reducer-state tests. Zero failures/skips.
- No internal plan-name, implementation-counter, source-text, or helper-output
 assertions were added. Runtime tests observe native table read outputs and
 global aggregates.

The prior encoded-SUM Pco capability limitation remains: unknown unsafe codec
shapes fall back; this work neither adds an upstream Pco take kernel nor claims
to solve its sparse-selection hazard. See the broad follow-up capability report
and `investigations/VORTEX_PUSHDOWN_QUEUE.md` in the comprehensive scan run.
