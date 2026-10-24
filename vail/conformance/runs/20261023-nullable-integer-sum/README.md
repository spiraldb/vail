# Prepared nullable integer field sums

The general integer list-field sum is now about **2× faster** on the profiled native DP query and uses **51.5% less CPU**. This removes per-entry type selection and ancestor-mask traversal, uses Arrow where its accumulator width is compatible, and exposes the remaining widening reduction to LLVM vectorization. No benchmark/schema-specific matcher or handwritten SIMD is added.

## Source and normal artifact

Base: `a0996a304a6c505de76dc66b32c32f59bdb07831` (same production sources as integration assembly `d7017cf7`). Candidate production source: `8a73d549`; final public test source: `53a50287`. Installed normal artifact SHA256:

`a7caca703c6d225e311f332dab397e9d403e72bf3ca391b07102ae6ba49c71f3`

The baseline artifact is `77b0c38a0f7bea5308f1651114d0bc8391d2da93beed0b0f4111bb01e7bd7d49`. Both artifact origins and candidate source fingerprints are saved. Builds used normal kache/maturin release, jobs=2, `target-cpu=native`, and an exclusively owned released target. No native artifact was copied or manually reused as a cache workaround. The initial trait-path compile error and pre-visibility-span build are preserved; final results use the successful normal visibility-span rebuild.

## Query-time preparation and hot-loop changes

During typed expression lowering, a prepared object resolves field positions, primitive width, accumulator/output width, and `filter_missing`, selecting a typed evaluator function. Its semantic identity excludes function addresses. Each batch calls that prepared function; Arrow downcasts only validate its expected physical type.

For each batch:

1. Clip the child array to the visible contiguous list-offset span with zero-copy slices and rebase row offsets. This happens before logical-mask calculation or combination. Tiny slices do not scan their hidden child owner.
2. Obtain ancestor and field **logical nulls**, combining them through Arrow `NullBuffer::union_many`. There is no ancestor-validity iteration in the value loop.
3. Reuse Arrow's primitive `sum` for Int32→Int32 and Int64→Int64. Same-width primitive headers/slices share buffers; Hail empty/all-null zero and strict-null policies are retained.
4. For Int32→Int64 only, use Arrow bitmap-word iteration and a small widening adapter. Full-valid words use a vectorizable slice reduction, zero words skip values, and mixed words use four independent branch-free masked accumulators. Arithmetic widens in registers; no Int64 child vector is allocated.

The full-valid/zero/mixed decisions are data-dependent once per 64 values, with concrete reasons: avoid mask arithmetic on full words and avoid all value loads on zero words. Primitive-width and missing-policy choices are absent from the entry loop. Partial-word handling is bounded to at most 63 values. Generated ARM assembly contains `sshll/sshll2`, `bic/and`, `add.2d`, and `addp` in the sampled mixed-null region. It has neither the old per-entry source-width choice nor the parent-mask loop. There are no architecture-specific intrinsics.

### Why a widening adapter remains

The exact lock pins Arrow **59.2.0** and DataFusion **55.0.0**. Arrow `sum<T>` and `sum_array<T,A>` return `T::Native`; their optimized generic accumulator internals are private. They expose no Int32-input/Int64-accumulator reduction without materializing a widened input. DataFusion `array_sum` coerces to Float64, loses large-integer exactness, and returns null for empty/all-null arrays. Its ordinary SUM accumulator also expects a coerced physical input. Thus same-width operations reuse Arrow; only its missing widening interface needs local arithmetic. `nullable-library-audit.md` records the exact source audit.

### Storage and allocation scope

Values remain zero-copy. Combined validity is at most one bit per **visible** child value; Arrow reuses the owned AND buffer across additional masks. A single existing mask can retain its owner without a combined-bit allocation. Mask counting/combination and typed input preparation together account for ~1.9% inclusive CPU in this profile. Batch-local owner vectors and row-sized result arrays remain; they scale with schema depth/row count, not entry payload. The existing `Vec<Option<_>>` row-result staging was retained; it is a small follow-up opportunity, not an entry-sized temporary. Four widening accumulators compile into registers/stack, not heap buffers.

## Correctness

**212 public tests passed; no new failures.** Two old heterogeneous-dictionary aggregate tests fail identically on the exact baseline (both receipts saved). Four missing-upstream-fixture errors in the first attempt were corrected by linking the existing source fixture; the final gate contains no fixture errors.

The **29 new public cases all pass on baseline and candidate**. They cover Int32/Int64, nested missing structs and fields, strict missing policy, empty/missing outer lists, 63/64/65 and 127/128/129 boundaries, nonzero bit-aligned and unaligned slices, overflow/large integer precision, sibling field positions, native round trips and filtered matrix entries. Independent Python integer oracles check values; tests neither inspect the prepared object nor spy on optimizer topology.

## Main native workload

M3 Max, 14 cores, 96 GiB RAM. Native fixture: 200,000 × 2,500 = 500M slots, Int32 DP `(row + col) % 101`, filtered entries for `col % 11 == 0`, missing DP for `col % 13 == 0`. The independent expected sum is **20,969,995,813**, checked on every action. This is synthetic periodic/compressible input, not a clinical-data claim.

Three alternating fresh-process pairs, one untimed warmup and eight measured queries per process (24 samples/mode):

| | Main baseline | Candidate |
|---|---:|---:|
| Median wall | 0.15624 s | **0.07848 s** |
| Median CPU | 1.82807 s | **0.88722 s** |
| Median per-process peak RSS | 959 MB | 849 MB |

OS cache was warm. Timings were isolated from builds, tests, trace exports and other benchmarks. RSS varies with allocator/in-flight buffer state and is not an exact allocation count. No Hail comparison is newly claimed.

## Generalized validity controls

Separate 40,000 × 2,500 native fixtures use modular mixed data and two independent logical null predicates, with Int32/Int64 and thresholds 0/1/10/50/90/100%. Every output matches an independent residue-count oracle. One warmup plus five measured queries per mode:

| Missing threshold | Int32 speedup | Int64 speedup |
|---|---:|---:|
| 1% | 1.21× | 1.14× |
| 10% | 1.90× | 1.46× |
| 50% | 1.49× | 1.23× |
| 90% | 1.46× | 1.28× |
| 100% | 1.34× | 1.29× |

Initial short all-valid Int32 observations were ~5% slower in wall time. Three additional alternating fresh-process rounds (33 measured samples/mode/width) resolved that concern: Int32 **16.669→16.635 ms**, Int64 **21.398→21.491 ms** (+0.4%); CPU is effectively unchanged (+0.2%/+1.0%). The other agent safely paused exactly its owned build descendants during this ~8-second control/copy-reference slot and resumed them immediately. Original observations remain preserved.

## Hidden-owner resource bound

A public PyArrow list slice imported through `Table.parallelize` retains a large child owner. Reducing 131 visible values costs **0.333 ms with a 1M owner and 0.325 ms with a 50M owner**. On a 50M owner, increasing the visible span to 8,192/131,072 values costs 0.336/0.363 ms. Input construction is untimed; all results are independently checked. These tiny action times include planning and do not establish a throughput curve, but show no hidden-owner-linear mask scan/allocation. Source inspection proves the clip occurs before union.

## Profiles and hardware counters

Separate software profiles repeatedly run the same verified query:

| Leaf CPU hotspot | Baseline | Candidate |
|---|---:|---:|
| Integer field sum | **61.3%** | **12.6%** |
| Vortex Boolean/fixed-width gathers | 19.1% | **38.1%** |

Candidate sum inclusive CPU is 14.7%, of which typed batch input/logical-mask preparation is 1.9%. The remaining dominant work is source materialization/gathering, not integer additions. These are sample fractions, not exact allocation/instruction counts.

Actual Instruments CPU Counters were recorded successfully. Cycle-weighted derived metrics after initial startup:

| Instruction-bandwidth category | Baseline | Candidate |
|---|---:|---:|
| Instruction delivery bottleneck | 26.6% | **11.3%** |
| Discarded bottleneck | 5.8% | **3.4%** |
| Instruction processing bottleneck | 11.4% | 36.0% |
| Useful | 56.1% | 49.3% |

These are **not ALU or SIMD saturation percentages**. Less total CPU work and a changed remaining bottleneck can reduce the “useful” fraction. Processing includes dependencies/resources and cannot be equated with arithmetic pressure. Default counters do not provide an exact retired SIMD operation breakdown. Sampled assembly, rather than template availability, verifies vector execution in the changed nullable loop.

### Empirical resource references

Over already materialized buffers larger than cache, with preparation outside timing:

- Flat Arrow C++ nullable sum, 14 threads: **58.8 GB/s** application input bytes (Int32), **84.4 GB/s** (Int64).
- Array copy, 14 threads: **221 GB/s application read+write bytes**; one thread: 124 GB/s.

These are observed software capacity references, not memory-controller measurements or theoretical peak. The native DP pipeline processes ~26 GB/s of logical DP+validity bytes but also reads compressed input, unpacks, gathers and materializes arrays, making extra memory passes; dividing these numbers is not a hardware-utilization percentage. We do not claim disk, DRAM, or ALU saturation. The evidence does show a large avoidable scalar-control cost removed and the bottleneck moving into source data movement.

## Adjacent audit and next scope

`array_reduce` sums already reuse Arrow; other product/min/max folds still contain per-element operation decisions. `list_aggregate/direct.rs` dispatches types outside rows and uses Arrow, but widens full child buffers for some reducers. Those remain separate general opportunities; this patch does not silently broaden into them. The ~38% source-gather cost needs an encoding/callee audit before labeling it unnecessary. Global-aggregation pushdown or compressed-array kernels may avoid materialization, but are not claimed by this fix.

Raw receipts, reproduction harnesses, source/native hashes, profiles, counter export and assembly are archived with checksums. Baseline profile evidence remains in the sibling `20261023-laptop-dp-profile` report.
