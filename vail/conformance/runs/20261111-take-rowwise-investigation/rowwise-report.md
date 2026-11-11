# Many row-wise aggregations: measured CPU account

Exact installed Vail native: `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193`, production `79744c031`, main `8bc7ced62`. No production change, build, runtime tuning, allocator experiment, or cloud run was made. Source input is the same full Hail-format profile used by both engines in the completed six-case comparison: 24,885 variant rows × 2,535 samples, 16 input partitions. The unchanged upstream function constructs `many_aggs(mt)` with 33 outputs (nine `count_where`, 24 `fraction`) and executes `mt.rows()._force_count()`.

The matched benchmark result is Hail **1.684508 s / 7.330098 core-s**, Vail **0.877585 s / 8.593542 core-s**: Vail finishes **1.919× faster but uses 17.236% more CPU**. Hail uses Spark local[4], Vail its existing Tokio32 pool with Rayon4; this is not a four-core cap on Vail. Observed CPU/wall ratios are about 4.35 and 9.79 respectively. Greater parallelism explains how lower latency can coexist with greater total work; it does not excuse the CPU deficit.

## It is execution, not Python plan construction

One separate three-call decomposition mechanically returns the receiver of the unchanged AST's final `_force_count()` rather than executing that call inside the constructor. Median read/IR/frontend construction is **21.89 ms / .02189 core-s**. The following normal force-count action is **823.18 ms / 8.58024 core-s**. Reusing that same already-constructed logical table still costs **843.56 ms / 8.59183 core-s**. The latter is a different lifecycle diagnostic, not a replacement benchmark result; physical action preparation still runs. It shows that avoiding Python/frontend reconstruction does not remove this CPU cost.

The physical plan trace comes from the actual force-count action (`VAIL_TRACE_FORCE_COUNT_PLAN=1`), not ordinary `physical_plan()` with a possibly different terminal order requirement. The recorded **AFTER part 0** plan, after force-count demand/order rewriting, has one source read and one shared 33-output row-result projection. All 33 expression roots match the BEFORE plan exactly; the raw trace has 16 AFTER partition markers. There is no Unnest, relational grouped hash aggregate, global sort, or repeated full input scan per output in this route.

The frontend's existing `matrixtable.py:1228–1246` bundles the compatible outputs and binds the bundle to one relational column. Consequently, extracting `x0..x32` does **not** rerun all 33 reducers 33 times. This existing sharing is real. But the bundle contains **33 independent predicate/reduction roots** over its shared cell list: the exact plan has 17 `array_transform` + segmented-reducer roots and 16 opaque whole `hail_operation` roots. Sharing a result bundle is not the same as fusing all sibling predicate producers and reducers into one pass.

## Profile account

One normal samply recording is configured for 12 seconds. After one full warmup, the unchanged original action repeats seven times over **6.2217 seconds / 60.5568 process core-seconds**; all worker and sampler groups are absent afterward. Warm/end markers trim the profile; observed positive `threadCPUDelta` intervals total 60.5534 seconds. A normal Firefox profile and presymbolicated sidecar are retained as `rowwise-profile.json.gz` and `rowwise-profile.json.syms.json`.

Approximate, non-overlapping attribution of CPU intervals to sampled endpoint stacks:

| Category | Fraction of sampled CPU | Approx. core-s per profiled action |
|---|---:|---:|
| Projection / expression evaluation | **52.68%** | **4.557** |
| Hail reader decoding stack | **33.10%** | **2.863** |
| Wait/scheduler endpoint samples | 9.39% | .812 |
| Other engine/runtime | 4.31% | .373 |
| Planning/preparation stack | .52% | .045 |

These are ordered, exclusive stack categories, not a sum of inclusive percentages. An endpoint landing in `__psynch_cvwait` can receive CPU accrued earlier in its sampling interval. The 9.39% row is **not CPU spent sleeping** or proof that a lock is the bottleneck. Interval attribution and finite samples are approximate. The profile includes the original frontend lifecycle; do not add its .045 preparation category to .0219 frontend CPU as independently measured disjoint components.

Within the projection category, the following subcategories are mutually exclusive; percentages below use the same whole-profile denominator:

| Expression work | Whole-profile fraction | Approx. core-s/action |
|---|---:|---:|
| Fallible integer numeric kernel (`columnar_numeric` → Arrow `try_binary`) | **17.15%** | **1.483** |
| Nested-list index checking + gather (`array_index_rows`) | **11.99%** | **1.037** |
| Call expansion and property access | 5.46% | .472 |
| Numeric casts | 4.79% | .414 |
| Comparison kernels | 3.34% | .289 |
| Other arithmetic | 1.68% | .145 |
| Segmented Boolean reduction kernels | **.60%** | **.052** |
| Remaining expression work, allocation/copy/bitmap leaves | 7.68% | .664 |

The numeric function is generic, so the sampled symbol is named conservatively. The actual constructed IR has exactly four modulo expressions, eight true divisions, four array indices, **no floor division or power expression**; this workload's intended fallible integer arithmetic is the four `% 2` predicates. This narrows the concrete work without pretending the monomorphized symbol uniquely names one source expression.

Self/leaf measurements support the account without adding overlapping call trees: fallible integer numeric loop 16.98%, `array_index_rows` 9.70%, numeric-cast loop 4.37%. Allocator-function leaves are about .48%; `memmove` self 4.13%; `madvise` self 1.21%. This is not a count of allocated bytes, page-fault cost, retained memory, or an argument for another allocator change. Copies beneath list gathers and arithmetic are already included in their stack categories above.

Hail decoding is substantial, about a third of this Vail profile. The remaining CPU deficit cannot be assigned entirely to aggregation merely by subtracting Hail's total CPU. No comparable Java CPU profile was captured in this investigation. Nevertheless, projection-side producer work is independently a large, concrete cost center; the actual reducers themselves are cheap here.

## The repeated producer work is concrete

A separate query-free inspection of the actual native IR retains 521 scalar nodes. Structural grouping requires identical input-owner expression and identical expression after alpha-renaming only its own map parameter; literal Arrow values/types are decoded exactly. It finds:

| Same-owner producer | Occurrences in sibling map bodies |
|---|---:|
| `AD[0]` | 2 |
| `PL[0]` | 2 |
| `GQ % 2` | 2 |
| `DP % 2` | 2 |
| `GQ / 5` | 4 |
| `DP / 5` | 4 |

The repeated predicates have different enclosing callback identities (`==0` versus `!=0`; `<10`, `<=10`, `>10`, `>=10`). DataFusion sees distinct semantic scalar UDFs at those sibling roots. It does not see their internal shared `AD[0]`, modulo, or division producers as relational expression nodes on which its ordinary projection CSE can operate. The physical plan has no shared relational columns extracting those producers. Existing Python `bind_shared_list_results` also treats array callbacks/list functions as opaque scope boundaries. This is why the existing shared bundle does not hoist these internal producers across consumers.

Source makes the costs understandable:

* `expressions.rs:3479–3514`, `array_index_rows`: for every referenced list, check list/index nulls, read its length, normalize negative indices, check bounds, append an absolute child index, then call Arrow `take`. The result is an ordinary primitive array. It is not just a cheap reference to `AD[0]`; the same-owner `AD[0]` and `PL[0]` each appear in two consumers. This copies selected primitive outputs, **not all AD/PL payloads or whole entry structs**.
* `expressions.rs:3391–3420`, `columnar_numeric`: accepts two full Arrow arrays. The integer loop checks divisor zero and applies Hail sign/overflow semantics for each value, then produces a new integer array. The constant `%2` remains an array-array kernel at this boundary. Scalar-aware specialization could retain the literal operand; no assembly-level strength-reduction claim is established here.
* The eight `/5` producers use ordinary typed cast/arithmetic/comparison vectors. There is real measured cast work; equal producers are hidden inside separate callbacks. Identical division results could be shared under the same owner and validity/error demand, without algebraically replacing comparisons or changing floating semantics.
* `calls.rs:128+` constructs result Boolean/integer arrays for property access. `legacy/packed_call.rs:241–252` expands packed Calls through a per-array `OnceLock`; this is **not nine independent full Call expansions**. The profile's combined Call category includes this first expansion plus subsequent property vectors.
* `list_aggregate/direct.rs:179–218` prepares reducer choice once, then uses list offsets and packed Boolean words. These final counters account for only .60% of sampled CPU; rewriting the counters alone is unlikely to address the main cost.
* `list_flatmap.rs:587–595` admits Add/Sub/Mul, integer/bool comparisons and Boolean operators in the block compiler. Modulo, division, floating comparisons, Call operations, and list indexing are not all admitted. Consequently the existing generic fusion does not cover these whole callbacks. Adding an output builder alone would not remove the demonstrated repeated producers.

## What Hail actually executes

The existing timing worker's actual Hail log is retained under `/private/tmp/vail-six-slower-20261111/time/test_matrix_table_many_aggs_row_wise-0-hail/hail.log`. `hail-lowered-one-row-stream.txt` extracts its executed lowerer output. At lines approximately 7104–7510 of that log, one row's entries are zipped with column records, absent entries are filtered, and **one `StreamFor` updates 34 aggregate states**: nine count-where sums, 24 fraction numerators, and **one shared present-entry denominator**.

Within that single entry loop, GT is bound once; AD/PL indexing, modulo, division, comparisons and state updates are scalar IR operations. Results go straight into scalar aggregate state. The IR contains repeated scalar operations too, so it does not prove Hail eliminates every identical modulo/divide. It does prove that Hail does not require one materialized Boolean/primitive vector per predicate followed by another list-reduction scan. Hail's lowering/code emission compiles this stream/aggregate IR; this report does not claim a specific JIT optimization or measure Java kernel percentages.

## General improvement priorities, not implemented

1. **Cost-aware sharing across sibling list predicates/reductions with the same owner/context.** Expose or prepare a common producer region before hiding each callback in its UDF: share exact pure expensive producers such as one bounded list-index vector or one division/modulo result among several predicates. Retain entry presence, missing-list behavior, bounds/errors and conditional demand; this is not permission to eagerly evaluate arbitrary UDFs or move errors across a filter. There is no need to introduce an unconditional streaming `bind` operator.
2. **Preserve scalar operands through batch arithmetic dispatch.** A scalar-aware fallible numeric kernel avoids materializing/reading a repeated divisor array and permits a typed constant-divisor implementation. Generic modulo semantics (negative operands, nulls, zero, signed overflow) must remain correct; do not special-case genotype names or this benchmark's literal values.
3. **General map-to-reduction / multi-output reduction fusion.** Extend the prepared block program's supported operations and reduce predicate blocks directly into row states, sharing producer reads across outputs. This needs an aggregate consumer as well as output-builder support; simply composing list maps or adding a Struct builder does not combine independent reductions. Start with the measured common-producer gap rather than replacing all kernels with a benchmark-specific handwritten loop.
4. **Packed Call property kernels and reader cost remain separate opportunities.** They may avoid expansion/property vectors generally, but are smaller measured projection components here. This profile does not establish that decoding itself is optimal or that another reader scheduling rewrite is justified.

These are source-supported proposed directions, not achieved gains. A 17.24% CPU deficit means reducing current Vail CPU by about 14.7% would reach this Hail point estimate; sampled cost centers are large enough to investigate, but their full percentages are not removable or additive speedup promises. No new engine implementation or benchmark tuning is part of this account.

## Evidence

`trace-result.json`, `profile-result.json`, `decompose-result.json`, and per-phase receipts pin the exact normal native/source and positive cleanup. `actual-count-plan-after-part0.txt` is the actual post-force-count-rewrite partition-0 plan; `expression-roots-after-part0.json` verifies its 33 roots (17 mapped/segmented, 16 opaque whole operations) match the BEFORE roots exactly. `actual-count-plan-before.txt` and `expression-roots.json` retain the earlier stage; the full raw trace has all 16 AFTER partition plans. `actual-ir-dag.json` and `ir-duplicates.json` retain the exact scoped producer audit and literals. The first IR serialization used NativeArrow object repr addresses and could not prove literal equality; that output is retained as `actual-ir-dag-initial.json`, corrected by a second query-free Arrow-C-stream serialization. Neither inspection executes a query. Every bounded diagnostic/inspection worker exited successfully with its group absent. No extra cases, profiler/benchmark overlap, feature code or main mutation occurred.
