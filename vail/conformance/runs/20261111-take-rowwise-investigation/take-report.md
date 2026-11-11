# Take/rekey: why Vail loses wall time

The actual plan establishes an execution-graph serialization boundary before partial aggregation. This is the leading explanation for the wall-time gap, supported by the low observed CPU/wall utilization; no counterfactual has measured its share of the gap. Python query construction and measured preparation are small. Vail is already more CPU-efficient on this case, but it feeds its partial aggregate from one globally ordered stream instead of 32 independent streams. The first useful parallel redistribution happens after that partial aggregate.

Exact measured main: `8bc7ced6205d5951a4c040255990cffba4e8d704`, production `79744c0316884b341ed04cc2f02d77fb6bd3d7ba`, normal installed native SHA `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193`.

## What the benchmark does

```
ht = read_matrix_table(full_gnomad).localize_entries('e', 'c')
ht.group_by(k=int(ht.row_idx / 50)).aggregate(
    value=agg.take(ht.row_value, 1)
)._force_count()
```

The unchanged full input has 250,000 rows, 1,000 entries per row, and 32 Hail source partitions. There are 5,000 output groups. Both engines read the exact same files. All timing, source and artifact pins are in `/private/tmp/vail-six-slower-20261111`.

| Original matched benchmark | Wall | Observed core-seconds | Core-seconds/wall |
|---|---:|---:|---:|
| Hail | 1.00012 s | 4.83869 s | 4.84 |
| Vail | 2.41742 s | 2.94077 s | 1.22 |

Vail is 2.417× slower in wall time but uses 39.22% less CPU. The CPU/wall quotient describes average observed CPU utilization, not an enforced core cap or proof of saturation. Hail includes stable observed JVM endpoint CPU, with the existing lower-bound limitation for unobserved short-lived children.

## Measured phase breakdown

A separate public-API decomposition ran one full warmup and three measured calls, without modifying production code. Medians:

| Phase | Wall |
|---|---:|
| Construct legacy read | 2.160 ms |
| Construct localization | 0.675 ms |
| Construct key expression | 0.035 ms |
| Construct grouping | 0.011 ms |
| Construct take expression | 0.029 ms |
| Construct aggregate table | 0.244 ms |
| Terminal `_force_count()` | 2,285.399 ms |

These phase medians are descriptive and should not be added to claim an exact median full-query time. The terminal action's process CPU median is 2.81458 core-seconds. The public Python/IR construction is about 3 ms, so it does not explain the multi-second latency.

One additional actual action used the build's existing `VAIL_TRACE_ACTION_PREPARATION` counters. Its terminal force-count wall was 2.29819 s. Preparation counters were:

| Preparation stage | Elapsed |
|---|---:|
| Session/batch advice | 0.354 ms |
| Vail logical lowering | 0.219 ms |
| Parameterization | 0.025 ms |
| DataFusion logical optimization | 0.488 ms |
| DataFusion physical planning/optimization | 18.470 ms |
| Broadcast rewrite | 0.053 ms |
| Indexed-unnest rewrite | 0.014 ms |
| Remaining physical/RNG binding | 0.103 ms |
| Sum of measured preparation intervals | **19.726 ms** |

The two initial markers are in `physical_with_prefetch`; the remaining markers are consecutive intervals in `compile_mode`. They measure elapsed preparation, not additive operator CPU. Together they account for less than 1% of this terminal action. Final action rewrites, startup, execution and teardown outside those counters occupy the remainder; it would be imprecise to label every remaining microsecond decoder time. The profile's main Python thread accounts for about 0.068 sampled core-seconds across three repeated actions, consistent with preparation being small.

## Actual executed plan and where parallelism is lost

This is the `VAIL_TRACE_FORCE_COUNT_PLAN after` plan of the actual benchmark action, not a separate `.physical_plan()` display. Its important portion is:

```
32 independent Hail source partitions
  → localization projections
  → OrderedConcatExec(children=32): ONE output stream
  → key-expression projections
  → Partial Take aggregate: ONE input stream
  → Hash Repartition(k, 32), input_partitions=1
  → FinalPartitioned Take aggregate
  → CoalescePartitions
  → count
```

The ordinary displayed plan had final sorts, but force-count removes those sorts. Thus the observed wall deficit is not explained by a final global sort in this action. Crucially, force-count does not remove the ordered boundary below the partial aggregate.

Why the boundary exists:

* `logical_aggregation.rs:1166–1170` calls `logical::ordered(child)` whenever a grouped output's `needs_order` is true.
* `needs_order` at `logical_aggregation.rs:721–771` treats `take` as order-sensitive: it is not in the list of freely mergeable aggregate names. Coordinate bundles also mark take as ordered.
* `logical.rs:1881–1908` lowers that boundary to `OrderedConcatExec::tagged` over views of every original partition.
* `partitioned.rs:1104–1240` exposes one output partition and drains each child stream to completion before advancing to the next one.
* `ordered_partition_streams` at `partitioned.rs:64–103` starts up to the normal execution concurrency, but each producer channel holds only two batches. Producers can decode an in-flight batch before blocking on a full channel; this is bounded prefetch, not sustained independent reduction across all partitions.
* Hail matrix layouts split at at most `262,144 / ncols` rows (`matrix_layout.rs:49,126`), about 262 rows for this 1,000-column input. A source partition has roughly 7,812 rows, so two queued batches plus in-flight work is a small prefix, not a whole partition. Once later partitions fill their queues, the ordered consumer largely services the current partition while their producers are blocked.

The actual one-stream plan and the source draining behavior are established directly. The roughly 1.2 observed CPU/wall utilization is consistent with that bottleneck. No counterfactual has yet measured how much faster a replacement plan would be, and the queue model is not an exact per-partition utilization measurement.

## What the CPU profile says—and does not say

One bounded normal-release samply recording ran a full warmup followed by three original actions. The measured repeated window was 7.17528 s and 8.60328 process core-seconds (about 1.20 average observed CPUs). The worker ended at profile offset 9.909 s, before the final recorded sample at 9.937 s; the configured 12-second recording was not truncated. All owned groups were absent afterward.

Hottest non-scheduler stacks are the Hail decoder:

* `BufferedScalars::probe`
* `prepared_scalar_row`
* `Node::append_prepared_struct_elements`
* decompression and `memmove`

Take-specific accumulation, sorting, lowering and schema setup are not dominant in this recording. This is qualitative sampled evidence; it does not prove every copy is necessary or that a function absent from a finite sample costs zero.

The generic analyzer attributes about 40.5% of interval CPU to a `__psynch_cvwait` endpoint. **This must not be read as 40.5% CPU spent spinning or waiting.** CPU delta is attached to the sampled endpoint stack; work performed before parking can be charged to the parked stack. That artifact makes a fine-grained CPU budget from those endpoint percentages unreliable. Inclusive percentages also overlap. The robust quantities here are process CPU/wall, preparation counters, the actual plan, the source buffering contract, and the identities of hot active decoder stacks.

## What Hail does differently

No new Hail process was needed: the retained matched worker's `hail.log` contains the actual lowered IR (`hail-lowered-take.txt`). `LowerTableKeyByAndAggregate` creates:

```
TableMapPartitions:
    StreamBufferedAggregate(..., buffer size 50): Take state per key
→ TableKeyBy(k)
→ TableMapPartitions:
    StreamGroupByKey(k)
    CombOpValue(Take state)
```

Hail performs local partial reduction inside independent source partitions before key redistribution and merges the bounded states afterward. It does not concatenate every original row into one stream before local aggregation. The observed CPU/wall quotient near 4.8 is consistent with its configured Spark local[4] workers plus driver activity. This is not a claim that Hail avoids every shuffle/copy or that these measurements enforce global thread parity.

## Correctness constraint: do not simply drop order

Pinned Hail's public `agg.take` doc says that with `ordering=None` there is no guaranteed ordering and the result may be nondeterministic. Vail's constructor likewise records `ordered=False` when no priority expression is supplied, and `TakeState::Unordered` means no explicit value-ordering expression.

However, current Vail tests establish a stronger stable source-prefix behavior for plain take:

* `test_parallel_execution.py:24`: plain take from 40 partitions returns `range(7)`.
* `test_logical_planning.py:66`: plain total take preserves partition order.
* `test_collection_aggregate_kernels.py:7`: mixed takes preserve first-prefix results for 1/3/7 partitions.
* `test_agg_composition_execution.py:63`: first two values include the encountered missing value `[3, None]`.

Grouped tests do not justify silently changing this broader Vail behavior. A blanket whitelist edit to `needs_order` could choose different records. The problem is implementing stable first-N by globally serial execution of every row, not that stable first-N intrinsically requires doing so.

## Smallest general improvement to investigate

Preserve the visible source-prefix semantics while moving partial aggregation before global concatenation:

1. Stamp the current `OrderedConcat` child-enumeration ordinal and the per-child output-row ordinal at the exact boundary that currently becomes `ordered(child)`: after all preceding filters/explodes and before any new repartition. Counters reset for each execution. Do not substitute an arbitrary physical-partition ID if it does not match this current encounter-order domain.
2. Let each child stream perform grouped partial take independently, retaining only its earliest N candidates per key. Each retained state entry is a defined `{ordinal, value}` record: a missing payload still qualifies and must not be mistaken for an absent candidate. The composite child/row ordinal is unique within the action.
3. Redistribute bounded candidates by key.
4. Merge each group's candidates by source ordinal and retain its first N, then remove hidden ordinal fields.

The codebase already has `scan_ordinal::SourceOrdinal` / `numbered_partitions` for freezing encounter order and a bounded explicit-order Take accumulator. These are reuse candidates, not an established drop-in solution: reuse requires proving the exact composite ordinal schema, comparison, merge/state representation, null-payload and spill behavior compatible with the contract above. Reusing proven existing machinery is preferable to inventing another sorting/shuffle framework. At most N candidates per group per source child need participate in the final merge; a full payload sort is unnecessary.

An initial rule should cover ordinary grouped plain take with proven source-order domains and repeatable keys/value expressions. Explicit TakeBy, folds, mixed order-sensitive reducers, source-order/tie contracts, RNG/effects and conditional error order need separate admission checks or the existing fallback. Preserve aggregate filters and all argument evaluation at their current semantic points. All source expressions that currently must execute still have to execute—even after the take state is full; this is not permission to skip checked/observable input work. Empty/null groups, filters, sliced batches, equal keys crossing batch/partition boundaries, multiple takes with different N, repeated execution and final merge state need public coverage.

The added ordinal arrays, partial state count, final bounded comparison, and retained-owner accounting must be measured. Vail already spends less CPU than Hail, so the target is to recover useful parallel execution without squandering that CPU advantage. No speedup claim is made until a normal-build matched counterfactual exists.

No production source, build, cloud VM, GitHub object or main branch was changed during this investigation.
