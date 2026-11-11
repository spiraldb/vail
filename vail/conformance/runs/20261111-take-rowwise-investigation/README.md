# Take/rekey and many row-wise aggregations: execution account

Two user-requested subagents investigated the completed six-case laptop rerun. No production code, allocator, runtime configuration or build changed. All diagnostics used the exact installed normal release native `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193` (production `79744c031`, measured main `8bc7ced62`). The subsequent main changes are reports only. All workload leases were exclusive; every owned worker/sampler group was verified absent before another workload. No cloud or GitHub activity.

| Case | Hail wall / core-s | Vail wall / core-s | Current deficit |
|---|---:|---:|---|
| Take/rekey | 1.00012 / 4.83869 | 2.41742 / 2.94077 | Vail 2.42× slower wall, but 39.22% less CPU |
| Many row-wise aggregations | 1.68451 / 7.33010 | 0.87759 / 8.59354 | Vail 1.92× faster wall, but 17.24% more CPU |

The [complete six-case rerun](../20261111-six-slower-laptop/README.md) contains the matched measurements. The separate profiles and decomposition below establish cost centers and execution contracts; they are not new before/after speedups. CPU includes the observed-descendant lower-bound limitation. Hail Spark local[4] and Vail's existing Tokio32/Rayon4 configuration do not impose equal total core limits.

## Take/rekey: the partial reduction is on the wrong side of the serial boundary

The actual post-force-count plan is:

```
32 source partitions
  → ordered concatenation into ONE stream
  → partial grouped take on that ONE stream
  → hash repartition into 32 streams
  → final grouped take
  → count
```

There is no terminal global sort in this action. The serial boundary survives the force-count rewrite. `needs_order` treats take as order-sensitive; grouped lowering orders the full child before partial aggregation. Producers prefetch only two batches per source, so they cannot keep independently reducing while the consumer drains one entire partition at a time. Typical batches are around 262 rows; each source partition has roughly 7,812 rows.

The main cost is not Python or planning: public construction is about 3 ms, and existing native preparation counters total 19.726 ms of a separate 2.298-second terminal action. The sampled repeated window consumes 8.603 core-seconds over 7.175 seconds—about 1.20 observed CPUs. Hottest active stacks are Hail decoding. Take-specific accumulation and planning are not dominant. These observations establish the serial execution graph and make it the leading explanation for the latency gap; no counterfactual has measured what fraction of that gap removing the boundary would recover.

Hail's retained actual lowered IR instead partially aggregates independently inside each source partition, redistributes bounded states, and combines them. It does not concatenate every original row globally before doing local take.

A blanket unordered-take whitelist would be unsafe for current Vail behavior: its tests require a stable source prefix, even though Hail's plain take does not guarantee that order. The general solution to investigate is **parallel local bounded take with source ordinals**, then a bounded final merge by ordinal. Stamp the exact current ordered-child output domain after preceding filters/explodes and before a new repartition; retain missing payloads as eligible defined candidates. This preserves which first N records are selected without serializing the whole input. Existing ordinal and TakeBy components are reuse candidates subject to exact state/schema/null/spill compatibility. Explicit ordering, folds, mixed reducers and effects need separate admission or fallback. This design is unimplemented.

[Full take/rekey account, phase counters, plan and contract constraints](take-report.md).

## Row-wise aggregation: repeated predicate producers, not slow counters

The actual post-force-count plan shares one input source and one 33-output result struct. It does not rerun the full 33-reducer program 33 times or unnest all entries. However, it still has 33 sibling predicate/reduction roots: 17 mapped/segmented roots and 16 opaque Hail-operation roots. Earlier fusion does not combine all those sibling reductions.

Public frontend/IR construction costs 21.9 ms / .0219 core-s in a separate decomposition; the terminal action costs 823 ms / 8.58 core-s. Reusing the already-constructed logical table still costs 8.59 core-s. Frontend construction tuning cannot explain most of the excess work.

Approximate exclusive attribution of sampled CPU intervals:

| Cost center | Fraction | Core-s per profiled action |
|---|---:|---:|
| Projection/expression evaluation | 52.68% | 4.557 |
| Hail reader decoding | 33.10% | 2.863 |
| Wait/scheduler endpoints | 9.39% | .812 |
| Other runtime | 4.31% | .373 |
| Planning/preparation | .52% | .045 |

Inside the projection category, the largest whole-profile components are the fallible integer numeric kernel (17.15%, about 1.48 core-s/action), list-index checking/gathers (11.99%, 1.04), Call expansion/access (5.46%, .47), and numeric casts (4.79%, .41). The final segmented Boolean reductions are only .60%, about .05 core-s/action. Rewriting the counters alone targets a small part of the measured work.

The exact constructed 521-node IR proves repeated equivalent pure producers within the same input-owner/context: `AD[0]` twice, `PL[0]` twice, `GQ % 2` twice, `DP % 2` twice, and `GQ / 5` and `DP / 5` four times each. List indexing constructs child indices and an Arrow primitive gather result per batch. Integer modulo uses a fallible array-array kernel, including runtime zero/sign handling, even for the scalar literal operand in this workload. These are repeated scalar-derived vectors and checks—not copies of every nested entry field.

The producer expressions are hidden inside distinct callback UDFs. DataFusion's ordinary projection CSE cannot see their internal common subexpressions. The existing block compiler also does not admit all modulo/divide/floating comparison/Call/index operations used here. A shared result struct and an output builder therefore do not give this query a shared multi-reducer program.

Hail's actual lowered IR has one entry `StreamFor`, after the missing-entry filter, updating 34 scalar states: nine count-where sums, 24 fraction numerators and one shared denominator. It keeps predicate calculations inside the compiled entry loop rather than requiring separate primitive/Boolean vectors and reduction scans for each predicate. The IR still contains repeated scalar arithmetic; no particular JIT CSE or machine-code strength reduction is claimed.

The general directions are exact pure-producer CSE across same-domain sibling reductions, scalar-aware numeric dispatch, and a prepared joint block program feeding the existing native count/fraction states. Shared work must preserve input-owner identity, active masks, missing-list/bounds behavior, conditional demand, error order and effects. These changes are unimplemented. No Java CPU profile was captured, so the entire Vail/Hail CPU difference cannot be attributed to aggregation or decoding by subtraction.

[Full row-wise account, exact duplicate audit, Hail IR and source references](rowwise-report.md).

## Evidence, limitations and review

Profiles use positive `threadCPUDelta` intervals attributed approximately to sampled endpoint stacks. Wait endpoints can receive earlier active CPU; 9.39% in the row-wise profile and 40.5% at a take wait leaf do not mean CPU spent sleeping or spinning. Inclusive call-tree percentages overlap. Category methods and limitations are retained. Finite sample absence is not proof of zero cost, and potential savings are not obtained by adding category percentages.

`take-evidence.zip` and `rowwise-evidence.zip` contain the complete sealed agent artifacts, including normal Firefox-compatible profiles, symbol sidecars, actual plan traces, Hail lowered IR, source/literal audits, diagnostic scripts, markers, raw samples and cleanup receipts. Corresponding `*-manifest.json` hashes every member. Both reports are also uncompressed for reading. Durable viewer artifacts are under the task's visualization directory in `take-rekey-20261111` and `rowwise-aggs-20261111`. `manifest.json` seals this combined packet.

The row-wise AFTER partition-0 expression roots are byte-equal to BEFORE roots and preserve all 33 outputs; raw trace includes all 16 AFTER partition plans. Take's actual AFTER plan is likewise used. The first row-wise query-free literal serialization used pointer repr and did not prove literal equality; its retained corrected Arrow-C-stream export supplies exact literals. Neither inspection executes a query. No extra benchmark cases or feature variants were added.

Required independent reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` independently checked source/contracts, Hail IR, profile windows, categories, literal-normalized duplicates and raw math. Report admission does not constitute implementation/performance admission for either proposed optimization. No code fix from this investigation has been merged.
