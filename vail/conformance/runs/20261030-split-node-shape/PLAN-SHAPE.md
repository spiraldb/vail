# Actual split HTS DataFusion node shapes

Captured on 2026-10-30 from normally built source 5ac7ae023 plus temporary opt-in trace hooks. The hooks record real action binding and runtime semantic programs; they do not change optimization or data computation. No new fusion has been implemented. The trace artifact is identified in build-receipt.json and the raw archive. The admitted production remains bf9d8aed5.

## Actions and input

The same persisted first 200 public profile.vcf.bgz records, 2,535 samples and four source partitions were used for three independent actions: default split force-count (204 rows), permit_shuffle split force-count (204 rows), and default split followed by public localize_entries(...).collect() (204 rows, 517,140 defined entry slots). These are diagnostic executions, NOT timings. The full-value action peaked near 1 GB process RSS. The unchanged/moved branch filter precedes the final entry update in both actual default plans.

## Relational operators

Below is an abbreviated DATA-FLOW view, from source to result. It omits row-schema bookkeeping projections, global bindings and execution wrappers, but not the important array/branch boundaries.

```text
VicebergScanExec — 4 source partitions
  ↓
ProjectionExec: normalize/sort alternate descriptors into a row list
  ↓
ProjectionExec: cells = select_fields(prepare_entries(cells, row, columns))
  ↓
ProjectionExec: expose alternate-descriptor list as an explode column
  ↓
UnnestExec: expand descriptors; repeat other columns, including cells
  ↓
ProjectionExec(s): construct new locus/alleles, a_index, old_locus/alleles
  ↓
FilterExec: unchanged-locus OR moved-locus condition (one per branch)
  ↓
ProjectionExec: cells = select_fields(update_entries(cells, row, columns))
  ↓
ProjectionExec: expose new key fields and partition tag
  ↓
unchanged branch: CheckKeyOrderExec — 4 input views
moved branch:     OrdinalExec → SortExec — 4 input views
  ↓
UnionExec — 8 inputs
  ↓
SortPreservingMergeExec → TablePartitionsExec — 1 output partition
```

Both default actions retain this singleton merge. In the captured force-count action, the surrounding ScalarSubqueryExec/TablePartitions wrappers prevent the terminal-count rewrite from stripping it. Do not substitute the earlier nonexecuting physical_plan() diagnostic for this actual action plan. The shuffle action has one source/preparation/explode route and a different ordering subtree; it is not simply the default tree with one condition changed.

The plan-text tree repeats shared subplans under partition views. It is NOT an invocation counter: four source scans printed under each view do not mean all four are executed by each view. SHAPE-SUMMARY.json counts distinct visited physical Arc identities using the deduplicating expression traversal, not repeated text lines. Arc identity also does not establish shared evaluated batches. The independent prior kernel counters establish duplicated preparation on the default route.

## What the expression nodes actually are

The two `cells = ...` projections above both contain this actual physical expression shape:

```text
ProjectionLabelExpr("__vail_matrix_cells")
  ScalarFunctionExpr: vail_semantic_kernel_array_transform_...
    ScalarFunctionExpr: vail_semantic_kernel_hail_operation_...
      cells column
      row column
      bound scalar-subquery/global columns
```

The terse Projection(__vail_matrix_cells) display hides these children; the expression trace expands them. This is not a Vortex list_transform node. DataFusion sees two Vail scalar UDFs. Their retained typed semantic IR and first runtime compiled programs reveal the following:

```text
inner UDF: AlignedMatrixMapExpr
  entry-presence CASE
  entry body as nested physical expressions

outer UDF: HigherOrderFunctionExpr(array_transform)
  input: materialized result of the inner UDF
  LambdaExpr
    construct cell.value using selected entry fields
    preserve missing-entry value through a CASE
```

Before Unnest, the aligned body calls variant_entry and adds a temporary field containing all alternate AD/PL outputs. Its following array_transform drops original AD/PL. After Unnest/filter, the aligned body downcodes GT and selects AD/PL for a_index, computes GQ, and preserves other fields. Its following array_transform removes the temporary field and restores original entry selection/order. These are two concrete adjacent transformation boundaries, one on each side of Unnest.

Observed default runtime programs (IDs are LOCAL capture labels, never optimizer identities):

| Program | Semantic root | Important compiled content |
|---|---|---|
| 6 | normalization/sort expression | array_filter → array_transform → array_sort |
| 7 | AlignedMatrixMap | entry presence guard, variant_entry |
| 8 | ArrayMap | field selection dropping original AD/PL |
| 47 | AlignedMatrixMap | per-output AD/PL indexing; GT allele map/sort/cast; GQ filter/sort/index/CASE |
| 48 | ArrayMap | final entry-field selection |

Full collection additionally observes program 65, an ArrayMap used by localize_entries. Runtime compilation preserves batch-oriented higher-order functions and existing scalar functions. A ScalarUDF label alone is not evidence of scalar interpreter execution. Repeated expression displays are not invocation counts; local expression caching and selected branch domains must be inspected before asserting recomputation.

## Array shapes at the boundary

```text
input cells:
  list<cell{value: nullable Entry{GT, AD:list<int>, PL:list<int>, DP, GQ}}>

cells immediately before Unnest:
  list<cell{value: nullable Entry{
    GT, DP, GQ,
    prepared: {
      ad: list<list<int>>,   // alternate → two output depths
      pl: list<list<int>>    // alternate → haploid/diploid output likelihoods
    }
  }}>

row's exploded list:
  list<{locus, alleles:list<string>, a_index:int}>

final cells per emitted alternate row:
  list<cell{value: nullable Entry{GT, AD:list<int>, PL:list<int>, DP, GQ}}>
```

Unnest crosses a relational boundary with the large prepared cells list already produced. A simple map/map composition can remove the adjacent entry transform boundaries; it does not by itself remove this Unnest payload gathering or unify the two branches.

## Optimizer guidance, not an implemented rewrite

1. First investigate composing each field-selection map into its existing AlignedMatrixMap body. This is a concrete, general producer/consumer pattern at both observed boundaries, and need not cross Unnest, filters, sorting or union.
2. Preserve single evaluation of the inner entry body, all demanded errors, field order, lambda/global/row/column bindings, and parent validity. In particular, the outer map constructs a defined cell Struct with value=NULL for a NULL cell ancestor, whereas preserve_cell=true on the aligned operator preserves that ancestor mask. A naive body substitution can change semantics; establish the nonnull-cell invariant at an eligible boundary or preserve the outer semantics explicitly.
3. Removing prepared-list copying through Unnest requires a further transformation across the relational expansion boundary, or an execution representation that keeps parent references until the final entry outputs are written. This is a larger change than map/map fusion.
4. Sharing evaluation across branches and retaining parallel output partitions are separate problems. No cache/replay/repartition framework is admitted by this trace.
5. There are many named_struct projections and opaque semantic GetField functions in the row-key bookkeeping. They also limit ordinary field-access simplification. Their shape alone is not a cost measurement or permission to bypass nested-null/type semantics.

## Reading the raw captures

- `force-default/executed-count-part-0-1.txt`: plan submitted to count execution, after action binding/count rewrites.
- `collect-default/ordered-prepare-0.txt`: bound plan prepared for the full-value collection.
- matching `*-expressions.txt`: physical expression trees at each distinct visited relational node.
- `kernel-7-...txt`, `kernel-8-...txt`, `kernel-47-...txt`, `kernel-48-...txt`: first entered runtime program per relevant semantic-UDF instance.
- `logical-*-before/optimized.txt`: compiler-stage plans after action resolution; first/before/after labels do not imply every node executed.

Node labels/types in expression trees are abbreviated to 240 characters; whole trees did not hit the 2 MB/depth-100 guard. Semantic files originally named .json cannot serialize RuntimeLiteral and explicitly fall back to Debug text. The accompanying .debug.txt files contain that same marked fallback; they are not JSON. The first-evaluate hook captures observed program shapes, not all invocations or every internal branch. Hooks are restored after capture; no diagnostic code is shipped.
