# Encoded scan projection implementation notes

The ordinary DataFusion scan currently requests field indices and filters. Vail additionally carries nested field paths into Iceberg's Vortex projection, but computed scalar expressions are not carried. Vortex's Arrow conversion materializes primitive dictionary outputs before DataFusion's expression evaluator.

| Operation | Pinned Vortex capability | Current Vail scan | Safe next use |
| --- | --- | --- | --- |
| Field/nested projection | get_item/select/list_project; parent struct validity masking | Yes | Keep existing pruning and logical validity |
| Row predicates | comparisons/boolean/null/like expressions and scan filter | Iceberg conversion supports subset; DF residual remains | Broaden exact translation only with null/cast semantics tests |
| Primitive arithmetic/cast | expression builtins and encoding rewrite rules | Above Arrow boundary | Computed projection with operation-specific overflow/error proof |
| List length | list_length | Above Arrow boundary | Per-row count/length where null/filtered-entry semantics match |
| List sum | list_sum_opts, grouped SumV2 | Above Arrow boundary | i32→i64 with proven bounded child count and null-skipping; preserve null lists, convert only proven empty/all-null to zero |
| Dictionary grouped sum | Existing extension dispatch before child canonicalization | Missing upstream kernel | Prototype here adds i32→i64 bounded grouped SumV2, avoiding expanded child allocation |
| List min/max | Generic grouped aggregate machinery; no direct expr list_min/list_max helper in this pinned API | Above Arrow boundary | Add semantic translation through grouped machinery; do not claim existing direct expression |
| List contains | list_contains | Above Arrow boundary | Requires exact equality/null semantics proof |
| Global aggregation | Vortex aggregate functions exist | DataFusion partial/final aggregate | Retain DF global aggregate; scan computes only row-preserving scalar columns initially |

## Computed projection contract

Use a typed expression/capability contract rather than schema/benchmark names. It must flow from transparent DF logical projection through provider, LocalRead, stream task projection and Iceberg FileScanTask. Native scan output schema must explicitly include synthetic computed columns. Simply changing Vortex's projection is insufficient: Iceberg's RecordBatchTransformer expects the original field-ID schema and would attempt to evolve a scalar replacement back to a list.

Proposed adapter shape: pack passthrough original fields separately from computed fields inside Vortex, run Iceberg schema evolution/metadata transformation only on passthrough fields, then append the computed results under the declared output schema. The source operands used solely by computed expressions need not cross Arrow. Preserve position/equality deletes, partition constants, virtual positions and row IDs. A computation whose file input needs schema evolution must either be normalized before encoded evaluation or use a correctness-preserving fallback after evolution; a reader-time binding failure cannot become a previously successful query error.

For joins, row-preserving deterministic projections may move below a join only when they depend solely on one side and have no outer-join null-extension/error-evaluation change. Keep multiplicity unchanged. Global reductions may not cross joins absent a cardinality proof. Use DF's projection/filter optimizer where it can see semantics; provide source conversion only after dependency analysis, retaining unsupported expressions above the scan.

## Prototype status

Dict grouped SumV2 uses the library's existing encoding dispatch, not a Vail-specific executor. i32 values with group length <=u32::MAX provably fit i64. Other element widths fall back. Codes and dictionary values decode once; visible group envelope clips the code owner before decoding. Code/dictionary bitmap presence selects one const-generic physical implementation per batch. The loop performs required data-dependent null checks and dictionary lookup, then adds directly to i64; no dense expanded child or widened input array. Source compilation passed. Tests/performance/review remain required; no speedup claimed.
