# Dictionary and set execution

The Python collection frontend constructs lazy scalar expression IR. Rust lowers
it to DataFusion physical expressions inside TableIR plans. This change adds no
Vail collection evaluation kernel, UDF implementation, or Python row evaluator.

| API | DataFusion execution |
| --- | --- |
| Dictionary keys, values, items, map_values | `array_transform` lambda over Arrow list-of-entry structs; struct projection; cast retaining the dictionary type tag |
| Dictionary get | `array_filter` on key equality, `array_length`, `array_element`, and `CASE`; default applies only when the key is absent |
| Dictionary/set contains | `array_filter` and `array_length` with missing-aware key equality |
| Dictionary/set size and array conversion | `array_length` or Arrow list cast retaining element storage |
| Set difference, intersection, union for compatible elements | `array_except`, `array_intersect`, `array_union` |
| Set algebra for float or unordered nested elements | `array_filter`, existing Vail comparison expressions, and `array_concat`; preserves IEEE total-order key equality |
| Set remove | `array_filter` |
| Set add | Singleton `make_array`, then union |
| Set construction and map | `array_distinct`; map uses `array_transform` first |
| Set flatmap | `array_transform`, `array_compact`, `flatten`, `array_distinct` |
| Set any, all, find | `array_any_match`, Boolean negation, `array_first`; find skips missing candidate values as Hail's fold-based implementation does |
| Set starmap and field projection | Tuple/struct projection inside `array_transform`, then distinct |
| Set float64 sum/product, numeric min/max | `array_sum`, `array_product`, `array_min`, `array_max`; `CASE` supplies Hail empty identities and missing-element propagation |

Collection casts change list field names used as Vail type tags. They cannot
change element storage. Native function calls use an explicit whitelist and
validate arity, argument representation, and inferred output types before execution.

## Operations requiring DataFusion support

These raise `NotImplementedError`; no custom replacements were added:

- **Expression-valued dictionary construction:** DataFusion `map` rejects missing
  and duplicate keys. Hail supports both. Concrete Python mappings and mappings
  with expression-valued values work because their keys are already unique.
- **Strict dictionary indexing (`d[key]`):** Hail errors for an absent key;
  DataFusion returns missing. The pinned built-in functions do not provide a
  compatible error expression. Use `get` for optional lookup.
- **Set deduplication of floats, nested sets/dictionaries, and ndarrays:** The
  distinct kernel merges signed zeros, and physical row equality is insufficient
  for unordered nested collections. Construction/mapping requiring this
  deduplication are rejected. Algebra on existing sets uses DataFusion expression
  composition with the existing shared key comparison and does work.
- **Set `fold` and `group_by`:** DataFusion 55's collection higher-order function
  registry has transform, filter, any-match, and first-match; arbitrary state fold
  and per-row grouping into a dictionary of sets are absent.
- **Integer/float32 set sum/product:** DataFusion's array functions use float64,
  losing exact large integers and changing float32 accumulation semantics.

The existing Python localization limitation for signed zeros in Python sets and
dictionaries remains: use array views to inspect native members without Python
collapsing distinct zeros.

Regression coverage includes missing dictionaries, absent versus missing values,
missing keys and set members, nested keys, signed zeros and NaN payloads,
collection lambdas, captured globals and aggregations, independent partitions,
and storage round trips. Native tests check built-in function lowering and reject
malformed IR at planning time.

Sources: the pinned DataFusion 55 registry sources and
[DataFusion scalar function documentation](https://datafusion.apache.org/user-guide/sql/scalar_functions.html).
Hail semantics follow the checked-out `typed_expressions.py` and
`test_expr.py` dictionary/collection tests.
