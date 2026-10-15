# Missing type operations

Inventory as of 2026-09-29, comparing Vail's public Python expression API
against the local Hail checkout at
`e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

This is an implementation checklist, not a claim that every existing operation
has complete semantic parity. The comparison uses source inspection and small
frontend probes; it is not a differential execution test of every operation.
Types, literal construction, storage, globals, grouping, and comparison support
do not imply that the corresponding expression methods are implemented.

Reference sources:

- Hail: `hail/hail/python/hail/expr/expressions/typed_expressions.py`,
  `hail/hail/python/hail/expr/functions.py`, and `hail/hail/python/hail/nd/`.
- Vail: `vail/python/vail/expressions.py`, `vail/python/vail/types.py`,
  `vail/python/vail/__init__.py`, and `vail/python/vail/nd.py`.

Paths above are relative to the outer repository. In the lists below, `hl` means
the Hail-shaped Vail module, and method names refer to expression objects.
`hl.agg` aggregators and Table transformations are separate inventories.

## Dictionaries

Implemented through DataFusion expressions: typed literals; `hl.empty_dict`;
`hl.dict` identity conversion and Python mappings (including expression values);
`get`, `contains`, `keys`, `key_set`, `values`, `items`, `size`, `length`,
`hl.len`, `map_values`, and `hl.array(d)` as key/value tuples.

- [ ] `hl.dict(collection)` for an expression-valued array/set of pairs. DataFusion's `map` rejects missing and duplicate keys; Hail accepts missing keys and retains one value per duplicate key. No custom constructor was added.
- [ ] `d[key]` with Hail's error on an absent key. DataFusion's lookup returns missing instead, and the pinned DataFusion functions do not expose a matching error/assertion expression. `d.get(key, default)` is supported and distinguishes an absent key from a present missing value.

See [Dictionary and set execution](DICTIONARY_SET_OPERATIONS.md).

## Sets

Implemented through DataFusion expressions: `hl.empty_set`; `hl.set` from
supported arrays or Python sets; `add`, `remove`, `contains`; `difference`,
`intersection`, `union`, `is_subset`; operators `-`, `&`, `|`, `^` and reflected
forms; subset/superset comparisons; `filter`, `flatmap`, `any`, `all`, `find`,
`starmap`, `map`, `length`, `size`, `hl.len`, projection from sets of structs,
and `hl.array(s)`. `map` now returns a deduplicated set. `flatmap` skips missing
inner collections using DataFusion `array_compact` before `flatten`.

- [ ] `hl.set(array)`, `map`, `flatmap`, and `starmap` when output deduplication contains floats, nested sets/dictionaries, or ndarrays. DataFusion's distinct kernel merges signed zeros and does not implement Vail's canonical equivalence for unordered nested collections. These inputs explicitly raise `NotImplementedError`. Algebra, membership, filtering, and updates on existing such sets use composed DataFusion expressions and preserve Vail key equality.
- [ ] `fold`: the pinned DataFusion higher-order functions have no collection fold/reduce with an arbitrary state lambda.
- [ ] `group_by`: no built-in per-row collection grouping into a dictionary of sets.
- [ ] Exact integer and float32 set `hl.sum`/`hl.product`: DataFusion's array sum/product use float64. Float64 sum/product and numeric min/max are supported, including empty/missing inputs and `filter_missing`.

Generic collection helpers listed below still need their Hail-supported set overloads.

## Tuples

Implemented: `ttuple`, literals, `hl.tuple(iterable)` with row/global expressions,
constant non-negative integer indexing, Python-style slicing (negative bounds,
negative steps, and empty results), Python `len`, iteration/unpacking, and
Hail's length-based `bool`. Slicing preserves a missing parent tuple. Empty
tuples remain distinct from empty structs through schemas and storage.

Construction uses DataFusion `named_struct`; element access uses `get_field`;
slicing selects and repacks fields with DataFusion `CASE` for missingness. There
is no custom tuple evaluation kernel. See [Tuple operations](TUPLE_OPERATIONS.md).

Hail's `TupleExpression.count` and `index` explicitly say “Do not use this
method”; they are abstract-sequence compatibility placeholders, not useful
Hail APIs to port. Negative integer indices now raise `IndexError`, matching
the pinned Hail implementation. Negative slice bounds remain supported.

## Ndarrays

Already present: `tndarray`, encoding/decoding of data and shape, and
`hl.nd.array` for concrete Python/NumPy data through Arrow, and lazy `hl.nd.arange`
with expression-valued int32 bounds/step using DataFusion range/array expressions.
`nd.array` itself does not yet accept row/global expression inputs as Hail does.

- [ ] Expression-valued `hl.nd.array`, including nested array expressions.
- [ ] Shape and layout: `a.ndim`, `a.shape`, `a.T`, `a.transpose(axes=None)`, `a.reshape(*shape)`.
- [ ] Indexing/slicing: multidimensional `a[...]`, mixed slices and indices, and `hl.nd.newaxis`.
- [ ] Element transformations: `a.map(f)`, `a.map2(other, f)`.
- [ ] Numeric operators: unary `-`, `+`, `-`, `*`, `/`, `//`, reflected forms, and broadcasting.
- [ ] Matrix multiplication: `@` and reflected `@`.
- [ ] Reduction: `a.sum(axis=None)`.
- [ ] Constructors: `hl.nd.from_column_major`, `full`, `zeros`, `ones`, `eye`, `identity`.
- [ ] Array manipulation: `hl.nd.diagonal`, `concatenate`, `vstack`, `hstack`.
- [ ] Elementwise extrema: `hl.nd.maximum`, `minimum`.
- [ ] Linear algebra: `hl.nd.solve`, `solve_triangular`, `qr`, `svd`, `eigh`, `inv`.

The constructor's literal-only behavior is partial support, rather than a
missing `nd.array` name. The other listed ndarray operations are absent.

## Strings

Vail deliberately uses valid UTF-8 and Unicode scalar positions, not Hail's
UTF-16 units. Length, indexing, slicing, search, reversal, translation, hamming
and entropy follow codepoint semantics for literals and dynamic expressions.
Stepped slices and general codepoint translation are supported. Regex methods
accept dynamic patterns and replacements using the DataFusion/Rust regex engine;
Java-only regex constructs remain outside that engine's grammar.

Separate missing APIs: Java-specific `parse_int*`/`parse_float*`, `hl.format`,
`hl.json`, and `hl.parse_json`. See [String operations](STRING_OPERATIONS.md)
for the explicit compatibility decision and full operation contract.

## Arrays and shared collection helpers

Already present on arrays: expression-valued list construction, integer
indexing, slicing (including steps), `map`, `filter`, `flatmap`, `length`, and
numeric-array `hl.sum`, `hl.product`, `hl.min`, `hl.max`.

- [ ] Shared expression methods on arrays: `any`, `all`, `find`, `fold`, `group_by`, `starmap`, `size`.
- [ ] Array methods: `aggregate(f)`, `contains`, `head`, `first`, `last`, `index` (value or predicate), `append`, `extend`, `scan(f, zero)`, `grouped(group_size)`.
- [ ] Projection from arrays of structs: `a.field` and `a['field']`.
- [ ] Numeric-array arithmetic: unary `-`; elementwise/broadcast `+`, `-`, `*`, `/`, `//`, `%`, `**`, including reflected forms.
- [ ] Constructors: `hl.empty_array`, `hl.zeros`, `hl.repeat`.
- [ ] Extend `hl.array` to one-dimensional ndarray conversion. Set and dictionary conversions are implemented.
- [ ] Functional wrappers: `hl.map`, `filter`, `flatmap`, `any`, `all`, `find`, `fold`, `group_by`, `starmap`.
- [ ] Prefix operations: `hl.array_scan`, `hl.cumulative_sum`.
- [ ] Zipping/indexing: `hl.zip` (including `fill_missing`), `hl.enumerate`, `hl.zip_with_index`.
- [ ] Ordering/search: `hl.sorted` (key and reverse options), `hl.reversed`, `hl.argmin`, `hl.argmax`, `hl.binary_search`, `hl.shuffle`.
- [ ] Nesting: `hl.flatten`, `hl.keyed_intersection`, `hl.keyed_union`.
- [ ] Reductions: `hl.mean`, `hl.median`, `hl.nanmin`, `hl.nanmax`.
- [ ] Complete `hl.min`/`hl.max` overloads: multiple expressions and scalar inputs, beyond one numeric array.
- [ ] General length: `hl.len` across Hail-supported types.

`aggregate` on an array is separate from total or grouped Table aggregation;
implementing `Table.aggregate` does not supply it. Likewise, array `group_by`
is a collection operation producing a dictionary, not `Table.group_by`.

## Implementation and verification requirements

- Prefer DataFusion expressions/functions and Arrow kernels; add typed IR
  and DataFusion lowering for remaining operations. Do not evaluate row
  expressions in Python.
- For ndarray linear algebra, use a suitable library instead of implementing
  numerical algorithms from scratch.
- Verify return types, outer and element missingness, empty inputs, nested
  values, invalid arguments, and expression-valued arguments against Hail.
- Keep ordering/equality consistent with the shared key implementation and
  the documented intentional Hail divergences.
- Mark an item complete only after its public API, lowering, and behavior
  tests are present; a constructor or an internal storage field is insufficient.
