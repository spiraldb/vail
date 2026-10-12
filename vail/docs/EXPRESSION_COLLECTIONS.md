# Expression collection additions

## Elementwise array arithmetic

Numeric arrays support `+`, `-`, `*`, `/`, `//`, `%`, `**`, and unary negation.
Array/scalar and scalar/array operations broadcast the scalar; array/array
operations require equal lengths and fail at evaluation if lengths differ.
Missing arrays and missing elements propagate. Numeric promotion and scalar
arithmetic reuse the existing expression lowering. Index ranges and array maps
lower to existing DataFusion components; no new arithmetic kernel is added.

## Expression-valued interval filters

`filter_intervals` accepts an unindexed array expression of intervals, or a
Python list mixing interval literals and interval expressions. Global expressions
are supported. Bounds are evaluated once, as in Hail's frontend, and the existing
interval-read path preserves fragment pruning. Missing interval arrays/elements
and row-indexed interval expressions are rejected, matching the API's unindexed
requirement.

## Expression-valued dictionaries

Dictionary construction accepts expression keys and values, and arrays/sets of
pairs. Missing pair entries are removed; missing keys and values are valid.
Keys use the existing typed canonical encoding. DataFusion encoding, array
sorting/distinct, filtering, mapping, and element selection handle construction;
no dictionary deduplication kernel is added.

The Hail runtime oracle exposes two duplicate-key behaviors:

- A constant array of pairs keeps the **last** value. Hail's constant interpreter
  converts pairs with Scala `toMap`.
- Runtime pairs and expression-keyed Python mappings keep the **first** value.
  Hail's emitted `ToDict` uses a stable sort and skips later equal keys. The
  mapping frontend creates a stream zip, so it follows this emitted path even
  when its expression keys are constants.

Vail selects the corresponding first/last DataFusion array element for these
constructor paths. Oracle scripts and outputs are saved alongside the validation
report. Missing keys can be retrieved; indexing an absent key raises an error.
