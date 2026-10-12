# Tuple construction and slicing

```python
value = hl.tuple([table.idx, table.label, table.global_value])
table = table.annotate(pair=value[::2], reverse=value[::-1])
```

`hl.tuple` accepts an iterable of values or expressions. The Python frontend
records a lazy `Tuple` expression with a heterogeneous type. Rust lowers it to
DataFusion's built-in `named_struct`, using reserved positional field names
`__vail_tuple_0`, `__vail_tuple_1`, etc. A DataFusion cast retains the declared
field types and nullability. Tuple field access lowers to built-in `get_field`.
No Vail tuple evaluation kernel is used.

A slice has a statically known result type. Python computes its field indices
using Python's slice rules, and constructs projections of those fields followed
by a tuple constructor. This computes no row values in Python. A DataFusion
`CASE` returns a typed missing tuple when the original tuple is missing,
including for empty slices. A present tuple containing missing fields remains
present.

Integer indexing accepts only constant Python integers in `[0, len(tuple))`,
matching Hail. Negative integer indices raise `IndexError`. Slice bounds and
steps may be negative; a zero step raises `ValueError`; expression-valued
indices/bounds are rejected. Python length, iteration/unpacking and Boolean
conversion use the tuple's static type. A missing nonempty tuple therefore has
positive length and converts to `True`, matching Hail.

## Empty tuples

DataFusion's `named_struct` requires at least one field, and an Arrow struct with
no fields cannot identify whether it represents Hail's empty tuple or empty
struct. Empty tuples use one reserved Boolean field, `__vail_tuple_empty`, whose
constructor value is `False`. It is a physical type tag, not a tuple element.
The built-in `named_struct` constructs it; native schema decoding recognizes it
as `tuple()`. Python wire encoding uses the same tag. User-visible length,
iteration, values, and string conversion remain those of an empty tuple.

This changes the previously ambiguous physical representation of empty tuples.
No migration of old ambiguous zero-field-struct data is attempted. Nonempty tuple
representation remains the same. Regression tests cover both empty and nonempty
tuple storage round trips, empty tuple keys/globals, missingness, collection
lambdas, grouped/total aggregation, and lookup expressions inside constructors.
Native tests assert `named_struct`/`get_field` lowering and verify the empty type
is distinct from an empty struct.

No requested tuple construction or slicing API requires a custom evaluator or
remains blocked on DataFusion support.

Sources: the pinned Hail `TupleExpression` and `hl.tuple` implementations, and
DataFusion 55's `core/named_struct.rs` and `core/getfield.rs`; see
[DataFusion scalar functions](https://datafusion.apache.org/user-guide/sql/scalar_functions.html).
