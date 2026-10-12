# Hail string conversion

`hl.str` preserves strings and missingness. Other values lower through a typed
native DataFusion scalar expression; Python only constructs the expression IR.

The pinned Hail runtime produced the [oracle observations](string-conversion-hail-oracle.json).
The Python regression tests assert primitive formatting, decimal rounding, nested
JSON, special values, and expression execution in tables.

- Integers and booleans use their ordinary textual forms.
- Top-level floats use five fractional digits in scientific notation, a signed
  exponent padded to at least two digits, and half-up decimal rounding.
- Top-level strings are returned unchanged, including control characters.
- Arrays, sets, dictionaries, tuples, and structs use compact JSON. Dictionaries
  export arrays of `{key,value}` objects, and struct fields retain schema order.
- Nested missing values become JSON `null`; missing whole expressions remain missing.
- Nested NaNs and infinities are JSON strings. Nested float32 values are widened
  to float64 before formatting, as in Hail's JSON exporter.
- Calls use genotype notation, loci use `contig:position`, and intervals use
  bracket notation. Nested values use Hail's JSON representation for their types.
- Top-level ndarrays include shape and nested row-major data; nested ndarrays
  export a JSON object with shape and flat row-major data.

Set and dictionary rendering preserves Vail's physical iteration order; it
does not emulate Scala hash-collection iteration. JSON shape and membership
match Hail, but entry order may differ. Tests compare parsed membership for
these collections.
