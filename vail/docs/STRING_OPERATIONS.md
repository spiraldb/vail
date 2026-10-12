# String operations through DataFusion

Python builds lazy scalar IR. Rust validates a whitelist of string functions and
lowers it to DataFusion scalar functions, casts, `CASE`, and binary expressions
within existing TableIR plans. No new Vail string UDF/evaluation kernel or Python
row evaluator is used. String offset arithmetic uses DataFusion integer binary
expressions directly rather than Vail's Hail integer-arithmetic adapter.

| Hail API | DataFusion lowering |
| --- | --- |
| `+`, reflected `+` | `concat`, with `CASE` to propagate missing inputs |
| `*`, reflected `*` | `repeat`, with `CASE` returning empty string for non-positive counts |
| `length`, `hl.len` | `character_length` plus `regexp_count` of supplementary Unicode scalars, giving UTF-16 units |
| `lower`, `upper` | Built-in Unicode case conversion |
| `strip` | `btrim` with the explicit U+0000..U+0020 Java trim character set |
| `contains`, `startswith`, `endswith` | `contains`, `starts_with`, `ends_with` |
| Unbounded `find` | `strpos`, `substr`, UTF-16 length of the preceding prefix, and `CASE` for absent/missing matches |
| BMP literal slicing/indexing | `substr` with normalized bounds; dynamic indexing is rejected because substring does not raise Hail's out-of-bounds error |
| BMP literal `reverse` | `reverse` |
| Literal-pattern `matches` | `regexp_like`; full matches use absolute start/end anchors |
| Literal-pattern/plain-replacement `replace` | `replace` |
| Literal-delimiter unlimited `split` | `string_to_array`, with `CASE` preserving Hail's empty-input result |
| Literal character `translate` | `translate`; deletion entries follow replacement entries in its arguments |
| String collection `join` / `hl.delimit` | `array_to_string`, with `"null"` for missing members |
| BMP literal `reverse_complement` | `reverse` and `translate` |

## Explicit gaps

- **General UTF-16 indexing/slicing/reversal:** DataFusion operates on Unicode
  scalars. Hail indexes UTF-16 units, can slice inside surrogate pairs, and its
  runtime reversal of supplementary characters differs from Unicode-scalar
  reversal. Only BMP literal inputs prove the same behavior without a runtime
  compatibility evaluator. A full identity slice works for arbitrary input.
  Bounded `find` is also rejected. Out-of-bounds constant indices raise early.
- **Java regular expressions:** DataFusion uses Rust regex syntax and semantics.
  Backreferences, lookaround, Unicode classes, anchors and other constructs do
  not have universal Java parity. Public regex methods accept only patterns
  containing no regex metacharacters. Capture extraction, templates, dynamic
  patterns, zero-width split/replacement and split limits are rejected.
- **Dictionary translation:** A DataFusion character translation maps character
  positions in two strings. It cannot directly consume a dynamic dictionary or
  replace one character with a longer string. Literal BMP character mappings and
  deletion work.
- **Numeric parsing:** The Hail oracle accepts Arabic decimal digits in integer
  parsing, and JVM hexadecimal literals, numeric suffixes and signed NaNs in
  float parsing. Generic DataFusion casts do not provide that complete grammar.
  The public `parse_*` APIs explicitly reject rather than silently return a
  different result for those inputs.
- **Formatting/serialization and statistics:** The pinned functions do not
  provide Hail-compatible Java printf, typed JSON serialization/parsing, hamming
  distance or string entropy. No replacements were hand-written. Non-string
  `delimit` is rejected because it needs Hail-specific formatting.
- **Case locale and Unicode version:** The tested default-locale examples match
  Hail, including final sigma and dotted I. DataFusion uses Rust Unicode tables;
  Java case conversion also depends on JVM Unicode tables and default locale.
  Exhaustive parity across locale/version combinations is not established.

## Evidence

The pinned Hail runtime produced
[these observations](research/string-operations-hail-oracle.json), captured by
[scripts/check_hail_string_operations.py](../scripts/check_hail_string_operations.py).
It confirms UTF-16 length/positions, supplementary reversal differences, Java
number syntax, missing-string zero repetition, joined missing members, and
empty/trailing split results. The script imports Hail only for oracle testing;
Vail's implementation does not import Hail.

Python tests exercise these cases plus row/global expressions, lookup results,
aggregation, independent partitions and Vortex write/read round trips. Native
tests assert built-in DataFusion kernels and reject invalid IR during planning.

DataFusion functions are checked against the pinned 55.0.0 registry sources and
[official scalar function documentation](https://datafusion.apache.org/user-guide/sql/scalar_functions.html).
