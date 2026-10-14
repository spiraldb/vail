# String operations and the UTF-8 contract

Vail deliberately diverges from Hail's UTF-16 string semantics. Strings contain
valid UTF-8 and operations count Unicode scalar values (codepoints). They never
index UTF-8 bytes or expose surrogate halves. For example, `hl.len('a😀b')` is 3,
`hl.literal('a😀b')[1]` is `'😀'`, and reversing it produces `'b😀a'`.

This applies to literals, row/global expressions, lookups, collection callbacks,
aggregations, and persisted values. No literal-only/BMP restriction applies.
Combining marks, variation selectors and zero-width joiners each count as their
own codepoint. Operations do not normalize Unicode or segment grapheme clusters.
Lone UTF-16 surrogates are not valid Unicode scalar values or UTF-8 strings.
String comparison and ordering use Unicode scalar order (equivalently valid
UTF-8 lexicographic order), which can differ from Java UTF-16 ordering.

## Supported text operations

| API | Semantics / execution |
| --- | --- |
| `length`, `hl.len` | Codepoint count using DataFusion `character_length` |
| `s[i]` | Dynamic positive/negative codepoint index; missing inputs propagate; out-of-range indices raise at evaluation |
| `s[start:stop:step]` | Dynamic codepoint bounds, clamped as array slices; positive/negative steps; a zero step raises |
| `find(sub, start, end)` | Codepoint offsets within normalized bounds; -1 if absent |
| `reverse`, `reverse_complement` | Reverse whole codepoints, including supplementary characters; complement only mapped bases |
| `lower`, `upper`, `strip` | DataFusion Unicode case conversion; strip retains the U+0000..U+0020 trim set |
| `+`, `*`, contains/prefix/suffix | Dynamic strings; missing-string repetition by non-positive counts gives the empty string |
| `matches`, `first_match_in` | Dynamic regex patterns; partial/full matching and capture groups; no match gives missing capture array |
| `replace` | Dynamic regex and replacement; all matches replaced; DataFusion replacement templates |
| `split` | Dynamic regex delimiter and limit; positive limits retain the unsplit remainder, negative limits preserve trailing empties, zero removes trailing empties |
| `translate` | Codepoint keys, supplementary characters, deletion, arbitrary replacement strings, dynamic dictionaries |
| `join`, `delimit` | String arrays/sets joined natively; delimit converts non-string elements using Vail's string conversion |
| `hamming` | Count unequal codepoints; unequal lengths or missing inputs give missing |
| `entropy` | Shannon entropy of codepoint frequencies in bits; empty string gives zero |

Python constructs lazy IR; execution stays in Rust/DataFusion. Built-ins handle
substring, length, reversal, search, matching, replacement and character
translation. Native scalar kernels supply regex splitting/capture extraction,
hamming distance and entropy. Dynamic dictionary translation and stepped slices
compose existing array lambdas, dictionary lookup and join operations.

## Regex and remaining compatibility limits

Regex operations use DataFusion/Rust regex syntax and Unicode behavior. Patterns
are no longer restricted to plain literals. Java-only lookaround/backreferences
inside patterns are not supported by that engine; invalid patterns fail during
execution. Replacement templates follow DataFusion's syntax (`$1`, `${name}`,
and its supported POSIX-style backreferences). This is not full Java regex parity.

Java-specific numeric parsing (`parse_int*`, `parse_float*`), Java printf
(`hl.format`), and Hail typed JSON serialization/parsing remain separate,
explicitly unsupported APIs. The UTF-8 decision does not define those grammars.
Unicode casing follows the Unicode tables supplied by Rust/DataFusion and can
differ from Java across locale/version combinations.

## Compatibility evidence

The historical [Hail oracle observations](research/string-operations-hail-oracle.json)
and [capture script](../scripts/check_hail_string_operations.py) record Hail's
UTF-16 behavior. They are evidence of the intentional divergence, not Vail's
expected outputs. Tests in `test_string_operations.py` and
`test_string_codepoints.py` assert Vail's codepoint contract, dynamic execution,
bounds/missingness, regex operations, and storage round trips.
