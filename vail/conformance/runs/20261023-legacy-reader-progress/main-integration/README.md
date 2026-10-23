# Final main integration evidence

Reviewed source: `9cc00b7cdf1dfda5da6d4a506c19ee4884ae1700`.
Main base: `462b70820f02c70ca06fbbf4703a3148e818d005`.
Required reviewer: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Source/test review cleared exact source; final evidence acceptance requested.

Review fixes: logical-null masks for semantic validity, recursive batch-prepared
reader bindings outside row/entry loops, cached index validation and binary seeks,
reused ndarray state, removal of unused encoded-int experiment, and public
behavior tests replacing private implementation assertions.

Normal maturin release build with kache; artifact provenance is recorded separately.
Passing validation: 408 Python tests; 64 execution Rust; 38 table library,
12 planning and 5 expansion; 29 storage library, 11 public reader and one ndarray;
both real Hail corpus tests; six public integer sum tests.

100,000 rows x 10,000 columns, two warmups and three measurements:

| Query | Median wall seconds | Median core-seconds |
|---|---:|---:|
| Full matrix decode/count | 6.223 | 71.897 |
| GT projection decode/count | 4.203 | 48.395 |
| DP sum | 5.320 | 59.865 |
| Table count, 100 million rows | 1.313 | 15.222 |

Actual full-read four-second sample: leading active leaves are scalar probing,
mixed-batch decoding, Zstd/Huffman decompression, and memmove. BatchColumn::new
has only 19 leaf samples; binding is not a leading leaf. Source control flow
prepares recursive bindings before the row loop, and the sample corroborates
that preparation is no longer hot. Sampling does not establish zero calls.

No new paired Hail run was performed for this exact revision. Earlier paired
results showed Vail consuming more CPU than Hail; these results do not establish
that the CPU gap is closed. Raw profiles and logs are retained here.
