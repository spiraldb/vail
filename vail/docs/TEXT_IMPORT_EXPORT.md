# Text import and export

`import_lines(paths)` is the foundation for Hail-shaped text imports. Its lazy
`TextRead` TableIR lowers to DataFusion `DataSourceExec` with a `LineSource`
implementing DataFusion's `FileSource` and `FileOpener` interfaces. DataFusion
projections attach the file name and Vail's internal partition identity.
Each input file is an independently executable partition. There is no sorting
on read and no Python row reader. Gzip and concatenated BGZF members stream
through native decompression; `.gz` input requires `force=True`, while `.bgz`
is recognized automatically. `force_bgz=True` also accepts other suffixes.

The source streams ObjectStore bytes through a tokio-util codec. `memchr` locates
LF, CRLF, and CR boundaries, matching Hail's line endings. Blank lines, quotes,
NULs, UTF-8, and unterminated final lines are preserved; invalid UTF-8 errors.
DataFusion has no built-in raw-line file source, so this small source extension
is necessary. CSV is not a substitute: it parses quotes and skips blank lines.

`import_table` samples one filtered row to discover its header, as Hail does.
This metadata action uses DataFusion `GlobalLimitExec`, without first counting
unknown input partitions. Parsing that header also executes in DataFusion.
The returned table remains lazy. It builds DataFusion filters and projections
over `import_lines`, with an expression IR node for single-line delimited
parsing. That node lowers to a DataFusion scalar UDF backed by the Rust `csv`
library. A compiled regular expression validates Hail's stricter quote grammar:
quotes enclose a whole field and there are no doubled-quote escapes. The library
parses fields; the adapter checks field counts and applies missing markers.
Malformed records error rather than becoming missing rows or short arrays.
Configured UDF identities include their delimiter, quote, missing markers, and
expected field count.

Supported import options include typed `types`, `impute`, `key`, `no_header`,
ASCII or regular-expression delimiters, ASCII quotes, missing-marker lists,
`source_file_field`, comment/filter patterns, blank-line skipping, and
`find_replace`. Type imputation uses native aggregate expressions over the
filtered input. `import_csv` supplies Hail's comma delimiter and quote defaults;
records still occupy one physical line.
Repeated header lines are removed by text equality, including lines in later
files, matching Hail's current implementation. Duplicate header names receive
Hail's suffixes. `StringExpression._split_line` exposes the same parsing IR;
ordinary `.split` remains a DataFusion `string_to_array` expression for literal
nonempty delimiters.

`read_csv` uses DataFusion's built-in CSV source, including standard CSV quoted
multiline values and doubled quotes. `read_json` uses its built-in JSON source
for newline-delimited JSON objects. Both accept an explicit `tstruct` schema;
otherwise Arrow's schema-inference libraries sample records as metadata work.
Inference does not execute the resulting table. Explicit schemas are preferable
when missing values or later records make sampled types ambiguous.

`Table.export`, `export_csv`, and `export_json` are actions. They lower the table
to a DataFusion plan and stream its ordered batches into DataFusion sinks.
`export` writes unquoted delimited fields, with a header by default and `NA` for
missing values. Float64 values use Hail's four-decimal scientific notation;
nested annotations use typed JSON. `parse_json` and `json` preserve logical
types including Calls, loci, intervals, tuples, sets, dictionaries and ndarrays.
CSV and JSON variants use their respective format semantics. Internal partition
columns and globals are not exported. Text `.gz` and `.bgz` output uses bounded
native compression; BGZF output includes standard members and its EOF marker.

## Remaining support

- Remote paths and independently seekable byte-range splitting of one text file.
  `min_partitions` repartitions the scan output.
- General Java-only regular-expression constructs such as lookarounds and
  backreferences are outside the native regex engine's supported syntax.
- Hail-specific numeric parsing forms such as Unicode integer digits and Java
  hexadecimal/suffixed float literals are not implemented by Arrow casts.
- `types_file` and parallel text export shards.

Oracle observations are recorded in
[text-operations-hail-oracle.json](research/text-operations-hail-oracle.json).
Run `scripts/check_hail_text_operations.py` with the local Hail runtime to refresh
them. Regression tests live in `python/tests/test_text_import_export.py`.
