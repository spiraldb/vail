# Importing Hail native storage

```python
import vail as hl

table = hl.Table.read_legacy('/data/old.ht')
matrix = hl.MatrixTable.read_legacy('/data/old.mt')

table.write('/data/table-vail')
matrix.write('/data/matrix-vail')
```

The legacy readers import Hail **0.2** native directory formats **1.0.0 through
1.8.0**, including files whose older extension was `.t` or `.hmt`. They require
no Hail installation, Spark, or JVM. Paths may be local filesystem paths,
`pathlib.Path` values, or local `file://` URIs. The ordinary `read_table` and
`read_matrix_table` functions continue to read Vail storage.

These are eager migration readers. They read every part in metadata order and
materialize the complete dataset, with intermediate Python objects, in memory.
They do not offer indexed interval reads, cloud filesystem access, or partition
selection. Use a machine with enough memory for the imported dataset and save
it in Vail storage for subsequent reads. Original storage partitioning is not
retained.

Supported wire layouts:

- `UnpartitionedRVDSpec`, `OrderedRVDSpec`, and `IndexedRVDSpec` with packed codecs;
  `OrderedRVDSpec2` and `IndexedRVDSpec2` with typed codecs.
- Uncompressed, LZ4 (legacy, HC, fast, and size-based), and Zstandard (ordinary
  and size-based) block buffers, both stream framing versions, and optional
  LEB128 integer buffers.
- Required and optional primitives, nested structs, arrays, sets, dictionaries,
  tuples, loci, intervals, calls, and column-major ndarrays. Arrays and binary
  strings support both legacy integer lengths and newer varint lengths;
  `EVarint` supports both integer widths.
- Dataset globals, row and column keys, duplicate column occurrences, original
  matrix column order, empty axes, and missing entry structs (filtered entries).
  A present entry struct containing missing fields remains present.
- Stored custom reference genome definitions, including compressed JSON files.

The importer checks stored partition counts and matrix row/entry partition
alignment. Unsupported encodings (including experimental `EStructOfArrays`),
unsupported versions, malformed markers, truncated data, and schema mismatches
raise errors. Hail 0.1 KeyTable/VariantDataset storage is unsupported. Vail's
existing type restrictions still apply; for example a matrix global named
`__vail_matrix_columns` conflicts with Vail's internal column metadata and is
rejected explicitly.

## Format evidence and validation

The implementation follows the pinned Hail checkout, particularly:

- `hail/hail/hail/src/is/hail/compatibility/LegacyEncodedTypeParser.scala`
- `hail/hail/hail/src/is/hail/compatibility/LegacyRVDSpecs.scala`
- `hail/hail/hail/src/is/hail/types/encoded/{EType,EBaseStruct,EArray,EBinary,EVarint,ENDArrayColumnMajor}.scala`
- `hail/hail/hail/src/is/hail/io/{BufferSpecs,InputBuffers}.scala`
- `hail/hail/hail/src/is/hail/rvd/AbstractRVDSpec.scala`
- `hail/hail/hail/src/is/hail/variant/Call.scala`
- `hail/hail/python/test/hail/helpers.py:create_all_values`
- `hail/hail/python/test/hail/matrixtable/test_file_formats.py`

`python/tests/test_legacy.py` checks decoded values in all 56 table and 60
matrix fixtures in Hail's `backward_compatability` corpus. These fixtures span
formats 1.0 through 1.8 and were written by Hail using its supported codecs.
Tests also cover documentation data, custom references, filtered entry slots,
empty matrix axes, newer stream framing/varint lengths, and corrupt inputs.
After initializing the pinned Hail submodule and building Vail, run from `vail/`:

```sh
python -m pytest python/tests/test_legacy.py -q
```
