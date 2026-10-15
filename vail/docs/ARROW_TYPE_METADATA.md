# Arrow logical type metadata

Vail distinguishes logical types using explicit, versioned Arrow metadata. Physical field names alone never identify tuples, calls, intervals, loci, ndarrays, sets, or dictionaries. Untagged structs and lists are ordinary structs and arrays, including fields named `__vail_tuple_0` or list elements named `__vail_set_item`.

Version 1 uses two metadata entries:

| Key | Value |
| --- | --- |
| `vail.type.version` | `1` |
| `vail.type` | The existing complete Vail logical type descriptor, encoded as UTF-8 JSON. |

This is a portable Arrow schema metadata format, retained for external Arrow inputs and storage; native expression types cross as PyO3 handles. The descriptor is schema information; runtime row values cross the Python boundary as shared Arrow buffers through the C Stream protocol. Persisted globals still use Arrow IPC. It includes tuple members, collection element/key/value types, interval point types, ndarray rank, and locus reference definitions.

Arrow data types themselves have no metadata map. The tag is carried on the first physical child field of a special struct type, or the element field of a special list type. It describes that enclosing type, not the child. This lets Python and Rust read the same contract from a nested Arrow data type, including tuples inside arrays and sets inside tuples. The empty-tuple Boolean sentinel also carries its tuple tag. Other field metadata is retained when adding a tag.

The Rust decoder, also used by the Python API, rejects incomplete tags, unknown versions, malformed descriptors, and incompatible physical layouts. A supplied Python logical schema is authoritative and can describe an untagged physical tuple explicitly; physical shapes and column names must still match. Large-offset Arrow strings and lists are normalized through Arrow casts as before.

## Execution and export

Type information comes from typed expressions and TableIR schemas. DataFusion operations remain library operations. Operators that construct new list/struct fields receive a metadata-free physical view where necessary, then a Rust Arrow cast restores the declared result type. The restoration guards prevent constant folding from recreating untagged list scalars. Scalar extraction preserves original nested Arrow arrays instead of rebuilding list fields through generic scalar conversion.

DataFusion 55's cross-join operator reconstructs its left columns through scalars and drops list metadata. Metadata-bearing Cartesian joins use DataFusion's existing nested-loop join operator instead, which selects values using Arrow arrays. Ordinary cross joins retain the standard operator. This is a physical planning adaptation, not a separate join implementation.

Rust restores the declared schema before yielding partition batches and before exposing collect/sample Arrow C Streams. Storage readers restore types from the snapshot's logical schema through Arrow normalization; they do not infer logical types from physical sentinels. Comparison code uses declared types or versioned tags, so an ordinary struct resembling a call or interval receives ordinary struct semantics.

Regressions are in `python/tests/test_arrow_type_metadata.py`, alongside the existing collection, aggregation, globals, and storage tests. Untagged legacy Arrow sentinels require an explicit schema to recover their intended special type; silently guessing is deliberately removed.
