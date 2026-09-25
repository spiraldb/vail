# Vail storage slice

`vail-storage` streams Arrow batches through the Vortex writer in Apache
iceberg-rust PR 2835 and commits its resulting `DataFile` in a real Iceberg
catalog transaction and Avro manifest. Reads use that PR's Iceberg
`TableScan`/`ArrowReader`, including its field-ID schema handling. The
materialized PR head is `0252fe9174aa3062da51328c444a765aa7de2dae`;
Vortex is pinned to `=0.86.1` in Cargo. `VORTEX` is still a nonstandard
Iceberg data-file format, so unmodified Iceberg engines cannot read it.

The snapshot summary points to an immutable descriptor and its SHA-256 hash.
The descriptor carries required Hail-level row/global types, globals, key field
IDs, comparator version, reference genomes, table UUID, snapshot/schema IDs,
and a hash of the live-file list. Before calling the PR reader, Vail compares
that list with actual Iceberg scan tasks and checks each local file's size and
SHA-256 using a bounded buffer. After decoding each file, it checks that file's row count.
The descriptor's optional ordered-run certificate is always absent for this
slice; readers reject a supplied certificate rather than trust an unverified
proof. Required semantic metadata must be present. Structural equivalence
between caller-supplied Hail JSON types and the Iceberg schema is a later gate.

The low-level writer creates an initial unpartitioned snapshot with one or more
Vortex files. Keyed files are certified against a typed primitive-key interval
index bound to the snapshot, schema, comparator, and live file hashes. Empty tables
commit an empty snapshot and retain their semantic metadata. Readers reject delete
files, non-Vortex files, split scan tasks, unsupported path encoding, and missing or
invalid ordering certificates.

`stream_snapshot` plans real Iceberg manifest tasks, checks their binding to the
index, and produces an ordered batch stream. It checks each requested file's size
and checksum before decoding it and validates its decoded row count. Dropping the
stream leaves subsequent files unopened. Reads never sort rows. Manifest planning
currently enumerates the live file set before interval selection.

`local_table::open_local` resolves only the directory entrypoint, immutable Iceberg
metadata, and Vail descriptor. Its `LocalRead` pins the snapshot and emits logical
partition boundaries plus batches when polled. It stores no materialized rows.
`read_local` remains an eager convenience wrapper for callers that request batches.
Python's Rust TableIR uses the lazy source instead.

The directory coordinator publishes a completed generation via atomic replacement
of `_vail_table.json`, with a local writer lock. Overwrite retains old generations so
existing read plans continue to work. It has no garbage collector or crash recovery
for stale locks. General appends, compaction, external writer coordination and
nonlocal object stores remain future work. The local entrypoint also retains logical
partition sizes. Unkeyed snapshots record an explicit file sequence in the hashed
descriptor; their manifest enumeration never determines row order.

`write_local_stream` consumes partition/batch events with backpressure. It flushes
one staging buffer at a 64 MiB retained-Arrow-memory target or 1,048,576 rows and at
partition boundaries, then waits for the PR writer to close that file before pulling
more data. The byte target is soft for a single oversized batch (flushed alone);
the row cap splits large batches. Only file metadata remains after a flush. Rust
callers may supply `WriteOptions` to adjust both thresholds. The previous Vec-based
helpers remain adapters for callers that already own their input batches.

`tests/roundtrip.rs` exercises fresh metadata reopening, a direct PR Iceberg
reader scan across multiple batches/splits, an empty snapshot, and DataFusion
ordered collection followed by Vortex/Iceberg storage. The local example
uses separate write and read processes.

Globals written through `write_local_stream_with_globals` are a single Arrow IPC
batch in the immutable generation. Their URI, SHA-256, and logical field types
are bound into the Vail snapshot descriptor. Empty globals require no sidecar.
Table construction reads only their type; the DataFusion GlobalsScanExec verifies
and reads the singleton when an action needs it. Globals exist even when the row
snapshot contains no files.
