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
SHA-256 using a bounded buffer. After decoding, it checks the total row count.
The descriptor's optional ordered-run certificate is always absent for this
slice; readers reject a supplied certificate rather than trust an unverified
proof. Required semantic metadata must be present. Structural equivalence
between caller-supplied Hail JSON types and the Iceberg schema is a later gate.

This implementation supports an initial, unpartitioned local snapshot with
zero or one Vortex data file. Empty tables commit an empty Iceberg snapshot and
retain globals. The table must still be empty at commit time after any catalog
conflict retry, so another writer cannot invalidate the descriptor. Reads
reject delete files, non-Vortex files, split scan tasks, multi-file descriptors,
order certificates, and unsupported local URI encoding. The PR writer streams
batches through a bounded channel, and its reader streams decoded batches;
Vail's integrity check makes a separate buffered pass over the local file.
Failed commits can leave unreferenced immutable data and descriptor files;
cleanup must only remove files unreachable from retained snapshots.

Reads never sort the table. For this one-file slice, pinned Vortex 0.86.1's
default scan preserves physical row sequence, and Vail limits Iceberg data-file
read concurrency to one. The API does not claim general table key order:
Iceberg manifest planning gives no global row-order guarantee, and the
descriptor carries no order certificate. Multi-file keyed reads will require
a snapshot-bound, typed composite-key interval index and fragment planning in
key-interval order. A missing or unknown index must fail until import or repair;
it must not trigger an automatic full-table read sort. Append, overwrite,
compaction, external writer coordination, nonlocal object stores, and validated
order certificates remain future work.

`tests/roundtrip.rs` exercises fresh metadata reopening, a direct PR Iceberg
reader scan across multiple batches/splits, an empty snapshot, and DataFusion
ordered collection followed by Vortex/Iceberg storage. The local example
uses separate write and read processes.
