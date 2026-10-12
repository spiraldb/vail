# Python data transport

Python table inputs and results use Arrow IPC streams. `Table.parallelize` builds typed Arrow arrays from Python values and passes IPC bytes to Rust. Rust decodes the stream into a `RecordBatch` held by the lazy `TableIR::Parallelize` source; transformations do not execute during construction. Arrow Table and RecordBatch inputs use the same transport.

`Table.collect` and `Table.to_pandas` execute the DataFusion plan and encode ordered output batches as an Arrow IPC stream. Scalar evaluation and bounded text-import sampling also return Arrow IPC streams. `Table.from_pandas` builds Arrow columns and feeds the same lazy source; `to_pandas` uses PyArrow conversion.

This bridge preserves Arrow validity separately from floating-point NaNs. It does not use JSON to transport input rows, collected rows, or evaluated scalar results. Locus metadata and ndarray rank remain part of the physical Arrow schema.

The broader migration is incomplete: expression IR still uses JSON and currently embeds literal values. Interval-read options and calculated partition bounds also retain legacy JSON boundaries. These are remaining data-serialization violations, not approved exceptions. Schema descriptors and expression operators currently use JSON as control-plane representation; changing that representation requires a separate IR transport migration.
