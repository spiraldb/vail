# Native Python / Rust boundary

Python and Rust exchange Arrow data using the Arrow C Stream protocol. Inputs
provide `__arrow_c_stream__`; Rust takes ownership of the stream's release
callback and retains Arrow buffers. Results expose independent C Stream exports
over shared Rust record batches. Neither direction builds an IPC byte stream.
Python row inputs still require construction of typed Arrow arrays, and combining
multiple input batches can require concatenation. These are explicit data
materialization steps, not serialization at the language boundary.

Expressions are immutable `NativeExpr` PyO3 objects containing shared Rust
`Arc<Expr>` nodes. Python expression methods construct Rust enum variants directly;
transient argument mappings contain native child handles, not encoded subtrees.
Inspection returns shallow views with native child handles. Python retains dataset
owners needed by captured globals and lookups. Reusing an expression as two child
arguments shares its Rust node rather than copying the expression tree.

Hail-compatible Python type classes wrap `NativeType` objects. Their fields,
children, and Arrow representation come from the Rust type definition. Schemas
cross as native type handles. Scalar literals use native scalars or Arrow buffers;
interval filters and calculated partition bounds use Arrow too. Configuration
arguments cross as ordinary Python values extracted by PyO3, without a text codec.

Serialized formats remain at actual storage boundaries: Vortex files, globals
Arrow IPC files, persisted matrix descriptors, reference-genome files, CSV/JSON/VCF import and export, and
Arrow schema metadata used to persist Vail semantic types. These metadata tags
also accompany schemas in the Arrow C Stream protocol for external Arrow
interoperability; Rust owns the encoder and decoder. VCF header descriptions
also require escaping as part of the VCF format. Human-readable plan strings are
diagnostics. None is an expression or data transport codec between Python and Rust.

`python/tests/test_native_memory_boundary.py` checks shared buffer addresses in
both directions, repeated stream export and lifetime after garbage collection,
shared expression node identity, immutable nodes, native type wrappers,
a 64-level shared expression graph, and execution with Python JSON and Arrow IPC
encoding and decoding disabled.
