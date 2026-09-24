# Python frontend integration

Audit baseline: Hail `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

## Reuse the compatibility frontend

The preferred starting point is to adapt the pinned Hail Python Table, expression, type, and IR code and supply a native backend. Reimplementing every Python method directly over DataFusion would duplicate validation, expression scopes, field resolution, join construction, and type inference. Reuse does not eliminate the Rust implementation work: every reachable value, aggregation, and Table IR operation still needs matching semantics.

Evidence:

- [Table imports and expression infrastructure](https://github.com/hail-is/hail/blob/e10cc355bc8cd0de9c5f29d5182d31527898bae1/hail/python/hail/table.py#L1): uses Hail expressions, analyzers, typecheck decorators, IR, and helpers; it also imports `pyspark` unconditionally. A native package must make that dependency optional at actual Spark conversion boundaries.
- [Backend execution](https://github.com/hail-is/hail/blob/e10cc355bc8cd0de9c5f29d5182d31527898bae1/hail/python/hail/backend/backend.py#L222): `execute` renders/finalizes IR, calls `_rpc`, and decodes a Hail binary result. `value_type` and `table_type` use the same backend seam. Simply overriding `_rpc` would require the existing text IR and result wire codec; that is a design choice, not free integration.
- [Backend imports](https://github.com/hail-is/hail/blob/e10cc355bc8cd0de9c5f29d5182d31527898bae1/hail/python/hail/backend/backend.py#L1): shared backend types import MatrixTable and BlockMatrix as well. The packaging task must inspect the transitive import closure.
- [Context dispatch](https://github.com/hail-is/hail/blob/e10cc355bc8cd0de9c5f29d5182d31527898bae1/hail/python/hail/context.py#L191): initialization selects existing backend kinds; native backend registration requires deliberate integration.
- [Reference genomes](https://github.com/hail-is/hail/blob/e10cc355bc8cd0de9c5f29d5182d31527898bae1/hail/python/hail/genetics/reference_genome.py#L132): reference registration, sequence lookup, and liftover cross the backend boundary. Genomic types in Tables cannot be represented solely by Python wrappers.

## Proposed seam

1. Freeze and adapt the frontend sources and their license notices; maintain a small, reviewable patch series and an upstream API inventory comparison.
2. Make `NativeBackend` translate structured Hail Python IR into a versioned native logical IR through PyO3. Preserve `finalize_randomness`, expression identity/source rules, and error provenance. Evaluate the existing rendered IR route during the first spike, but avoid implementing a legacy wire protocol solely to discard it again.
3. Override action/type/result conversion paths consistently. Merely replacing `execute` is insufficient: type queries, references, persistence, filesystem operations, and registered functions need native implementations too.
4. Make the Rust logical IR, type system, semantic functions, and execution session independent of Python. Rust users construct the same plans through native builders; no Python interpreter or Hail service is required.
5. Initially expose `import vail as hl`. Track `import hail as hl` as an explicit compatibility distribution/namespace decision. Internal `import hail` statements mean a simple package-directory rename is insufficient. Avoid a global `sys.modules` alias that prevents Hail and Vail from coexisting in an oracle process.
6. Run Hail and Vail oracle programs in separate environments/processes. Serialize typed fixtures and outcomes rather than depending on coincident Python import state.

## Required spike

Load the adapted frontend without Java, Spark, or a service; construct a range Table, annotate a nullable expression, key and order it, and execute `take` through Rust. Exercise `table_type`, globals, a source-mismatch error, and reference metadata. Demonstrate that Rust produces the same result without importing Python. Record all imported packages and backend calls. Only then commit to the frontend extraction and bridge protocol.

Full signatures, validation behavior, return types, warnings/errors, defaults, and deprecated public methods remain compatibility work. Native embedding and full Hail import compatibility are distinct acceptance checks.
