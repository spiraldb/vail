# Scalar row captures versus flat batch expansion

Source `f514c283`; native SHA256 `e22af44b162ae002fcd8ed104df4c334c2745390baf302908c051ff656da33cd`. Normal Rust 1.98 release, kache, isolated target. Both modes use the same artifact and normal runtime (32 workers, DataFusion concurrency 32). Three repetitions per mode/shape, separate processes, engines/modes serial, alternating order. No cloud or Hail baseline used. Laptop activity was not controlled; these are local comparative measurements, not final paired Hail/Vail results.

| Rows × columns; partitions | Scalar median wall / core-s | Batch median wall / core-s | Scalar / batch median peak GiB |
|---|---:|---:|---:|
| 10,000 × 500,000; 2,500 | 4.3769 / 39.6050 | 5.5939 / 56.2724 | 7.48 / 6.03 |
| 250,000 × 1,000; 250 | 0.2769 / 3.2281 | 0.2175 / 2.5183 | 0.397 / 0.459 |
| 25,000 × 10,000; 250 | 0.1523 / 1.4205 | 0.1974 / 2.0436 | 0.465 / 0.500 |

Expansion wins for the 1,000-column representative. Scalar context wins on the exact wide case and the 10,000-column representative. No automatic dispatch threshold is inferred from three shapes.

The experiment reuses the existing flat evaluator. It constructs row RunArray metadata with `RunArray::<Int32Type>::try_new` and decodes it with `arrow_cast::cast`. Shared column arrays are repeated with native Arrow `concat` over borrowed contiguous slices; primitive children copy through memcpy, replacing the prior indexed take. The prepared native body then evaluates once per input RecordBatch. This eliminates per-row produced-array concatenation but allocates expanded metadata.

The exact logical matrix has 5 billion entries, but telemetry records **two complete map executions**, totaling 10 billion physical entry evaluations in 5,000 batches. The logical plan contains two coordinate-SUM branches. This existing duplication is unchanged in both experimental modes. Actual batch metadata expansion: 40,001,520,000 bytes of repeated column arrays plus 40,000,480,000 decoded row bytes, including array headers. Compressed row metadata totals only 1,040,000 bytes. The scalar path avoids that expansion but its two map executions concatenate approximately 40 GB of produced Int32 values. These counts describe output writes, not memory-bus reads plus writes.

Fresh exact 1 kHz profiles: scalar 4.002 wall / 34.887 core-s; batch 5.714 wall / 51.542 core-s. Scalar leaves: SUM in-place addition 36.22%, widening 19.39%, scalar-plus-array 8.53%, produced-value concat 11.845% (1,352/11,414 samples). Batch: SUM 25.54%, widening 12.41%, array-plus-array 13.89%, REE decode 14.90%, decode take values 9.42%, column concat 7.523% (1,349/17,932 samples). Thus the batch mode removes output concat while introducing metadata decode/copy work. Neither profile has bulk CrossJoin metadata replication.

Validation: 220 public Python tests pass in default mode; 23 aligned-map/entry-sum tests pass with batch mode forced. A public Expr lowering/evaluation native regression passes for nullable Utf8View/LargeUtf8 values inside nested structs. Struct child normalization adapters are prepared during lowering; runtime reuses prepared field descriptors.

The native Arrow expression APIs generally allocate result arrays. `binary_mut` can reuse uniquely owned primitive buffers for SUM state, but arbitrary PhysicalExpr evaluation does not accept caller-owned destination buffers. These measurements do not claim reusable destination buffers for every intermediate expression.
