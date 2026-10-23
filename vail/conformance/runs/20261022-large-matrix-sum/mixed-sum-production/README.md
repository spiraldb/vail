# Clean production mixed-width SUM milestone

Source `b032dd70` on branch `dk/dense-sum-production`, native SHA256 `ad710e2d7f7433aae8cfafde59cf0440d4d5302a6bf32061f60d193000c5adfe`, normal Rust 1.98 release. The experimental `f514c283` dispatch/column-concat source is **excluded**. The neighboring experimental report retains its original source and artifact provenance and is not evidence for this production artifact. The attempted frontend native-coalesce change was reverted; the lazy matrix SUM guard remains unchanged pending the separate generic shared-singleton work.

Dense SUM reads Int32 directly into Int64 state and Float32 directly into Float64 state through Arrow `binary_mut`. The frontend no longer materializes a widened map before the adapter. Int32 map arithmetic still wraps before accumulation. Boolean input uses Arrow's native UInt8 cast because Boolean arrays are bit-packed; this creates a byte-per-entry buffer, not an Int64 buffer. Null/filter handling and typed partial-state representation are retained.

Aligned capture types are prepared during lowering. Raw row/global capture storage is normalized once per input batch, including nested field nullability and Utf8View/LargeUtf8 physical storage. Public native regressions evaluate these cases through Expr lowering and the aligned-map boundary.

224 public Python tests and eight public native tests pass. Tests include Float32 widening, Int32 map wrapping, filtered/missing/empty axes, nested capture descriptors, globals, RNG and folds.

The exact upstream range-matrix query returned 500,000 columns. Its fresh 1 kHz profile measured 3.269 seconds wall and 20.812 core-seconds. These are uncontrolled laptop diagnostic timings, not paired Hail/Vail benchmark results. The existing lazy singleton guard still executes two aggregate branches, so the logical 5 billion entries produce 10 billion map entry evaluations. That duplication is being addressed separately.

The profile contains direct `binary_mut<Int64Type, Int32Type>` at 34.79%, native Int64 state merges at 8.03%, Int32 map arithmetic at 13.64%, and memmove at 22.50%. The widened-array cast hotspot is absent. Peak RSS was 9.96 GiB; no memory improvement is claimed.
