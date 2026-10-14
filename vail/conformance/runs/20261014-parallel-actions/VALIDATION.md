# Parallel count, aggregation, and write validation

Source revision: `a3e497c7732f13f630e7cc9e3937c89b63fe3a31`.

- Complete Vail Python suite: **1,059 passed, 1 skipped, 19 subtests passed** (236.87 seconds).
- Rust Table library and integration suites: **66 passed**.
- Rust storage library and integration suites: **48 passed**.
- Focused Python count/statistics/storage/join suites: **71 passed**.

Counts drain up to 32 independent execution partitions concurrently. Unordered numeric statistics lower to DataFusion partial/final aggregation. Writes execute partition streams and Vortex file writers concurrently with bounded buffers, then restore partition/file metadata order before publishing one snapshot.

Regression coverage includes a blocked first partition during count, reordered partition-file writes, pruning reads after unrelated files are removed, error propagation, and no snapshot publication after a failed write. Logical partition/file ordinals are retained in physical filenames.

The fresh 32-vCPU cloud benchmark is separate from this local correctness validation. Previous benchmark results must not be attributed to this revision.
