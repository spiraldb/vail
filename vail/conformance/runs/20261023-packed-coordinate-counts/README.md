# Packed coordinate count experiment — 2026-10-23

Investigates `many_aggs_col_wise` on the exact upstream query (33 named aggregators, 2535 columns) using the existing `/tmp/entries-profile25.mt` fixture. Baseline is main 5b2c933c with local lambda CSE already enabled. The candidate changes boolean/count accumulation only; numeric aggregate paths are unchanged.

## Reproduction and retained experiments

- `experiment.py`: exact upstream action, query wall/self process CPU/RSS plus native SHA256. Run serially with EXECUTION_CONCURRENCY=32 in each normal installed environment. Source and fixture locations are explicit; the fixture is reused, not regenerated.
- `candidate-probe.rs`, `candidate-probe.log`: independent exploratory algorithm prototypes and scalar oracle, including rejected scalar/sparse/CSA/basic NEON/zero-skip NEON variants. Raw ns/input-bit timings are exploratory, not rigorous end-to-end comparisons.
- Stable production timing probe: `benchmarks/packed_coordinate_count.rs`, compiled by `kache rustc -O --edition=2024 benchmarks/packed_coordinate_count.rs -o /tmp/packed-coordinate-count`. Semantic regression checks are in the public coordinate aggregate tests, not private kernel assertions.

## Research and choice

Klarqvist, Muła, Lemire, *Efficient Computation of Positional Population Counts Using SIMD Instructions*: https://arxiv.org/abs/1911.02696 . Clausecker, Lemire, Schintke, *Faster Positional-Population Counts for AVX2, AVX-512, and ASIMD*: https://arxiv.org/abs/2412.16370 . These describe positional population count and carry-save-adder networks. This workload has arbitrary row strides rather than contiguous fixed-width words, so the implementation gathers offset-aware 64-bit tiles and intersects up to four packed masks in registers.

For widths >=32 with >=4 rows, the ARM implementation uses CSA15 plus NEON byte-lane counts in groups of at most255 rows and fixed64-byte scratch; fallback is portable CSA16 or sparse/direct decoding, with a horizontal count_ones special case for width1. No full boolean-to-byte or numeric mask arrays. Zero-word skipping was rejected for the SIMD path: it helped entirely empty masks but hurt sparse mixed masks in the retained probe. Numeric counters remain i64/u64 with prior wrapping behavior.

COUNT obtains logical_nulls once per batch, preserving null semantics for NullArray and dictionary values. Predicate/fraction validity and entry filters remain packed, including unaligned sliced bitmaps. Fraction denominators count active rows even when predicate values are null.

Actual workload results and profiles are pending; do not infer a measured end-to-end speedup from the microbenchmarks.
