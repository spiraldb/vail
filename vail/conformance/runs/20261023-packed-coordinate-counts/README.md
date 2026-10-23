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

## Results

Baseline main5b2c vs candidate e6faf320 (local CSE enabled in both), alternating3independent processes each, exact fixture/query,32execution threads. Median wall2.6566→1.7279s (−35.0%), CPU29.5643→12.9376core-s (−56.2%), peakRSS11.043→7.550GB (−31.6%). Baseline wallrange2.588–2.738s; candidate1.274–1.829s, so wall variability is material. CPUranges29.555–29.782 and12.776–13.102. See pairs.json and summary.json. Ambient laptop activity uncontrolled; observed ranges are not confidence intervals.

Fresh paired stock PyPI Hail0.2.139/Java11,32cores24GBheap, same cached exact data, serial fresh workers,3runs/engine: Vail median1.4727s (1.4569–1.4911),13.0647core-s (13.0444–13.4557); Hail5.7504s (5.7179–5.9942),35.1039core-s (35.0848–36.1589). Hail/Vail3.9046wall,2.6869CPU. Independent medians. Harness samples process-tree CPU at10Hz retaining exited children. This singlecase is not a full-suite result. See fresh-paired/aggregate-report.html and case-medians.csv. Its Vail fixture resolves to the same /tmp/entries-profile25.mt.

Separate200Hz profiles: before32.4896/after15.1049sampled core-s including startup/planning, unlike query-only receipts. Boolean numericcast self4.4910→0, BoolVec construction3.1826→0, BooleanArray valueaccess4.2802→0sampled core-s. Packed-count inclusive0.4993core-s after (3.3%), self0.2952. Listlength inclusive remains5.5127core-s. Inclusive categories overlap; zero samples do not prove an operation can never occur. These removed paths are absent in the exact workload profile. profile_costs.py reproduces categories; original and symbolized profiles retained.

Validation:140Python tests passed corrected installed artifact;3public packed-count native tests pass, including logical/physical null differences, sliced masks, partial state and overflow. Existing coordinate reducer tests retained too. Normal kache builds, exact nativeSHA receipts saved. Production source review cleared logical-null correction ba2fd722; e6faf320 changes only timing probe. Integration receipt follows separately.

