# Genomic aggregate implementation research

## Reuse review

Before implementing the kernels, I searched crates.io and Rust package documentation for general Hardy–Weinberg, call-statistics, and inbreeding routines. The only directly relevant Rust registry result was [`rsomics-vcf-hardy`](https://docs.rs/crate/rsomics-vcf-hardy/latest), which implements per-site VCF/vcftools-compatible testing. Its VCF-oriented input and specific vcftools output contract do not cover Hail's HWE result (including its expected heterozygote frequency, ploidy filtering, and Levene–Haldane mid-p behavior). No suitable general Hail-compatible call-statistics or inbreeding crate surfaced, so those small streaming reductions need local kernels.

The established [`ndarray`](https://crates.io/crates/ndarray) crate provides N-dimensional arrays and views. Vail's Arrow wire form already carries contiguous row-major data and shape. The implementation checks shape equality and uses Arrow 59.2's `arrow_arith::numeric::add_wrapping` for typed elementwise addition on the flattened data. It fills missing elements with zero before calling that kernel, matching Hail's aggregate behavior. This reuses the array library's arithmetic without converting every row into another ndarray representation.

## Hail behavior consulted

The implementation contract follows Hail's upstream [`aggregators.py`](https://github.com/hail-is/hail/blob/master/hail/python/hail/expr/aggregators/aggregators.py):

- `hardy_weinberg_test(call)` returns `het_freq_hwe` and an exact two-sided p-value with mid-p correction. Only diploid calls contribute to genotype counts; haploid, higher-ploidy, and missing calls are ignored. The scalar `hl.hardy_weinberg_test` is a separate API with one-sided and two-sided modes.
- `inbreeding(call, prior)` ignores rows with missing call or prior, adds expected homozygotes `1 - 2p(1-p)`, counts observed homozygous calls and nonmissing calls, and computes `f_stat = (observed_homs - expected_homs) / (n_called - expected_homs)`. Its output fields are `f_stat: float64`, `n_called: int64`, `expected_homs: float64`, and `observed_homs: int64`.
- `call_stats(call, alleles)` counts each called allele (including haploid calls), sets `AN` to the number of called allele copies, computes `AF = AC / AN` (the entire `AF` field is missing when `AN` is zero), and counts homozygotes only for diploid calls. Hail's output `AC` is `array<int32>`, `AF` is `array<float64>`, `AN` is int32, and `homozygote_count` is `array<int32>`.
- `info_score(gp)` expects a defined three-element genotype-probability array. It accumulates the IMPUTE information measure; missing inputs are excluded, and a monomorphic estimate has score 1. Its `n_included` output is int32.
- `ndarray_sum` combines same-shaped values elementwise, preserving ndarray rank and element type. It is missing when no defined value is observed and fails on shape mismatch.

For implementation, Call uses Arrow struct fields `__vail_call_alleles: List<Int32>` and `__vail_call_phased: Boolean`. ndarray uses `__vail_ndarray_data: List<T>` and `__vail_ndarray_shape: List<Int64>`; data is flattened in row-major order.
