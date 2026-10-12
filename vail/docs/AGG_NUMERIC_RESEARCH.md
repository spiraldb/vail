# Numeric aggregator semantics and implementation research

This note records the public Hail contract used for Vail's advanced numeric
aggregators. Hail's API reference is the semantic source of truth:
[Hail aggregators](https://www.hail.is/docs/0.2/aggregators.html).

## Contracts

| Aggregator | Hail contract | Empty / missing observations |
|---|---|---|
| `hist(x, start, end, bins)` | Struct with `bin_edges`, `bin_freq`, `n_smaller`, `n_larger`; intervals are left-inclusive and right-exclusive, except that the final bin includes `end`. | Missing values are skipped. Hail's extra-table requirements also specify ignoring NaN and placing either signed zero in the zero bin. |
| `corr(x, y)` | Pearson correlation using paired observations. | Pairwise missing values are skipped; no defined pairs returns missing. |
| `linreg(y, x, nested_dim=1, weight=None)` | Ten-field regression summary: coefficient vectors, residual/error measures, model comparison statistics, and `n`. | Rows require all predictors, response, and optional weight defined. Hail documents statistic fields as missing when `n <= number of covariates` or covariates are linearly dependent. |
| `approx_quantiles(x, qs, k=100)` | One quantile for scalar `qs`, or a vector for a quantile list; quantiles range from 0 to 1. | Hail describes results as approximate and nondeterministic; `k` trades memory for accuracy. |
| `approx_median(x, k=100)` | Equivalent to `approx_quantiles(x, .5, k)`. | Same behavior as approximate quantiles. |
| `approx_cdf(x, k=100, _raw=False)` | Compact distribution summary; `_raw` selects the lower-level representation. | The implementation must retain a bounded sketch rather than collect all values. |
| `downsample(x, y, label=None, n_divisions=500)` | Representative coordinate points partitioned by a dynamic two-dimensional grid, retaining optional labels. | Empty input returns an empty array. |

The Hail API reference documents the detailed signatures and results, including
the histogram boundaries, regression fields, and pairwise correlation rule.
The repository's `extra-table-requirements-expr.json` adds explicit regression
empty behavior (`n=0`, other fields missing), histogram NaN/negative-zero edge
cases, and empty downsample behavior.

## Bounded numerical methods

Correlation should use DataFusion's registered `corr` aggregate where the
physical expression system can express pairwise-valid inputs. Otherwise an
online co-moment accumulator is sufficient: retain count, means, and centered
cross/second moments, and merge partial states with the standard parallel
variance formulas. This state is constant-size and avoids catastrophic
cancellation from subtracting large raw sums.

Linear regression needs a stable solver and probability distributions for the
reported t and F statistics. Prefer `nalgebra` for the normal-equation/QR solve
and `statrs` for Student-t and F tails; do not compute these by collecting
observations or by hand-rolling matrix algebra. Rank deficiency must be
detected, since Hail returns missing inferential fields for dependent columns.

For approximate quantiles, `kll-rs` 0.1.4 provides an Apache DataSketches KLL
implementation for `f64`. Its documented API supports update, merge, rank and
quantile queries, and binary serialization, so it can be used for bounded
DataFusion state across partitions:
[docs.rs `kll-rs`](https://docs.rs/kll-rs/0.1.4/kll_rs/). This produces valid
approximate estimates but does not establish bitwise compatibility with Hail's
randomized sketch. Tests should check Hail's result shape and rank-error
expectations, not equality of randomized estimates. The higher-level `sketches`
crate (0.1.2) also exposes KLL but its public sketch API does not provide merge
state serialization; `kll-rs` is therefore the better fit here.

The KLL backend operates on `f64`; Vail rejects int64 observations outside
±2^53 rather than silently changing their order. Its serialized state cannot
produce Hail's internal compaction-level `_raw=True` result, so that mode
returns an explicit unsupported error. The public CDF's ranks count values
strictly below each displayed value, matching Hail's documented interval
representation.

`ndhistogram` 0.12.0 supplies the one-dimensional `Hist1D<Uniform<f64>, i64>`
storage and bin lookup for `hist`. Its uniform axis has underflow and overflow
bins. Hail includes `end` in the last regular bin while `Uniform` uses an
exclusive upper bound, so Vail maps that one boundary to the last regular bin.
The accumulator stores the library histogram and merges its bounded bin counts.

`hist`, `corr`, and streaming sketches have naturally bounded state. A
multivariate regression accumulator can also keep bounded state by retaining
the cross-product matrix and response cross-products (O(p^2) for p predictors),
then solving once in `evaluate`. `downsample` should use bounded per-division
representatives; accumulating the full point set would violate the large-input
contract.

## Merge and precision limits

The histogram merges fixed `bins + 2` library counters. Correlation merges
DataFusion's six online moment fields, including its unsigned count. KLL merges
serialized sketches. Regression merges its count, cross-product matrix,
response products, and a negative-weight flag; memory is O(p²). Downsample
merges at most one retained point per occupied grid cell, so memory is
O(`n_divisions²`). All five aggregators keep state independent of input row
count.

The KLL results are approximate and need not match Hail's randomized CDF
sample exactly. The current `kll-rs` wrapper has no Hail-compatible
compaction-level `_raw` output, so Vail rejects `_raw=True` at expression
construction. The public CDF schema contains `values` and `ranks` only; it
does not advertise unavailable compaction counts. To avoid an unannounced int64-to-f64 ordering
change, quantile and CDF inputs outside ±2^53 fail explicitly. Histogram
coordinates and integer correlation inputs do convert to f64, as Hail does for
its histogram bin formula and floating correlation expression. Regression
uses bounded normal equations with `nalgebra`; nearly dependent predictors may
have more numerical error than a QR solve, while singular systems produce
missing statistics. Downsample retains one point per cell and its choice can
vary with input and merge order; its result is representative, not an exact
sample of all points.

## Reference links

- [Hail aggregator API reference](https://www.hail.is/docs/0.2/aggregators.html)
- [Rust `kll-rs` crate 0.1.4](https://docs.rs/kll-rs/0.1.4/kll_rs/)
- [Rust `sketches` crate 0.1.2](https://docs.rs/sketches/0.1.2/sketches/)
- [Rust `nalgebra` documentation](https://docs.rs/nalgebra/latest/nalgebra/)
- [Rust `statrs` documentation](https://docs.rs/statrs/latest/statrs/)

Quantile probabilities may be expressions over globals, including missing values
and arrays with missing members. The adapter selects zero-based rank
`floor(q * n)`, matching Hail `_quantile_from_cdf`, rather than DataSketches'
inclusive `ceil(q * n) - 1` convention. For four exact retained values
`[0, 1, 2, 3]`, Hail's median is 2.

Hail's public `k` is not constrained to the DataSketches minimum of eight.
`ApproxCDFStateManager` applies an eight-item minimum to each level; Vail maps
requested `k < 8` to DataSketches `k=8` rather than rejecting it. This aligns
input acceptance and the minimum retained capacity, but does not make the
two KLL implementations' compaction schedules identical. The selected Rust
library is limited to `u16` k and cannot represent Hail's full int32 domain.
