# Hail aggregator inventory

Audited against the checked-out `hail/expr/aggregators/__init__.py` and
`aggregators.py`. All 33 public exports are present; 32 have native execution
paths, and `approx_cdf` explicitly raises an unsupported-operation error.
The inventory test checks export names and keyword parameter
names against that source. This is API coverage, not a claim of complete semantic
parity with every Hail input. Five underscore-prefixed helpers are excluded.

| Public aggregators | Execution / libraries |
| --- | --- |
| count, count_where, sum, product, min, max, mean, stats, fraction, any, all | DataFusion AggregateExec with Hail-specific Arrow accumulator identities, missing-value rules and integer arithmetic |
| collect | DataFusion UDAF retaining ordered Arrow chunks |
| collect_as_set | DataFusion DistinctArrayAggAccumulator |
| take | Arrow lexsort and take kernels, bounded retained top-n |
| filter, explode | DataFusion FilterExec and UnnestExec scopes |
| group_by, counter | Nested DataFusion AggregateExec and null-safe joins; dictionary Arrow representation |
| array_agg, array_sum | DataFusion zip-unnest, grouped coordinate aggregation, sort and collect; array-length invariant checked by UDAF |
| fold | DataFusion UDAF executing compiled scalar PhysicalExpr sequence/combine functions |
| hist | ndhistogram uniform histogram with Hail endpoint adapter |
| corr | DataFusion CorrelationAccumulator with paired missing-value handling |
| approx_quantiles, approx_median | Apache DataSketches KLL via kll-rs, serialized merge state |
| approx_cdf | Explicitly unsupported; no partial CDF schema |
| linreg | Bounded cross-products; nalgebra linear algebra; statrs Student-t/F tails |
| downsample | Bounded dynamic two-dimensional grid adapter for Hail representative semantics |
| hardy_weinberg_test, inbreeding, call_stats, info_score | Arrow UDAFs implementing Hail genetics formulas; no matching library for exact Hail HWE mid-p found (see genomic research) |
| ndarray_sum | Arrow add_wrapping and zip kernels over typed array state; shape validation |

Table grouping and nested aggregation are planned through DataFusion. Python
builds expression IR and localizes results; it does not group or aggregate rows.
The current physical execution is single-stage. Merge-capable UDAF state does
not imply distributed partial execution has been enabled.

## Remaining compatibility limits

- `approx_cdf` is rejected in both Python and native execution because the current
  KLL backend cannot expose Hail's retained items and per-level compaction state.
  See INTENTIONAL_HAIL_DIVERGENCES.md.
- Approximate quantiles use Apache DataSketches KLL.
  Hail k below 8 is accepted using the sketch library's minimum capacity 8;
  values above 65535 are unsupported. Vail rejects int64 observations outside
  ±2^53 to avoid silently changing order; numerical parity is tracked in OPEN_ISSUES.md.
  Approximation guarantees have not been established as identical to Hail's k.
- Histogram bounds/bin counts, requested quantiles, fold initial states and
  call_stats allele counts accept scalar expressions over local or captured
  globals. They are evaluated lazily through a singleton DataFusion projection,
  independently of the row stream, including on empty input. See
  AGGREGATE_INITIALIZATION.md for the execution contract and tests.
- The HWE `one_sided` argument accepts a Boolean expression, including local or
  captured globals and aggregate results. Mode selection stays in the lazy
  DataFusion result projection, with a missing mode producing a missing result.
  Sketch k, take n, and regression nested_dim are Python
  integers in Hail's public API and remain static configuration here.
- Regression uses normal equations, so nearly dependent predictors may lose
  precision. Matrix solves and probability distributions use libraries.
- Dictionary/set localization and canonical grouping-key ordering are supported;
  the broader Hail dictionary expression API remains separate work.
- The upstream test selection exercises supported table/expression operations;
  it does not run every test in Hail's complete expression suite. The pinned Hail
  runtime oracle is available; HWE expression modes and `k=65536` acceptance were
  checked against it on 2026-09-29.

## Validation

Tests cover actual execution as well as construction: empty/missing input,
filtered and exploded scopes, nested dictionaries, array coordinates, grouped
aggregation, ordered take, set collection, fold, numeric kernels, genomic
statistics and ndarray sums. Twelve original Hail aggregator test bodies are
run directly by `python/tests/test_upstream_aggregators.py`. Detailed commands,
counts and failures are preserved by `scripts/run_all_tests.py` under
`conformance/runs/`.

Latest verification (2026-09-29): `python -m pytest python/tests
conformance/test_harness.py -q` passed 311 tests and 19 subtests;
`cargo test -q -p vail-execution -p vail-table` passed 42 tests.
The HWE mode regressions use Hail's golden counts `(1, 2, 1)`:
two-sided p-value `0.6571428571428571`, one-sided p-value
`0.5714285714285714`, and expected heterozygote frequency
`0.5714285714285714`. Tests also cover global and aggregate-valued modes,
missing modes, empty input, lazy global access, native/Python CDF rejection,
and rejection at `k=65536`.
