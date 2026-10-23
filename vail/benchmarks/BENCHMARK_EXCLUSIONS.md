# Benchmark comparison exclusions

`known_unsupported_benchmarks.json` is the versioned default scope policy. It lists
39 exact upstream case IDs from the October 23, 2026 cloud scan, the missing API,
workflow or adapter limitation, and the observations from both Vail input formats.
The policy applies to comparison benchmarks, not conformance tests.

`upstream_benchmark_coverage.py` loads this file automatically. An excluded Hail
case gets `comparison_not_applicable`; an excluded Vail case gets `unsupported`.
Both have `actual_sample: false` and a policy reference. No worker, Spark JVM,
fixture preparation or timed query starts for that case. Existing resumed
receipts remain historical observations. The report records the policy hash and
version. Every supported case remains eligible, including cases that previously
failed because of a runtime bug.

The policy deliberately does not infer unsupported status from an arbitrary
failure. The ndarray construction TypeError, gnomAD keyword TypeError, range-array
panic, native indexed-read missing-file error and timeouts remain visible failures.
An explicit missing API or an acknowledged unimplemented operation is different
from those product failures.

## Re-enabling a case

Do not periodically rerun excluded Hail cases to rediscover the same limitations.
Reconsider an entry only when a corresponding functionality or adapter fix is
suspected. First run an exact targeted Vail probe into a fresh output report:

```sh
python benchmarks/upstream_benchmark_coverage.py --engine vail \
  --case 'benchmark_table::test_table_aggregate_approx_cdf' \
  --probe-excluded --probe-reason 'Suspected approx_cdf implementation fix in COMMIT' \
  --output /tmp/approx-cdf-probe.json
```

`--probe-excluded` requires explicit case selection and a concrete reason. It
cannot reuse a previous report with `--resume`. For the three-mode cloud harness,
probe the same exact case separately with Hail-written and converted native
inputs, outside the comparison scan; passing one storage mode does not establish
the other. Confirm correctness, retain the probe evidence, then remove only the
verified entry in a reviewed policy change. Hail comparison timing can resume
once a usable Vail comparison exists.

The cloud `run_scan.py` adapter accepts the same JSON shape via `--skip-policy`:
pass this repository file explicitly rather than regenerate the list from a
transient VM state. Its Hail mode preserves old receipts and creates
`comparison_not_applicable`, `actual_sample: false` for future excluded attempts.
It keeps Vail failures in the report. Cases outside the table/matrix inventory of
the repository coverage runner remain listed in the policy so broader inventories
and future adapters share the same default exclusions.

## VDS combiner distinction

The upstream `test_vds_combiner_chr22` imports `new_combiner` directly from
`hail.vds.combiner`, constructs a restartable combiner and invokes `.run()`.
Vail's `vds/README.md` explicitly lists that restartable `new_combiner`/
`load_combiner` workflow as unported. The cloud adapter returned `unsupported`
before executing that Vail benchmark. This was not an observed Vail query failure.

Vail does implement `transform_gvcf` and `combine_variant_datasets`. Other combiner
cases were excluded because their upstream imports still called Hail helpers and
the adapter had not remapped and verified them. Their policy kind is
`harness_limitation`, not `missing_api`. An adapter fix and targeted probes can
re-enable those cases independently of the restartable combiner workflow.

Source and timed execution remain frozen at the policy's recorded revision. This
policy does not change reader, query or storage behavior.
