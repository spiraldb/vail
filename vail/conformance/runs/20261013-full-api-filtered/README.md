# Full Hail Python API suite, filtered by test purpose

Results: [category summary and every failing definition](REPORT.md), [every failing parameter/body case](ALL_FAILURES.md), [machine-readable results](results.json), [selection/exclusion reasons](selection.json), and [zip-join timeout analysis](SLOW_ZIP_JOIN.md).

Tested Vail: `464265f3ac864729a4f2115e10492e3044f5d400`, using its merged release wheel. The run includes all upstream Python API areas, rather than only Table tests. Runtime/product code was not changed for this audit.

Reproduction:

```sh
python conformance/suite_selection.py ../hail/hail/python/test/hail selection.json
python conformance/full_hail_pytest.py --source ../hail/hail/python/test/hail \
  --output pytest-api --selection selection.json --module-timeout 1800 --per-test-timeout 120
python conformance/supplement_filtered_suite.py selection.json pytest-api bodies-final --jobs 2 --timeout 120
```

This run started supplementary attempts for already completed modules in `bodies-initial`, then used `--prior bodies-initial` for the remaining definitions. `bodies-corrected` preserves the exact original 56 Table / 60 Matrix backward-compatibility fixture paths using local enumeration instead of requiring cloud filesystem SDKs. Those tests then fail in their original fixture setup because ndarray `reshape` is missing; their behavior is not claimed as verified.

Ordinary Python dependencies installed into the isolated test environment: scipy, avro, orjson, dill, requests, pyspark 3.5.9 (for import surfaces; no Spark backend test was run), typing_extensions, decorator, frozenlist, and humanize. Upstream Hailtop immutable-value/decorator helpers are loaded without exposing the stock Hail runtime package. Dependency preflight logs are outside the repository and do not contribute to counts.

The harness now has an external native-safe per-test deadline in addition to SIGALRM. That addition was made after this run exposed a native call that ignored Python alarm delivery; the recorded timeout was stopped manually. Hard-deadline and normal-exit subprocess smoke checks passed. Counts and selection were audited: every included definition is accounted for exactly once, and excluded definitions are absent from the report.
