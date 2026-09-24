# Table conformance harness

The harness compares a pinned Hail process and a native worker in separate subprocesses. It uses only the Python standard library in the harness itself.

## Run it

From the `vail` project directory:

```sh
python3.11 -m unittest conformance.test_harness -v
.venv/bin/python -m conformance.run --backend native --command '.venv/bin/python -m scripts.native_conformance_worker'
VAIL_HAIL_PYTHON=/path/to/pinned-hail-python python3.11 -m conformance.run --backend oracle
```

`--backend oracle` uses `VAIL_HAIL_PYTHON` when set, otherwise the current Python interpreter, and adds the checked-out Hail Python source to `PYTHONPATH`. The Hail source checkout must be built and have its runtime Python dependencies/JARs available. Oracle initialization failure is reported as `UNAVAILABLE` and fails the command. It never promotes source-derived fixture expectations to captured oracle results.

A native `UNSUPPORTED` fixture is reported separately and exits with status 2; it is not counted as passed. A failed assertion or unavailable oracle exits with status 1. The optional `--write-outcomes PATH` stores raw worker responses.

## Wire format

Each request is one JSON line containing a whole fixture from `fixtures/case_*.json`. The response is one JSON line:

```json
{"protocol":"vail-conformance/1","fixture_id":"case-name","status":"ok","steps":[{"id":"collect","value":{"rows":[{"k":1}],"key":["k"]}}]}
```

A step error is `{ "id": "...", "error": { "class": "...", "category": "source_mismatch|unsupported|...", "message": "..." } }`. A worker may report whole-fixture `status` as `unsupported`, `unavailable`, or `error` with a diagnostic. Native request shapes and outcome shapes are implemented in `protocol.py`.

Schemas use compact type names (`int32`, `int64`, `bool`, `float32`, `float64`, `string`) plus nested `{"struct": [...]}` and `{"array": ...}` forms. Missing is `{"$missing": true}`; special float values are tagged (`NaN`, `+Infinity`, `-Infinity`, `-0`) by `codec.py`. The fixture field `nullable` is an input validation constraint for whether a fixture may contain missing values. It is not a claim that Hail's `tstruct` exposes Arrow-style requiredness; adapters may map columns conservatively as nullable.

Assertions distinguish exact ordered results, bags (order ignored, multiplicity preserved), and ordered sort-key groups with a bag comparison inside ties. Source-derived expectations have `provenance: source-derived-not-oracle-captured` until the Hail worker has actually run and the checked results are recorded explicitly.

## Initial C1 run record

Baseline source is Hail commit `e10cc355bc8cd0de9c5f29d5182d31527898bae1`. The generated [API ledger](api-ledger.json) records direct Table/GroupedTable members, inherited BaseTable methods and protocols, constructors/factories, `read_table`/`range_table`, direct IR class references, and explicitly unknown transitive IR closure. The original compact public inventory remains the generator default for `docs/research/hail-api-inventory.json`; create this expanded ledger with `python3.12 scripts/inventory_hail_api.py ../hail/hail/python/hail/table.py --ledger --output conformance/api-ledger.json`. The source commit is derived from the checked-out Hail Git repository unless `--commit` is supplied. Neither mode imports Hail.

Native run (2026-09-24) used the repository `.venv` Python 3.12 worker `scripts.native_conformance_worker`: key/null order, duplicate-key bag/order, and sorted head/take passed. The globals/source-error fixture returned `UNSUPPORTED: native globals not implemented`; the runner counted one unsupported fixture and exited 2. This is visible coverage status, not a parity pass.

Oracle attempt ran `VAIL_HAIL_PYTHON=/private/tmp/vail-hail-oracle/bin/python python3.11 -m conformance.run --backend oracle conformance/fixtures/case_key_order_nulls.json`; `/private/tmp/vail-hail-oracle` was an isolated uv-created Python 3.12 environment with the Hail source tree added to `PYTHONPATH`. It failed during Hail initialization with `ModuleNotFoundError: No module named 'hailtop.version'`. The checked-out source lacks generated `hailtop/version.py` and `hail-all-spark.jar`; that isolated environment also has no `pyspark`, `numpy`, `pandas`, `avro`, or `deprecated` modules. No Spark/JVM packages were installed, and no result is marked oracle-captured.

## Original upstream test bodies

`uv run --no-sync python -m conformance.upstream_table_cases` loads an explicit allowlist from the pinned Hail test source and executes the original bodies/assertions against Vail. It omits the Hail module imports, initialization fixtures and decorators. It is neither full upstream pytest collection nor a Hail oracle run. The current run has 89 passing cases (nine complete definitions plus 80 `rt`/`par` head/tail variants), and one failing definition, `test_annotate`, which stops at unsupported `_same`. The 80 cache/checkpoint variants remain untested. Select and both transmute bodies pass; full annotate additionally requires higher-order array and set operations. See [the grouped results](../docs/TABLE_EXPRESSION_TEST_RESULTS.md). [The results](upstream-table-results.json) include case parameters, source SHA-256 and Vail source fingerprints.

Run all local Python tests with `uv run --no-sync python -m pytest python/tests conformance/test_harness.py -q`. The `python -m` invocation keeps the project root available for the conformance package. See [the per-test inventory](../docs/HAIL_TEST_INVENTORY.md) for remaining requirements and scope limitations.

The inventory generator is `uv run --no-sync python scripts/inventory_hail_tests.py`. It merges the static Python/Scala census with the reviewed `table-test-requirements.json` and `extra-table-requirements-*.json` overlays, then attaches current source-body evidence. It writes the JSON inventory and the main/supplementary Markdown matrices. Reviewed requirements, static inference and execution results are separate fields; unknown fixture/provider behavior is retained for review. Changing pinned source or Vail files covered by an evidence hash requires updating the corresponding review/evidence first.

The full reviewed run is saved in [integrated-verified](runs/20260924T205540Z-integrated-verified/summary.json), with command logs, source fingerprints, native extension hashes and separate failure/unsupported/unavailable counts. Reproduce it with `uv run --no-sync python scripts/run_all_tests.py --label integrated`.
