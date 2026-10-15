"""Readable summary, retaining separate pytest and isolated-body evidence."""
from collections import Counter
import json
from run_suite import ROOT

d=json.loads((ROOT/'categorized.json').read_text());categories=d['definition_categories']
assert len(d['definitions'])==1546
assert len({(r['module'],r['name']) for r in d['definitions']})==1546
assert sum(d['definition_statuses'].values())==1546
assert not any(r['status'] in ('not_attempted','collection_blocked') for r in d['definitions'])
assert not any(r['status'] in ('not_executed','incomplete') for r in d['cases'] if r['source_definition'])
labels={
'missing_expression_or_table_api':('Missing expression/Table/type APIs','abs, expression show/cache, Table export, coercers, ordering and unsupported aggregators'),
'missing_high_level_api':('Missing analytics/genomics/plotting APIs','Regression, PCA/relatedness, sample_qc, Pedigree, experimental, VDS, plotting'),
'missing_matrix_numerical_api':('Missing BlockMatrix/ndarray APIs','BlockMatrix constructors, from_entry_expr, ndarray indexing and numerical operations'),
'hail_ir_jvm_or_backend_internals':('Hail IR/JVM/backend contracts','Direct IR construction/typechecking/structural assertions; Spark, service and Py4J-specific tests'),
'format_io_and_fixture_compatibility':('Import/export and fixture/storage compatibility','BGEN/GEN/PLINK, matrix text import, compressed export, cloud paths and legacy Hail .mt fixtures'),
'expression_typing_scope_or_validation':('Expression typing/scope/validation','Mixed numeric array promotion, bind(_ctx), lambda scopes, signatures and error contracts'),
'harness_or_fixture_setup':('Harness/helper/fixture blockers','TemporaryFilename/Directory, private comparison helpers, unresolved fixtures/imports'),
'behavior_or_assertion_mismatch':('Behavior/assertion mismatches','NaN min/max, ordering/comparisons, fold rounding, partition layout, VCF rendering, phased BN golden'),
'planning_or_execution':('Planning/execution/serialization and exception compatibility','Column aggregation/init, missing matrix row HWE reducer, take coercion, struct pickle and expected error handling'),
 'timeout_or_native_crash':('Bounded timeouts/native panics','Three pytest timeouts, one isolated-body timeout, two empty-struct Arrow panic failures'),
}
lines=['# Full upstream Python API suite after variant QC','',
 f'Tested Vail commit: `{d["provenance"]["commit"]}`.',
 f'Pinned Hail source: `{d["provenance"]["upstream_commit"]}`.',
 f'Native extension SHA-256: `{d["provenance"]["native_sha256"]}`.','',
 '**The full suite does not pass.** All 62 `test_*.py` modules under `hail/hail/python/test/hail`, including the three backend modules, were attempted. The source census has 1,546 definitions. This scope covers the Hail Python API suite; Scala tests, hailtop infrastructure and doctests are outside this run.','',
 '## Authoritative pytest outcomes','',
 '- **424 passing source test cases** (parametrization counted separately).',
 '- **794 failing test bodies**, **459 setup failures**, **3 external timeouts**.',
 '- **26 modules with collection errors**. Their uncollected definitions are not counted as pytest passes.',
 '- All collected source cases received an outcome. Continuations ran the cases after timeouts. A further exact-definition selection recovered 14 cases initially excluded because pytest `--deselect` uses prefix matching.',
 '- Twelve incidental imported helper cases are excluded from the above counts (ten pass, two setup failures); the raw event totals remain in `categorized.json`.','',
 '## Definition-level evidence','',
 '- **407 definitions** pass all their observed pytest parameter cases.',
 '- Of the **476 collection-blocked definitions**, isolated original-body attempts pass **145**, while **331 fail or remain blocked**.',
 '- Thus **994 definitions** still have an observed failure/blocker in either mode. The 145 body-only passes do not repair their modules\' pytest collection failures.','',
 'Each failing definition receives one category based on its first observed failure. Counts do not mean there is only one issue in the definition, nor that every remaining statement executed.','',
 '| First failure category | Definitions | Examples |','| --- | ---: | --- |']
for key,(label,example) in sorted(labels.items(),key=lambda x:-categories[x[0]]):lines.append(f'| {label} | {categories[key]} | {example} |')
lines += ['', '## Concrete behavior and planning gaps','',
 '- Thirteen assertion failures concern exact `n_partitions()` values after key-prefix interval extraction; these are partition-layout/pruning expectations rather than evidence of incorrect collected rows.',
 '- Other behavior failures include NaN min/max (`NaN` versus `0.0`), array/dictionary ordering and comparisons, fold multiplication differing in its last bits, missing scope exceptions, tuple versus list reference metadata, VCF formatting/headers, and diagnostic wording.',
 '- The fixed phased Balding–Nichols genotype list still differs from Hail\'s golden list. This is an actual compatibility failure, not an IR assertion.',
 '- Matrix column aggregate initialization still rejects group-dependent initializers; another column aggregation is rejected because row fields remain outside an aggregator.',
 '- General MatrixTable row `agg.hardy_weinberg_test` still lacks a per-list reducer. `variant_qc` uses the scalar counts function, so porting QC did not implement that separate matrix aggregate path.',
 '- Two VDS test bodies stop at native `take` argument coercion. Two Struct pickle/dill bodies hit `RecursionError`.',
 '- Several execution-looking failures deliberately trigger errors and then expect Hail\'s exception class/message. The planning/execution category includes these compatibility failures; it is not a count of broken valid-data query plans.','',
 '## Native failures and bounded tests','',
 'Both native panic failures involve constructing an empty-field Arrow StructArray without supplying its length:', '',
 '- `expr/test_expr.py::Tests.test_initop_matrix_aggregate_rows_by_key_rows`.',
 '- `table/test_table.py::test_group_within_partitions_after_import_vcf`.','',
 'External pytest limits are 180 seconds per test, with five seconds of supervisor allowance:', '',
 *[f'- `{t["module"]}::{t["nodeid"]}`: {t["error"]}.' for t in d['timeouts']],
 '- Isolated-body `table/test_table.py::Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245` exceeded its 180-second worker limit.',
 '', 'These are bounded observations, not proof that the tests would never finish. Later pytest cases were resumed, and none remain unexecuted.','',
 '## Randomness and variant QC','',
 '- All six definitions in upstream `test_randomness.py` pass through pytest.',
 '- Direct Hail IR/structural assertions remain intact and fail where Vail lacks Hail\'s IR objects; these are counted as IR/backend contracts.',
 '- Both dedicated upstream `variant_qc` bodies pass. Their module remains collection-blocked by its unrelated `vail.methods` import.',
 '- The combined sample/variant QC call-rate body stops at missing `sample_qc`.',
 '- The phased BN golden mismatch described above remains. Passing the dedicated randomness module does not imply every random-data consumer passes.','',
 '## Collection errors','',
 'Optional Python packages missing at collection include frozenlist, aiohttp, humanize and rich; pyspark is also absent for Spark-dependent modules. Others are missing Vail modules/types or backend contracts. Supplemental bodies expose further implementation gaps beneath these imports.','',
 '| Module | Collection error |','| --- | --- |']
for c in d['collection_errors']:lines.append(f'| {c["module"]} | {c["error"].replace("|", "\\|")} |')
lines += ['', '## Method and reproduction','',
 'The existing `full_hail_pytest.py` adapter redirects Hail imports to Vail in a temporary copy, initializes the real native Vail backend, adapts shared fixtures/sample snapshots and backend lifecycle, and leaves body assertions intact. Hail flag decorators are no-ops and JVM health fixtures are omitted. This is an adapted upstream run against Vail, not an unmodified Hail/Spark test run.','',
 '`full_hail_bodies.py` supplements only definitions which pytest could not collect. It retains original statements/assertions and parameter values while adapting imports, backend lifecycle and fixtures. Unresolved fixture/helper dependencies are blockers, never passes.','',
 'The orchestration uses four isolated pytest module workers, then up to two supplementary body workers; each module runs in its own temporary checkout. Module limits are 1,800 seconds. Raw events, logs, source hashes, source inventory, test-level classifications and error clusters are saved here.','',
 'From the Vail project directory:', '', '```sh',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_suite.py',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_bodies.py',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/analyze.py',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/run_residuals.py',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/analyze.py',
 '.venv/bin/python conformance/runs/20261014-upstream-after-variant-qc/write_report.py',
 '```', '',
 'For a fresh rerun, copy these scripts into a new run directory: existing results are deliberately reused.','',
 '## Remaining failures by definition','',
 '| Module | Definition | Mode | Category | First error |','| --- | --- | --- | --- | --- |']
for r in d['definitions']:
 if r['category'] not in ('passed','passed_body_only'):
  error=r.get('error','').replace('\n',' ')[:300].replace('|','\\|')
  lines.append(f'| {r["module"]} | {r["name"]} | {r["mode"]} | {r["category"]} | {error} |')
(ROOT/'README.md').write_text('\n'.join(lines)+'\n')
print('Report written; every source definition accounted for, no collected source cases missing.')
