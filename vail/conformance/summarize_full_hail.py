"""Join full pytest and supplementary original-body results without double counting."""
import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import subprocess


def randomness_role(name, body, module):
    leaf = name.split('.')[-1]
    if (module == 'test_randomness.py' or re.search(r'(randomness|seeded|random_seed|rng)', leaf)
            or leaf.startswith(('test_rand_', 'test_random_', 'test_sample'))
            and not leaf.startswith(('test_sample_qc', 'test_sampleqc', 'test_sample_and_variant'))):
        return 'specifically_randomness'
    if re.search(r'\b(rand_\w+|balding_nichols_model)\b|\.sample\(|\brandom\.', body):
        return 'random_data_for_other_functionality'
    return 'no_random_dependency_identified'


def category(error, error_type='', prior=''):
    text = error.lower()
    if "no module named 'hail'" in text:
        return 'harness_or_fixture_setup_blocker'
    if any(s in text for s in ('has no attribute \'rand_', 'random expression', 'randomness', 'balding_nichols_model', "name 'rand_")):
        return 'randomness_dependency'
    if error_type == 'AssertionError' or prior == 'assertion_mismatch':
        return 'assertion_mismatch'
    if any(s in text for s in ('panicked', 'panicexception', 'option::unwrap', 'native_panic')) or prior == 'native_panic':
        return 'native_panic'
    if any(s in text for s in ('worker exceeded', 'time limit', 'timed out', 'timeout')):
        return 'timeout'
    if prior == 'backend_internals' or any(s in text for s in ('backenddependency', 'scala/backend', 'vail.ir', 'vail.backend.', 'vail.utils.java', 'py4j', '_jvm')):
        return 'hail_backend_or_ir_dependency'
    if any(s in text for s in ('_vail_matrix.json', 'unsupported matrix descriptor', 'fastlanes.delta', 'no such file or directory')) or prior in ('legacy_matrix_format', 'fixture_resource'):
        return 'fixture_or_storage_format_blocker'
    if error_type in ('NameError', 'KeyError') or prior in ('harness_dependency',):
        return 'harness_or_fixture_setup_blocker'
    if any(s in text for s in ('import_vcf', 'export_vcf', 'import_bgen', 'export_bgen', 'import_plink', 'export_plink')):
        return 'format_io_api'
    if any(s in text for s in ('vail.methods', 'vail.vds', 'vail.experimental', 'vail.linalg', 'vail.ggplot', 'vail.genetics')) or prior == 'genetics':
        return 'genetics_or_other_high_level_api'
    if error_type in ('AttributeError', 'ImportError', 'ModuleNotFoundError') or 'unsupported' in text or 'not implemented' in text or 'not yet implemented' in text or error_type == 'NotImplementedError':
        return 'missing_or_unsupported_api'
    if error_type in ('TypeError', 'ValueError', 'ExpressionException') or prior in ('literal_inference', 'expression_or_validation'):
        return 'expression_types_or_validation'
    return 'execution_or_other_error'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory
    inventory = json.loads((root / 'pytest-resolved-deps/definition-inventory.json').read_text())
    inventory = [r for r in inventory if 'backend' not in Path(r['module']).parts]
    suite = Path(json.loads((root/'matrix.json').read_text())['source']).parents[1]
    imports = {}
    for module in {r['module'] for r in inventory}:
        imports[module] = {alias.asname or alias.name: node.module + '.' + alias.name
                           for node in ast.parse((suite/module).read_text()).body
                           if isinstance(node, ast.ImportFrom) and node.module
                           for alias in node.names}
    rows = {(r['module'], r['name']): dict(module=r['module'], name=r['name'], line=r['line'], status='not_attempted',
            randomness_role=randomness_role(r['name'], r['body'], r['module'])) for r in inventory}
    # Supplementary body results are explicitly identified as adapted runs.
    paths = sorted((root / 'bodies').glob('*.json')) + sorted((root / 'corrected-bodies').glob('*.json'))
    for path in paths:
        if path.name.startswith('results'):
            continue
        result = json.loads(path.read_text())
        key = result['module'], result['name']
        if key not in rows:
            continue
        variants = result.get('variants', [])
        bad = [v for v in variants if v['status'] != 'passed']
        failure = bad[0] if bad else result
        rows[key].update(status=result.get('status', 'blocked'), mode='isolated_body',
                         variants_attempted=len(variants), variants_passed=sum(v['status']=='passed' for v in variants),
                         error=failure.get('error', result.get('worker_error', '')),
                         error_type=failure.get('error_type', ''), phase=failure.get('phase', ''),
                         setup_errors=result.get('setup_errors', []))
    table = json.loads((root / 'table/results.json').read_text())
    for result in table['results']:
        key = 'table/test_table.py', result['test_id']
        cases = result.get('cases', [])
        failure = next((v for v in cases if v['status'] != 'passed'), {})
        rows[key].update(status=result['status'], mode='isolated_table_body', variants_attempted=len(cases),
                         variants_passed=sum(v['status']=='passed' for v in cases),
                         error=failure.get('error',''), error_type=failure.get('error_type',''), phase=failure.get('phase',''))
    matrix = json.loads((root / 'matrix.json').read_text())
    for result in matrix['cases']:
        candidates = [key for key in rows if key[0]=='matrixtable/test_matrix_table.py' and key[1].split('.')[-1]==result['case']]
        for key in candidates:
            rows[key].update(status=result['status'], mode='isolated_matrix_body',
                             variants_attempted=len(result.get('variants',[])),
                             variants_passed=sum(v['status']=='passed' for v in result.get('variants',[])),
                             error=result.get('message',''), error_type=result.get('error_type',''),
                             prior_category=result.get('category',''), phase=result.get('phase',''))
    if (root/'vcf-all.json').exists():
        for result in json.loads((root/'vcf-all.json').read_text())['results']:
            key = 'methods/test_impex.py', 'VCFTests.' + result['case']
            rows[key].update(status=result['status'], mode='isolated_vcf_body',
                             variants_attempted=1, variants_passed=int(result['status']=='passed'),
                             error=result.get('message',''), error_type=result.get('error_type',''))
    collected = defaultdict(list)
    collection_errors = []
    for path in sorted((root/'pytest-resolved-deps').glob('*.jsonl')):
        module = path.name[:-6].replace('__','/')
        if 'backend' in Path(module).parts:
            continue
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event['kind'] == 'collection':
                collection_errors.append(dict(module=module, error=event['error']))
            elif event['kind'] == 'test':
                collected[(module, event['nodeid'])].append(event)
    pytest_counts = Counter()
    pytest_by_definition = defaultdict(list)
    for (module, nodeid), events in collected.items():
        failure = next((e for e in events if e['outcome']=='failed'), None)
        call = next((e for e in events if e['when']=='call'), None)
        status = 'failed' if failure else call['outcome'] if call else 'setup_blocked'
        pytest_counts[status] += 1
        parts = nodeid.split('::')[1:]
        if parts:
            parts[-1] = parts[-1].split('[')[0]
            pytest_by_definition[(module, '.'.join(parts))].append(dict(status=status, error=(failure or call or events[0]).get('error','')))
    for key, events in pytest_by_definition.items():
        if key not in rows:
            continue
        bad = [e for e in events if e['status'] != 'passed']
        rows[key].update(pytest_statuses=dict(Counter(e['status'] for e in events)))
        # Prefer actual pytest when it reaches assertions; keep supplemented modes otherwise.
        if not bad:
            rows[key].update(status='passed', mode='pytest', variants_attempted=len(events), variants_passed=len(events), error='', error_type='')
        elif rows[key]['status']=='not_attempted':
            rows[key].update(status='failed', mode='pytest', error=bad[0]['error'])
    for row in rows.values():
        if row['status'] == 'passed':
            row['category'] = 'passed'
        elif row['status']=='not_attempted':
            row['category']='not_attempted'
        else:
            row['category'] = category(row.get('error',''), row.get('error_type',''), row.get('prior_category',''))
        if row.get('error')=='sample' and row['randomness_role']=='specifically_randomness':
            row['category']='randomness_dependency'
        if 'requires_lowering' in row.get('error',''):
            row['category']='hail_backend_or_ir_dependency'
        if row.get('error_type')=='KeyError' and row.get('phase') in ('body','test_body'):
            row['category']='expression_types_or_validation'
        if row['category']=='harness_or_fixture_setup_blocker':
            match = re.search(r"name '([^']+)' is not defined", row.get('error',''))
            dependency = imports[row['module']].get(match[1], '') if match else ''
            if match and match[1] in ('ir', 'Env'):
                row['category']='hail_backend_or_ir_dependency'
            elif dependency.startswith(('hail.backend.', 'hail.ir.', 'hail.utils.java.', 'hailtop.')):
                row['category']='hail_backend_or_ir_dependency'
            elif dependency.startswith(('hail.linalg.', 'hail.methods.', 'hail.experimental.', 'hail.vds.')):
                row['category']='genetics_or_other_high_level_api'
            elif dependency.startswith('hail.') and match[1] not in ('FatalError','HailUserError','TemporaryFilename','new_local_temp_file'):
                row['category']='missing_or_unsupported_api'
            if dependency:
                row['missing_import']=dependency
        if row['category']=='randomness_dependency' and row['randomness_role']=='no_random_dependency_identified':
            row['randomness_role']='random_data_for_other_functionality'
    counts = Counter(r['category'] for r in rows.values())
    random_counts = Counter(r['randomness_role'] for r in rows.values() if r['category']=='randomness_dependency')
    report = dict(commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                  scope='Upstream Python test/hail API suite, excluding backend directories, hailtop infrastructure, Scala and doctests',
                  definitions=len(rows), statuses=dict(Counter(r['status'] for r in rows.values())),
                  categories=dict(counts), randomness_first_blocker=dict(random_counts),
                  pytest_parameter_cases=dict(pytest_counts), pytest_collection_errors=collection_errors,
                  randomness_intent_among_all_nonpasses=dict(Counter(r['randomness_role'] for r in rows.values() if r['status']!='passed')),
                  native_library_sha256=hashlib.sha256(Path('vail/python/vail/_native.abi3.so').read_bytes()).hexdigest(),
                  results=list(rows.values()))
    (root/'categorized.json').write_text(json.dumps(report,indent=2))
    lines = ['# Hail API suite status', '', f'Commit: `{report["commit"]}`.', '', report['scope'] + '.', '',
             'All in-scope test modules were attempted using pytest. Hail imports were redirected to Vail; initialization, flags and JVM health infrastructure were omitted. Missing API/internal imports remain collection errors.', '',
             'Original-body runners supplement collection-blocked modules. They retain assertions and parameter values, omit backend decorators/imports, and report unresolved setup dependencies as blockers. These results are not an unmodified Hail pytest pass.', '',
             f'Inventory: **{len(rows)} definitions**. Categories below count definitions once, using the first observed blocker. Parameter cases are recorded separately.', '',
             '| Category | Definitions |', '| --- | ---: |']
    lines.extend(f'| {k} | {v} |' for k,v in counts.most_common())
    lines += ['', '## Randomness as the first blocker', '', '| Purpose | Definitions |', '| --- | ---: |']
    lines.extend(f'| {k} | {v} |' for k,v in random_counts.items())
    lines += ['', 'Randomness purpose is classified from the original test name and body. Explicit randomness/seed/sampling tests are separated from other tests using generated data. Missing sampling APIs count as randomness dependencies. Tests blocked earlier by missing backend or fixture APIs retain their earlier blocker.', '',
              '## Results by API area', '', '| Area | Passed | Failed or blocked |', '| --- | ---: | ---: |']
    areas = defaultdict(Counter)
    for row in rows.values():
        areas[row['module'].split('/')[0] if '/' in row['module'] else 'root API/internals']['passed' if row['status']=='passed' else 'nonpass'] += 1
    lines.extend(f'| {area} | {counts["passed"]} | {counts["nonpass"]} |' for area,counts in sorted(areas.items()))
    lines += ['',
              '## Failures and blockers', '', '| Module | Definition | Category | Randomness purpose | First error |', '| --- | --- | --- | --- | --- |']
    for row in rows.values():
        if row['status'] != 'passed':
            error = row.get('error','').replace('\n',' ')[:220].replace('|','\\|')
            lines.append(f'| {row["module"]} | {row["name"]} | {row["category"]} | {row["randomness_role"]} | {error} |')
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in ('results','pytest_collection_errors')}, indent=2))


if __name__ == '__main__':
    main()
