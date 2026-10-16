"""Report filtered Hail API coverage, separating execution and setup blockers."""
import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re


def short_error(error):
    lines = error.splitlines()
    terminal = [line[4:].strip() for line in lines if line.startswith('E   ')]
    return '\n'.join(terminal) if terminal else error


def bucket(row, error):
    text = error.lower()
    module, name = row['module'], row['name'].lower()
    if 'panicked' in text or 'panicexception' in text or 'result::unwrap' in text:
        return 'Native execution panics', 'Fix the native crash; the assertion was not completed.'
    if row.get('status') == 'timed_out' or 'timeout' in text or 'exceeded' in text and 'second' in text:
        return 'Planning/execution timeouts', 'Investigate the slow plan/execution; rerun this exact test after fixing it.'
    if any(s in text for s in ('temporaryfilename', 'temporarydirectory', 'new_local_temp', 'new_temp_dir')):
        return 'Test helper dependencies', 'Supply the temporary-path/context helper used by this mixed API test.'
    if any(s in text for s in ('rand_unif', 'rand_norm', 'rand_bool', 'balding_nichols', 'randomness', 'rand_int')):
        return 'Random-data dependencies in mixed tests', 'This is not a randomness-only test; its fixture/body still requires the named random-data API.'
    if any(s in text for s in ('_vail_matrix.json', 'legacy', 'metadata.json.gz', 'unsupported matrix descriptor')) or 'backward_compatability' in name:
        return 'Legacy Hail fixtures/storage', 'Port this fixture to the new format or implement the deliberately deferred legacy reader.'
    if any(s in text for s in ('set/dictionary ordering', 'approx_cdf', 'compaction_counts', 'utf-16', '65535')):
        return 'Documented unsupported semantics', 'See INTENTIONAL_HAIL_DIVERGENCES.md; this is an explicit limitation.'
    if any(s in text for s in ('backenddependency', 'env.backend', 'env.hail', 'scala/backend', 'requires_lowering', "attribute '_references'", 'py4j', 'scala_object')):
        return 'JVM-dependent setup in mixed tests', 'Replace the Hail/JVM-dependent fixture or inspection; the public behavior remains unverified.'
    if any(s in text for s in ('reshape', 'vail.nd', 'ndarray', 'blockmatrix', 'vail.linalg')) or module.startswith('linalg/') or module=='expr/test_ndarrays.py':
        return 'Ndarray and BlockMatrix APIs', 'Implement the named ndarray/linear-algebra operation or its type/shape handling.'
    if module.startswith('vds/') or 'vail.vds' in text:
        return 'VariantDataset and combiner', 'Implement the VariantDataset/combiner API required by this test.'
    if module.startswith(('plot/', 'ggplot/')) or 'vail.ggplot' in text:
        return 'Plotting', 'Implement the plotting/ggplot API required by this test.'
    if module.startswith('experimental/') or 'experimental' in text:
        return 'Experimental APIs', 'Implement the named experimental API (annotation database, time, whitening, or dataset operations).'
    if any(s in text for s in ('import_bgen', 'export_bgen', 'import_plink', 'export_plink', 'import_gen', 'import_matrix_table', 'compressed vcf', 'export_vcf', 'export_matrix')) or module=='methods/test_impex.py':
        return 'Format import/export', 'Implement or correct the specific import/export option, schema, or rendered output shown below.'
    if any(s in text for s in ('liftover', 'fasta', 'sequence context', 'has_sequence', 'add_sequence')):
        return 'Reference sequence/liftover', 'Implement reference sequence/liftover support; coordinate-only references are insufficient.'
    if module.startswith('genetics/') or any(s in name for s in ('locus', 'call_', 'reference_genome')) and module.startswith('expr/'):
        return 'Call/Locus/reference/pedigree APIs', 'Complete the named value/expression/reference method or correct its validation/representation.'
    if module.startswith('methods/'):
        return 'High-level genetics/statistics methods', 'Implement the named public genetics/statistics method or correct its numerical/result contract.'
    if module.startswith('extract_intervals/'):
        return 'Interval pruning and partition counts', 'Push the predicate through transformed/Matrix row sources and prune the correct partitions.'
    if any(s in name for s in ('agg', 'scan', 'counter', 'hist', 'cdf', 'quantile', 'linreg')):
        return 'Aggregation and scans', 'Complete the named aggregator/scan, source binding, configuration, or numerical/error behavior.'
    if any(s in text for s in ('coercer', '_indices', 'typechecker', 'vail.typecheck', 'vail.expr.types')) or module.startswith('typecheck/') or module=='expr/test_types.py':
        return 'Frontend types/namespaces/metadata', 'Implement the missing frontend import/type-coercion/source-metadata contract.'
    if module.startswith('matrixtable/'):
        return 'Matrix transformations and actions', 'Correct the named Matrix transformation, axis binding, action, or result schema.'
    if module.startswith('table/'):
        return 'Table transformations/actions/options', 'Correct the named Table method, error contract, option, or partition behavior.'
    if module.startswith('expr/'):
        return 'Scalar/collection/string/numeric expressions', 'Implement the named expression operation or correct its type, missingness, rendering, or validation.'
    if module.startswith('utils/'):
        return 'Python value/utilities interoperability', 'Complete the named value/helper behavior, including hashing, repr, pickle, or container conversion.'
    return 'Other API/fixture behavior', 'Resolve the specific error shown below; no pass is inferred.'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory
    selection = json.loads((root / 'selection.json').read_text())
    rows = {(r['module'], r['name']): dict(r, cases=[], mode='unattempted') for r in selection['included']}
    imports = {}
    for module in {r['module'] for r in selection['included']}:
        mapping = {}
        for node in ast.parse((Path(selection['source']) / module).read_text()).body:
            if isinstance(node, ast.ImportFrom) and node.module:
                mapping.update({a.asname or a.name: node.module+'.'+a.name for a in node.names})
            elif isinstance(node, ast.Import):
                mapping.update({a.asname or a.name.split('.')[0]: a.name for a in node.names})
        imports[module] = mapping
    collection_errors = []
    for folder in ['bodies-initial', 'bodies-final', 'bodies-corrected']:
        for path in (root/folder).glob('[0-9]*.json'):
            result = json.loads(path.read_text())
            key = result['module'], result['name']
            if key not in rows:
                continue
            row = rows[key]
            row.update(mode='isolated_original_body', evidence=str(path.relative_to(root)), setup_errors=result.get('setup_errors', []))
            row['cases'] = result.get('variants', []) or [dict(status='blocked', error=result.get('error', result.get('worker_error', result.get('status',''))), error_type=result.get('error_type',''), phase=result.get('phase','setup_or_parameterization'))]
            if result.get('worker_error'):
                row['cases'].append(dict(status='timed_out' if 'exceeded' in result['worker_error'] else 'blocked', error=result['worker_error'], phase='worker'))
    events_by_case = defaultdict(list)
    for path in (root/'pytest-api').glob('*.jsonl'):
        module = path.name.removesuffix('.jsonl').replace('__','/')
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event['kind']=='collection':
                collection_errors.append(dict(module=module, error=event['error'], evidence=str(path.relative_to(root))))
            if event['kind']=='test':
                events_by_case[(module,event['nodeid'])].append(event)
    for (module,nodeid), events in events_by_case.items():
        parts = nodeid.split('::')[1:]
        parts[-1] = parts[-1].split('[')[0]
        key = module,'.'.join(parts)
        if key not in rows:
            continue
        failure = next((e for e in events if e['outcome']=='failed'), None)
        call = next((e for e in events if e['when']=='call'), None)
        skip = next((e for e in events if e['outcome']=='skipped'), None)
        event = failure or call or skip or events[-1]
        status = 'failed' if failure and failure['when']=='call' else 'blocked' if failure else call['outcome'] if call else 'skipped' if skip else 'blocked'
        if '[XPASS(strict)]' in event.get('error',''):
            status = 'xpass_strict'
        row = rows[key]
        if row['mode']!='pytest':
            row.update(mode='pytest',cases=[],evidence='pytest-api/'+module.replace('/','__')+'.jsonl')
        row['cases'].append(dict(status=status,nodeid=nodeid,error=event.get('error',''),phase=event.get('when'),duration=event.get('duration')))
    for timeout in json.loads((root/'native-timeouts.json').read_text()) if (root/'native-timeouts.json').exists() else []:
        parts=timeout['nodeid'].split('::')[1:];parts[-1]=parts[-1].split('[')[0]
        row=rows[timeout['module'],'.'.join(parts)]
        row['cases']=[dict(status='timed_out',nodeid=timeout['nodeid'],error=f"Native execution exceeded {timeout['declared_limit_seconds']} second per-test limit; {timeout['reason']}",phase='call')]
    failure_cases=[]
    for row in rows.values():
        cases=row['cases'];statuses=Counter(c['status'] for c in cases)
        row['case_statuses']=dict(statuses)
        if not cases:
            row['status']='unattempted'
        elif 'timed_out' in statuses:
            row['status']='timed_out'
        elif 'failed' in statuses:
            row['status']='failed'
        elif 'blocked' in statuses:
            row['status']='blocked'
        elif 'xpass_strict' in statuses:
            row['status']='xpass_strict'
        elif statuses.get('passed') and statuses.get('skipped'):
            row['status']='partial_skipped'
        elif statuses.get('passed')==len(cases):
            row['status']='passed'
        else:
            row['status']='skipped'
        bad=[c for c in cases if c['status'] not in ('passed','skipped')]
        first=bad[0] if bad else {}
        error=short_error(first.get('error',''))
        missing=re.search(r"name '([^']+)' is not defined",error)
        if missing and missing[1] in imports[row['module']]:
            row['missing_import']=imports[row['module']][missing[1]]
            error += '\nMissing import: '+row['missing_import']
        if not error and first:
            error = first.get('error_type', 'AssertionError') + ': see the failed assertion in raw evidence'
        row['first_error']=error
        row['category'],row['needed']=bucket(row,error) if bad else ('Passed' if row['status']=='passed' else 'Skipped', '')
        if row['status']=='xpass_strict':
            row['category']='Strict XPASS: upstream expected failure now passes';row['needed']='Review/remove the upstream xfail marker; this is not a failed behavior assertion.'
        if bad:
            missing_api = re.search(r"module 'vail(?:\.([^']+))?' has no attribute '([^']+)'", error)
            missing_method = re.search(r"'([^']+)' object has no attribute '([^']+)'", error)
            if missing_api:
                row['needed'] = 'Implement hl.' + ((missing_api[1]+'.') if missing_api[1] else '') + missing_api[2] + '; this is the first missing API.'
            elif missing_method:
                row['needed'] = 'Expose '+missing_method[1]+'.'+missing_method[2]+' with its Hail method contract.'
            elif row.get('missing_import'):
                row['needed'] = 'Provide '+row['missing_import'].replace('hail.', 'vail.', 1)+'; its missing import prevents this test from reaching later assertions.'
            explicit = {
                '_maybe_flexindex_table_by_expr': 'Normalize direct/prefix/interval keys through the existing keyed-lookup machinery and expose this helper.',
                'unbound lambda variable': 'Carry lambda bindings through nested/grouped aggregate lowering.',
                'group-dependent initializers': 'Bind group-dependent initialization expressions on the Matrix aggregation axis.',
                'per-list hardy_weinberg_test': 'Provide the per-list HWE reducer used by Matrix entry aggregation.',
                'types files and parallel text export': 'Implement parallel export manifests and exported type files.',
                'only literal patterns': 'Implement the requested regex operation or document this exact unsupported regex mode.',
                'partial_type': 'Partial-type inference is explicitly deferred; this test requires it.',
                'cannot infer a Vail type for NAType': 'Treat pandas.NA as missing while inferring the scalar numeric type from the remaining values.',
                'partition not used yet': 'Correct the lifecycle/consumption of shared DataFusion repartition outputs in the Matrix lookup graph.',
                'StructArray::try_new_with_length': 'Preserve explicit row lengths for zero-field structs in grouping after VCF import.',
            }
            for marker, needed in explicit.items():
                if marker in error:
                    row['needed'] = needed
        for case in bad:
            failure_cases.append(dict(module=row['module'],name=row['name'],line=row['line'],status=case['status'],mode=row['mode'],category=row['category'],needed=row['needed'],error=short_error(case.get('error','')),parameters=case.get('parameters',{}),nodeid=case.get('nodeid',''),evidence=row.get('evidence','')))
    statuses=Counter(r['status'] for r in rows.values())
    nonpasses=[r for r in rows.values() if r['status'] not in ('passed','skipped','partial_skipped')]
    categories=Counter(r['category'] for r in nonpasses)
    case_statuses=Counter(c['status'] for r in rows.values() for c in r['cases'])
    import vail._native
    result=dict(commit=selection['vail_commit'],scope=selection['scope'],included=len(rows),excluded=len(selection['excluded']),definitions=dict(statuses),cases=dict(case_statuses),categories=dict(categories),collection_errors=collection_errors,native_sha256=hashlib.sha256(Path(vail._native.__file__).read_bytes()).hexdigest(),results=list(rows.values()))
    (root/'results.json').write_text(json.dumps(result,indent=2)+'\n')
    def cell(text):return str(text).replace('|','\\|').replace('\n',' ')
    lines=['# Full filtered Hail API suite against Vail','',f"Vail commit: `{result['commit']}`.",'',f"**{len(rows)} included definitions; {len(selection['excluded'])} excluded.** Execution modes and unattempted definitions are reported below; isolated-body attempts are separate from pytest execution. Original-body attempts supplement definitions blocked at collection or left after a native timeout; no excluded definition contributes to the results.",'','This is the upstream Python API suite redirected to Vail, not Scala tests or a stock Hail run. Hail initialization, backend health/flags decorators, and two sample Matrix fixtures are adapted. Tests and assertion bodies are preserved. The full selection and reasons are in [selection.json](selection.json).','', '## Broad remaining categories','', '| Category (first blocker per definition) | Definitions |','| --- | ---: |']
    lines.extend(f'| {cell(k)} | {v} |' for k,v in categories.most_common())
    lines += ['', '## Coverage and outcomes','', '| Definition outcome | Count |','| --- | ---: |']
    lines.extend(f'| {k} | {v} |' for k,v in statuses.items())
    lines += ['', 'A definition passes only if every attempted parameter case passes. Skips, setup/collection blockers, strict XPASS, and timeouts are separate from assertion failures. Counts are definitions unless explicitly called parameter cases. A first blocker does not prove later assertions work.', '', '| Parameter/body-case outcome | Count |','| --- | ---: |']
    lines.extend(f'| {k} | {v} |' for k,v in case_statuses.items())
    areas=defaultdict(Counter)
    for row in rows.values():areas[row['module'].split('/')[0] if '/' in row['module'] else 'root API'][row['status']]+=1
    lines += ['', '## By API area', '', '| Area | Passed | Failed | Blocked | Timed out | Skipped / partial | Strict XPASS |','| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
    for area,c in sorted(areas.items()):lines.append(f"| {area} | {c['passed']} | {c['failed']} | {c['blocked']} | {c['timed_out']} | {c['skipped']+c['partial_skipped']} | {c['xpass_strict']} |")
    lines += ['', '## Every remaining failing/blocked definition', '', 'Each row links to the raw evidence, with all parameter variants retained. [Every failing parameter/body case](ALL_FAILURES.md) is listed separately.', '', '| Test | Outcome | Category | First observed error / missing API | Required functionality | Evidence |','| --- | --- | --- | --- | --- | --- |']
    for row in sorted(nonpasses,key=lambda r:(r['category'],r['module'],r['name'])):
        error=cell(row['first_error'])[:450]
        lines.append(f"| `{row['module']}::{row['name']}` | {row['status']} | {cell(row['category'])} | {error} | {cell(row['needed'])} | [raw]({row.get('evidence','')}) |")
    lines += ['', '## Limitations', '', '- Timeouts are reported from the recorded attempt evidence; they are not assertion mismatches. See the runner configuration and raw evidence for the limits used.', '- Legacy `.ht`/`.mt` fixtures are retained where they are part of public read/import tests, but Vail lacks the legacy reader. Only the two existing sample VCF snapshots are rebuilt by the adapter.', '- Missing public packages and missing mixed-test fixture dependencies are reported explicitly; they are not counted as passes or silently deselected.', '- Numerical/collection behavior deliberately covered by the incompatibilities document is identified as a documented limitation where the first error makes that clear.','']
    (root/'REPORT.md').write_text('\n'.join(lines))
    details=['# Every failed or blocked parameter/body case','', '[Summary and definition counts](REPORT.md).','','| Test / parameter case | Outcome | Category | Actual error | Evidence |','| --- | --- | --- | --- | --- |']
    for case in failure_cases:
        identity=case['module']+'::'+case['name']
        if case['nodeid']:identity=case['module']+'::'+case['nodeid'].split('::',1)[-1]
        elif case['parameters']:identity+=' '+repr(case['parameters'])
        details.append(f"| {cell(identity)} | {case['status']} | {cell(case['category'])} | {cell(case['error'])[:650]} | [raw]({case['evidence']}) |")
    (root/'ALL_FAILURES.md').write_text('\n'.join(details)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('results','collection_errors')},indent=2))


if __name__=='__main__':main()
