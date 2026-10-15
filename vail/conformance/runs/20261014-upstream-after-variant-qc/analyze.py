"""Deduplicated upstream outcomes and first-failure taxonomy."""
import ast
from collections import Counter, defaultdict
import json
import re
from run_suite import ROOT, SOURCE, save


def short(error):
    lines=re.findall(r'^E\s+(.*)',error,re.M)
    return lines[-1] if lines else error.strip().splitlines()[-1] if error.strip() else ''


def classify(message,kind='',phase='',module='',missing_import=''):
    text=message.lower()
    if module.startswith('backend/') or module=='test_ir.py':return 'hail_ir_jvm_or_backend_internals'
    if "no module named 'hail'" in text:return 'harness_or_fixture_setup'
    if kind in ('timeout','native_crash') or any(x in text for x in ('worker exceeded','time limit exceeded','timed out','panicexception','panicked','option::unwrap','result::unwrap','worker exited -')):return 'timeout_or_native_crash'
    if any(x in text for x in ('vail.ir','vail.backend.','vail.utils.java','hail scala/backend','backenddependency','_jvm','py4j','pyspark','_with_flags','_get_flags','get_flags','_set_flags','requires_lowering')):return 'hail_ir_jvm_or_backend_internals'
    if any(x in text for x in ('_vail_matrix.json','fastlanes.delta','unsupported matrix descriptor','no such file or directory','filenotfounderror','unsupported uri scheme','text io currently supports local paths','gs://','s3://')):return 'format_io_and_fixture_compatibility'
    if any(x in text for x in ('import_bgen','import_gen','import_plink','export_bgen','export_gen','export_plink','import_vcf','export_vcf','import_fam','import_bed','import_locus','import_matrix','export_elasticsearch','load_dataset')):return 'format_io_and_fixture_compatibility'
    if ('no module named' in text or 'cannot import name' in text) and not any(x in text for x in ('vail','hailtop','hail.')):return 'optional_test_dependencies'
    if any(x in text for x in ('temporaryfilename','temporarydirectory','hailusererror','fatalerror','new_temp_file','new_local_temp_file','new_local_temp_dir','get_dataset','resource_dir','_values_similar')):return 'harness_or_fixture_setup'
    if 'set/dictionary/ndarray' in text:return 'missing_expression_or_table_api'
    if any(x in text for x in ('blockmatrix','ndarray','vail.nd','vail.linalg','matmul','linalg')) or (module.startswith('linalg/') and kind in ('AttributeError','ImportError','ModuleNotFoundError','NotImplementedError')):return 'missing_matrix_numerical_api'
    if any(x in text for x in ('regression','pc_relate','pca','skat','identity_by_descent','impute_sex','mendel','de_novo','sample_qc','concordance','maximal_independent_set','split_multi','filter_alleles','ld_prune','ld_matrix','pedigree','trio','vep','nirvana','lambda_gc','genetic_relatedness','realized_relationship','hwe_normalized','linear_mixed','full_outer_join','plot','visualize','ggplot','experimental','vds','methods')):return 'missing_high_level_api'
    if kind=='AssertionError' or any(x in text for x in ('assertionerror','did not raise','regex pattern did not match')):return 'behavior_or_assertion_mismatch'
    if missing_import:
        if missing_import.startswith(('hail.ir','hail.backend','hail.utils.java','hailtop')):return 'hail_ir_jvm_or_backend_internals'
        if any(x in missing_import for x in ('.methods','.experimental','.vds','.plot','.ggplot')):return 'missing_high_level_api'
        if any(x in missing_import for x in ('.nd','.linalg')):return 'missing_matrix_numerical_api'
        return 'missing_expression_or_table_api'
    if phase in ('fixture','setup_or_parameters','setup','teardown') and kind in ('NameError','KeyError'):return 'harness_or_fixture_setup'
    if kind=='NotImplementedError' and not any(x in text for x in ('per-list','aggregate initialization','group-dependent','correlated')):return 'missing_expression_or_table_api'
    if any(x in text for x in ('invalid tableir','datafusion','schema error','planning error','aggregation expressions require','correlated','logical plan','physical plan','per-list','aggregate initialization','group-dependent','tableir','unnest')):return 'planning_or_execution'
    if any(x in text for x in ('cannot infer','incompatible types','invalid expression','scope violation','source mismatch','typeerror','expected type','type mismatch','unexpected keyword','positional argument','requires a','must be','cannot cast')) or kind in ('TypeError','ValueError','ExpressionException','KeyError'):return 'expression_typing_scope_or_validation'
    if kind in ('AttributeError','ImportError','ModuleNotFoundError','NotImplementedError') or any(x in text for x in ('has no attribute','not implemented','unsupported','cannot import name','no module named')):return 'missing_expression_or_table_api'
    if kind=='NameError':return 'harness_or_fixture_setup'
    return 'planning_or_execution'


def key(module,nodeid):
    parts=nodeid.split('::')[1:]
    if not parts:return None
    parts[-1]=parts[-1].split('[')[0]
    return module,'.'.join(parts)


def main():
    inventory=json.loads((ROOT/'inventory.json').read_text());known={(r['module'],r['name']) for r in inventory}
    imports={}
    for module in {r['module'] for r in inventory}:
        imports[module]={a.asname or a.name:(n.module or '')+'.'+a.name for n in ast.parse((SOURCE/module).read_text()).body if isinstance(n,ast.ImportFrom) for a in n.names}
    groups=defaultdict(list);collection=[];timeouts=[];collected=set()
    modules=json.loads((ROOT/'pytest-modules.json').read_text())
    for result in modules:
        module=result['module']
        for attempt in result.get('attempts',[]):
            for path in (ROOT/attempt['directory']).glob('*.jsonl'):
                for line in path.read_text().splitlines():
                    event=json.loads(line)
                    if event['kind']=='test':groups[(module,event['nodeid'])].append(event)
                    elif event['kind']=='collection':collection.append(dict(module=module,error=short(event['error']),full_error=event['error']))
                    elif event['kind']=='inventory':collected.update((module,n) for n in event['items'])
            if attempt.get('timeout'):timeouts.append(dict(module=module,nodeid=attempt.get('nodeid'),error=f"External {attempt['timeout_scope']} timeout after {attempt['elapsed_seconds']:.1f}s (limit {attempt['declared_limit_seconds']}s)"))
    cases=[]
    for (module,nodeid),events in groups.items():
        failed=next((e for e in events if e['outcome']=='failed'),None);call=next((e for e in events if e['when']=='call'),None)
        status=('body_failed' if failed['when']=='call' else failed['when']+'_failed') if failed else call['outcome'] if call else 'skipped' if any(e['outcome']=='skipped' for e in events) else 'incomplete'
        selected=failed or call or events[-1];full=selected.get('error','');message=short(full);k=key(module,nodeid);kind=message.split(':',1)[0] if ':' in message else ''
        if re.search(r'^E\s+(?:assert\b|AssertionError|.*DID NOT RAISE|Use -v to get more diff)',full,re.M):kind='AssertionError'
        if kind=='AssertionError':message='; '.join(re.findall(r'^E\s+(.*)',full,re.M))[:1000]
        cases.append(dict(module=module,nodeid=nodeid,definition=k[1] if k else None,source_definition=k in known,status=status,error=message,error_type=kind,phase=selected.get('when'),category=classify(message,kind,selected.get('when',''),module) if status not in ('passed','skipped') else status))
    for t in timeouts:
        k=key(t['module'],t['nodeid']) if t['nodeid'] else None
        prior=next((r for r in cases if r['module']==t['module'] and r['nodeid']==t['nodeid']),None)
        if prior:prior.update(status='timeout',error=t['error'],category='timeout_or_native_crash')
        else:cases.append(dict(**t,definition=k[1] if k else None,source_definition=k in known,status='timeout',category='timeout_or_native_crash'))
    seen={(r['module'],r['nodeid']) for r in cases}
    for module,nodeid in collected-seen:
        k=key(module,nodeid);cases.append(dict(module=module,nodeid=nodeid,definition=k[1] if k else None,source_definition=k in known,status='not_executed',category='not_executed',error='Collected but no test report'))
    by_definition=defaultdict(list)
    for row in cases:
        if row['source_definition']:by_definition[(row['module'],row['definition'])].append(row)
    supplement={}
    if (ROOT/'supplement-results.json').exists():supplement={(r['module'],r['name']):r for r in json.loads((ROOT/'supplement-results.json').read_text())}
    rows=[]
    for definition in inventory:
        k=definition['module'],definition['name'];row=dict(definition);pc=by_definition.get(k,[]);body=supplement.get(k)
        row['pytest_cases']=dict(Counter(c['status'] for c in pc));blocked=next((c for c in collection if c['module']==k[0]),None)
        if blocked:row['collection_error']=blocked['error']
        if pc:
            bad=[c for c in pc if c['status'] not in ('passed','skipped')]
            if bad:row.update(status='pytest_nonpass',mode='pytest',category=bad[0]['category'],error=bad[0]['error'],example=bad[0]['nodeid'])
            elif any(c['status']=='skipped' for c in pc):row.update(status='pytest_skipped_or_partial',category='skipped',mode='pytest',error='One or more parameter cases skipped')
            else:row.update(status='pytest_passed',category='passed',mode='pytest',error='')
        elif body:
            variants=body.get('variants',[]);row.update(supplement_status=body.get('status'),supplement_variants=dict(Counter(v['status'] for v in variants)))
            if body.get('status')=='passed':row.update(status='body_passed_pytest_blocked',mode='isolated_body',category='passed_body_only',error=blocked['error'] if blocked else '')
            else:
                bad=next((v for v in variants if v['status']!='passed'),body);error=bad.get('error',body.get('worker_error',body.get('error','')));kind=bad.get('error_type','');phase=bad.get('phase',body.get('phase',''))
                match=re.search(r"name '([^']+)' is not defined",error);missing=imports[k[0]].get(match[1],'') if match else ''
                row.update(status='body_nonpass_pytest_blocked',mode='isolated_body',category=classify(error,kind,phase,k[0],missing),error=error,error_type=kind,phase=phase)
                if missing:row['missing_import']=missing
        else:row.update(status='collection_blocked' if blocked else 'not_attempted',category=classify(blocked['error'],phase='collection',module=k[0]) if blocked else 'not_attempted',error=blocked['error'] if blocked else '',mode='pytest_collection')
        rows.append(row)
    valid=[c for c in cases if c['source_definition']];incidental=[c for c in cases if not c['source_definition']]
    report=dict(provenance=json.loads((ROOT/'provenance.json').read_text()),modules_attempted=len(modules),collection_errors=collection,pytest_cases=dict(Counter(c['status'] for c in valid)),pytest_raw_cases=dict(Counter(c['status'] for c in cases)),incidental_imported_cases=len(incidental),definition_statuses=dict(Counter(r['status'] for r in rows)),definition_categories=dict(Counter(r['category'] for r in rows)),failing_case_categories=dict(Counter(c['category'] for c in valid if c['status'] not in ('passed','skipped'))),supplement_definitions=len(supplement),supplement_statuses=dict(Counter(r.get('status') for r in supplement.values())),timeouts=timeouts,definitions=rows,cases=cases)
    save(ROOT/'categorized.json',report)
    clusters=defaultdict(list)
    for c in valid:
        if c['status'] not in ('passed','skipped'):clusters[c['error']].append(c)
    save(ROOT/'error-clusters.json',[dict(error=e,cases=len(cs),category=cs[0]['category'],examples=[c['module']+'::'+c['nodeid'] for c in cs[:5]]) for e,cs in sorted(clusters.items(),key=lambda x:-len(x[1]))])
    print(json.dumps({k:v for k,v in report.items() if k not in ('definitions','cases','collection_errors')},indent=2))

if __name__=='__main__':main()
