from pathlib import Path
import json,hashlib,math,statistics,collections,tarfile,ast,shutil,zipfile
src=Path('/private/tmp/vail-cloud-scan-20261023');root=src/'final/results';out=Path('vail/conformance/runs/20261027-supported-scan-recovery')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
for rel in ['scan-provenance.json','unsupported-evidence-audit.json','cloud-scan-final-results.tar.gz']:
 shutil.copy2(src/rel,out/'original'/Path(rel).name)
for rel in ['machine-provenance.json','upstream/results.json','upstream/inventory.json','upstream/skip-policy.json','index-repair/results.json','index-repair/supersedes.json','upstream-summary.json','fixture-links.txt']:
 p=root/rel;target=out/'original'/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
primary=json.loads((root/'upstream/results.json').read_text());repair=json.loads((root/'index-repair/results.json').read_text());lookup={}
for prefix,report in [('upstream',primary),('index-repair',repair)]:
 for row in report['results']:
  key=(row['case']['id'],row['mode'],row['repetition']);label=row['case']['id'].replace('::','--').replace('/','_');path=root/prefix/f"rep-{row['repetition']:02d}"/row['mode']/(label+'.json')
  assert json.loads(path.read_text())==row
  lookup[key]=(row,str(path.relative_to(root)),sha(path))
for folder in ['harness','index-repair-harness']:
 for name in ['run_scan.py','upstream_benchmark_coverage.py','benchmark_telemetry.py','profile_annotate.py']:
  p=src/folder/name
  if p.exists():target=out/'runner'/folder/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
for name,h in primary['metadata']['runner_sha256'].items():assert sha(src/'harness'/name)==h
archive=Path('/private/tmp/vail-cloud-upstream-e10cc355.tar.gz');sources={}
with tarfile.open(archive) as t:
 for member in t.getmembers():
  name=Path(member.name).name
  if name in primary['metadata']['upstream_sha256'] and '/benchmark/hail/' in member.name:
   data=t.extractfile(member).read();assert hashlib.sha256(data).hexdigest()==primary['metadata']['upstream_sha256'][name];(out/'upstream'/name).write_bytes(data);sources[name]=ast.parse(data)
policy_path=Path('vail/benchmarks/known_unsupported_benchmarks.json');policy=json.loads(policy_path.read_text());shutil.copy2(policy_path,out/'original/current-policy.json')
groups=collections.defaultdict(list)
for (case,mode,rep),value in lookup.items():groups[(case,mode)].append(value)
def passes(values):
 return len(values)==3 and {v[0]['repetition'] for v in values}=={1,2,3} and all(r.get('actual_sample') is True and r['status']=='passed' and math.isfinite(r.get('benchmark_seconds',-1)) and r.get('benchmark_seconds',-1)>0 and math.isfinite(r.get('telemetry',{}).get('cpu_seconds',-1)) and r.get('telemetry',{}).get('cpu_seconds',-1)>0 for r,_,_ in values)
summary={ (r['case'],r['mode']):r for r in json.loads((root/'upstream-summary.json').read_text())['rows']}
selected=[];omitted=[]
ids=sorted({k[0] for k in groups})
for cid in ids:
 modes=[m for m in ['hail-hail','vail-hail','vail-vail'] if passes(groups[(cid,m)])]
 if cid in policy['cases'] or not {'hail-hail','vail-vail'}<=set(modes):
  omitted.append(dict(case_id=cid,reason='versioned_policy_exclusion' if cid in policy['cases'] else 'no_three_actual_successful_native_Vail_and_Hail_pairs',positive_modes=modes));continue
 case=groups[(cid,'vail-vail')][0][0]['case'];node=next(n for n in sources[case['module']].body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==case['name'])
 assertions=sum(isinstance(n,ast.Assert) for n in ast.walk(node));returns=sum(isinstance(n,ast.Return) and n.value is not None for n in ast.walk(node));app=summary[(cid,'vail-vail')]['format_applicability'];entries={}
 for mode in modes:
  vals=sorted(groups[(cid,mode)],key=lambda x:x[0]['repetition']);entries[mode]=dict(receipts=[dict(path=p,sha256=h,repetition=r['repetition'],wall_seconds=r['benchmark_seconds'],cpu_seconds=r['telemetry']['cpu_seconds'],elapsed_seconds=r.get('elapsed_seconds'),fixture_seconds=r.get('fixture_seconds')) for r,p,h in vals],wall_median=statistics.median(r['benchmark_seconds'] for r,_,_ in vals),cpu_median=statistics.median(r['telemetry']['cpu_seconds'] for r,_,_ in vals),elapsed_median=statistics.median(r.get('elapsed_seconds',r['benchmark_seconds']) for r,_,_ in vals))
 selected.append(dict(case_id=cid,case=case,format_applicability=app,positive_modes=modes,historical=entries,fixture_manifest_key=hashlib.sha256(json.dumps([sorted(case['fixtures']),case['fixture_parameters']],sort_keys=True).encode()).hexdigest(),validator=dict(kind='upstream_inline_assertions_present' if assertions else 'upstream_execution_only_no_local_assertion',assert_nodes=assertions,return_nodes=returns,worker_discarded_return=True,full_cross_engine_output_equality='NOT established by passed receipt; pending validator review'),execution_ready=False,pending=['current_Vail_correctness_preflight','input_content_enrollment','baseline/current/Hail artifact runtime verification','validator_scope_review']))
assert len(selected)==112
meta=dict(source_revision=primary['metadata']['source_revision'],policy_sha256=sha(policy_path),policy_version=policy['policy_version'],policy_exclusions=len(policy['cases']),supported_native_Hail_cases=len(selected),supported_all_three=sum('vail-hail' in r['positive_modes'] for r in selected),storage_native=sum(r['format_applicability']=='shared-table-matrix-fixture' for r in selected),format_independent=sum(r['format_applicability']=='format-independent' for r in selected),support_rule='three ACTUAL finite positive wall+CPU passed samples each; index-repair supersedes exactly9receipt slots; policy subtraction BOTHengines',baseline_provenance=json.loads((root/'machine-provenance.json').read_text()),execution_authorized=False)
(out/'positive-support-pool.json').write_text(json.dumps(dict(metadata=meta,cases=selected,omitted=omitted),indent=2)+'\n')
# Existing supplemental85jobs remain distinct suite, preserving originals and runner.
shutil.copy2(src/'completed-supplemental/results.json',out/'original/supplemental-results.json')
for name in ['run_supplemental.py','compare_hail.py','legacy_scale_fixtures.py','benchmark_telemetry.py','run_guards.py']:
 shutil.copy2(src/'supplemental'/name,out/'runner'/name)
wheel=next((src/'wheels').glob('*.whl'));wheel_record=dict(path=str(wheel),bytes=wheel.stat().st_size,sha256=sha(wheel),members={})
with zipfile.ZipFile(wheel) as z:
 for name in z.namelist():
  if name.endswith('_native.abi3.so') or (name.startswith('vail/') and name.endswith('.py')) or name.endswith('.dist-info/METADATA'):
   with z.open(name) as f:wheel_record['members'][name]=hashlib.file_digest(f,'sha256').hexdigest()
(out/'baseline-wheel-lock.json').write_text(json.dumps(wheel_record,indent=2)+'\n')
budgets={}
for mode in ['hail-hail','vail-vail','vail-hail']:
 rows=[r for r in selected if mode in r['historical'] and (mode!='vail-hail' or r['format_applicability']!='format-independent')]
 budgets[mode]=dict(cases=len(rows),sum_case_median_query_seconds=sum(r['historical'][mode]['wall_median'] for r in rows),sum_case_median_cpu_seconds=sum(r['historical'][mode]['cpu_median'] for r in rows),sum_case_median_worker_elapsed_seconds=sum(r['historical'][mode]['elapsed_median'] for r in rows))
(out/'historical-budget-inputs.json').write_text(json.dumps(dict(scope='upstream only; format-independent Vail measured once, not in duplicate storage mode; historical medians not current ETA',modes=budgets),indent=2)+'\n')
print(meta['supported_native_Hail_cases'],meta['supported_all_three'],collections.Counter(r['validator']['kind'] for r in selected));print(budgets);print('wheel_native',[(k,v) for k,v in wheel_record['members'].items() if k.endswith('.so')])
