from pathlib import Path
import json,collections,hashlib,subprocess
root=Path('/Users/danielking/.codex/worktrees/1611/vail');runs=root/'vail/conformance/runs';p=runs/'20261014-upstream-entire-pytest';b=runs/'20261014-upstream-entire-bodies';out=runs/'20261014-upstream-entire-report';out.mkdir(exist_ok=True)
mods=json.loads((p/'modules.json').read_text());counts=collections.Counter();events_by_def=collections.defaultdict(list);errors=[]
for f in sorted(p.glob('*.jsonl')):
 module=f.name[:-6].replace('__','/')
 for line in f.read_text().splitlines():
  e=json.loads(line)
  if e['kind']=='inventory':counts['collected_test_cases']+=len(e['items'])
  if e['kind']=='collection':counts['collection_errors']+=1;errors.append(dict(module=module,**e))
  if e['kind']=='test':
   if e['when']=='call' or e['outcome']!='passed':counts[e['when']+'_'+e['outcome']]+=1
   parts=e['nodeid'].split('::')[1:]
   if parts:
    parts[-1]=parts[-1].split('[')[0];events_by_def[(module,'.'.join(parts))].append(e)
body=json.loads((b/'results-parallel.json').read_text())
if (b/'async-fs.json').exists():body.append(json.loads((b/'async-fs.json').read_text()))
by_def={(r['module'],r['name']):r for r in body};body_counts=collections.Counter(r.get('status') for r in body)
selection=json.loads((runs/'20261014-randomness-full/upstream-inventory.json').read_text());random_rows=[];rc=collections.Counter()
for s in selection:
 key=(s['module'],s['name']);events=events_by_def.get(key,[]);calls=[e for e in events if e['when']=='call'];bad=[e for e in events if e['outcome']=='failed']
 if bad:status='failed'
 elif calls and all(e['outcome']=='passed' for e in calls):status='passed_observed_cases'
 elif events:status='skipped_or_incomplete'
 else:status='not_executed_by_pytest'
 rc[status]+=1
 r=by_def.get(key)
 random_rows.append(dict(**s,pytest_status=status,pytest_call_cases=len(calls),pytest_errors=[{'when':e['when'],'error':e['error']} for e in bad],body_result=r))
summary={'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'branch':subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip(),'scope':'All 62 test_*.py modules under pinned hail/hail/python/test/hail, including backend modules; Python API tests, not Hail Scala or hailtop suites. Imports redirected to Vail; init, backend flags, and snapshot fixtures adapted.','modules_attempted':len(mods),'module_timeouts':[m for m in mods if m.get('timeout')],'pytest':dict(counts),'body_definitions_attempted':len(body),'body_definition_status':dict(body_counts),'randomness_selection_definitions':len(selection),'randomness_pytest_status':dict(rc),'randomness_body_status':dict(collections.Counter(r['body_result'].get('status') if r['body_result'] else 'pending' for r in random_rows)),'extension_sha256':hashlib.sha256((root/'vail/python/vail/_native.abi3.so').read_bytes()).hexdigest(),'limitations':['Collection failures and timeouts are not passes.','Per-test timeout 60 seconds, module timeout 600 seconds for pytest.','Supplemental bodies run in isolated subprocesses with a 45-second limit per definition; decorators, imports, fixtures and initialization adapted. These are separate from normal pytest results.','The 131-definition randomness selection uses direct rand_*, shuffle and sampling calls and same-module callers; it does not prove identification of every indirect randomness consumer.','Pass counts for observed pytest cases do not establish all variants executed when a module times out.']}
for name,value in [('summary.json',summary),('randomness-results.json',random_rows),('collection-errors.json',errors)]: (out/name).write_text(json.dumps(value,indent=2)+'\n')
print(json.dumps(summary,indent=2))
