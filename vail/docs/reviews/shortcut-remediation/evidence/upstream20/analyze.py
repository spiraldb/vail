import json,re,runpy,hashlib
from pathlib import Path
from collections import Counter,defaultdict
helpers=runpy.run_path('vail/conformance/summarize_full_hail.py')
root=Path('/private/tmp/vail-shortcuts-upstream20-full')
inv=json.loads((root/'definition-inventory.json').read_text());mods=json.loads((root/'modules.json').read_text());completed={m['module']:m for m in mods}
rows={(r['module'],r['name']):dict(module=r['module'],name=r['name'],role=helpers['randomness_role'](r['name'],r['body'],r['module']),status='unreached') for r in inv}
cases=[];errors=[]
for p in sorted(root.glob('*.jsonl')):
 module=p.name[:-6].replace('__','/');events=defaultdict(list);collected=[];started=set()
 for line in p.read_text().splitlines():
  try:e=json.loads(line)
  except json.JSONDecodeError:continue
  if e['kind']=='collection':errors.append(dict(module=module,error=e['error']))
  elif e['kind']=='inventory':collected+=e['items']
  elif e['kind']=='started':started.add(e['nodeid'])
  elif e['kind']=='test':events[e['nodeid']].append(e)
 for node in dict.fromkeys([*collected,*events]):
  ev=events[node];failure=next((e for e in ev if e['outcome']=='failed'),None);call=next((e for e in ev if e['when']=='call'),None);skip=next((e for e in ev if e['outcome']=='skipped'),None)
  mod=completed.get(module,{})
  status='failed' if failure else call['outcome'] if call else 'skipped' if skip else 'interrupted' if node in started else 'unreached'
  if module not in completed and status=='interrupted':status='running'
  if module not in completed and status=='unreached':status='pending_case'
  if mod.get('timeout') and mod.get('nodeid')==node:status='timeout'
  error=(failure or call or skip or {}).get('error','');type_match=re.search(r'^E\s+(\w+(?:Error|Exception)):',error,re.M)
  parts=node.split('::')[1:];parts[-1]=parts[-1].split('[')[0];key=(module,'.'.join(parts));role=rows.get(key,{}).get('role','unknown')
  diagnostic='\n'.join(line for line in error.splitlines() if re.match(r'^E\s',line)) or error
  error_type=type_match[1] if type_match else 'AssertionError' if re.search(r'^E\s+assert\s',error,re.M) else ''
  category=helpers['category'](diagnostic,error_type,(failure or {}).get('category','')) if status=='failed' else status
  if status=='failed' and error_type=='AssertionError' and re.search(r'assert[^\n]*\.n_partitions\(',error):category='intentional_partition_layout_difference'
  if status=='failed' and ('vail.ir.' in error or 'requires_lowering' in error):category='hail_backend_or_ir_dependency'
  c=dict(module=module,nodeid=node,name=key[1],role=role,status=status,phase=(failure or call or skip or {}).get('when'),duration_seconds=(failure or call or skip or {}).get('duration'),category=category,error=error);cases.append(c)
bydef=defaultdict(list)
for c in cases:bydef[c['module'],c['name']].append(c)
for key,row in rows.items():
 ev=bydef[key];counts=Counter(c['status'] for c in ev)
 if counts:
  status=next((s for s in ('timeout','failed','interrupted','running','unreached','pending_case','skipped','passed') if counts[s]),'unreached')
  row.update(status=status,cases=dict(counts))
  bad=next((c for c in ev if c['status'] not in ('passed','skipped')),None)
  if bad:row.update(category=bad['category'],error=bad['error'])
 elif any(e['module']==row['module'] for e in errors):row['status']='collection_blocked'
 elif row['module'] not in completed:row['status']='module_not_completed'
definition_categories=Counter(r.get('category',r['status']) for r in rows.values())
areas=defaultdict(Counter)
for r in rows.values():areas[r['module'].split('/')[0] if '/' in r['module'] else 'root API'][r['status']]+=1
report=dict(native_library_sha256=hashlib.sha256(Path('/private/tmp/vail-shortcuts-package19/vail/_native.abi3.so').read_bytes()).hexdigest(),structural_ir_adaptation=json.loads((root/'structural-ir-assertions.json').read_text()),scope_exclusions=json.loads((root/'scope-exclusions.json').read_text()),fixture_adaptations=json.loads((root/'fixture-adaptations.json').read_text()),failed_case_phases=dict(Counter(c['phase'] for c in cases if c['status']=='failed')),definition_categories=dict(definition_categories),areas={k:dict(v) for k,v in areas.items()},completed_modules=len(completed),eligible_modules=56,definitions=len(rows),definition_statuses=dict(Counter(r['status'] for r in rows.values())),parameter_cases=dict(Counter(c['status'] for c in cases)),failure_categories=dict(Counter(c['category'] for c in cases if c['status']=='failed')),collection_errors=errors,timeout_modules=[m for m in mods if m.get('timeout')],randomness={role:dict(definitions=dict(Counter(r['status'] for r in rows.values() if r['role']==role)),parameter_cases=dict(Counter(c['status'] for c in cases if c['role']==role))) for role in ('specifically_randomness','random_data_for_other_functionality')},results=list(rows.values()),cases=cases)
(root/'analysis-live.json').write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k not in ('results','cases','collection_errors','structural_ir_adaptation','scope_exclusions','fixture_adaptations')},indent=2));print('collection_errors:',len(errors))
