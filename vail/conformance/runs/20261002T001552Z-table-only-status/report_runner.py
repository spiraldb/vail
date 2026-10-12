import ast, concurrent.futures, datetime, hashlib, json, os, subprocess, sys, time
from pathlib import Path
root=Path.cwd();source=root.parent/'hail/hail/python/test/hail/table/test_table.py';runner=root/'conformance/all_upstream_table_cases.py'
import importlib.util
spec=importlib.util.spec_from_file_location('runner',runner);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
inv=json.loads((root/'conformance/hail-test-inventory.json').read_text())['tests'];wanted={r['qualified_name'].replace('::','.') for r in inv if r['source'].endswith('table/test_table.py') and r['reviewed_scope']=='table_only'}
tests=[(name,node.lineno,source) for name,node in mod.definitions(ast.parse(source.read_text())) if name in wanted]
grouped=source.with_name('test_grouped_table.py')
tests.extend((name,node.lineno,grouped) for name,node in mod.definitions(ast.parse(grouped.read_text())))
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=root/'conformance/runs'/f'{stamp}-table-only-status';out.mkdir(parents=True);print(out,flush=True)
start=time.time()
def run(test):
 name,line,test_source=test;p=out/f'{name}.json'
 with (out/f'{name}.log').open('w') as log:
  try:
   result=subprocess.run([sys.executable,str(Path(__file__).with_name('body_worker.py')),str(test_source),name,str(p)],stdout=log,stderr=log,timeout=30);problem=None if result.returncode==0 else f'worker exit {result.returncode}'
  except subprocess.TimeoutExpired:problem='worker exceeded 30s'
 cases=json.loads(p.read_text()) if p.exists() else []
 if problem:cases.append({'status':'blocked','phase':'worker','error':problem})
 status='blocked' if not cases or any(c['status']=='blocked' for c in cases) else 'failed' if any(c['status']=='failed' for c in cases) else 'passed'
 return {'test_id':name,'source_line':line,'source':str(test_source),'status':status,'cases':cases}
results=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 for result in pool.map(run,tests):
  results.append(result)
  report={'source':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'mode':'all-original-table-only-bodies-against-vail','limitations':mod.__doc__,'definitions_summary':{s:sum(r['status']==s for r in results) for s in ['passed','failed','blocked']},'cases_summary':{s:sum(c['status']==s for r in results for c in r['cases']) for s in ['passed','failed','blocked']},'dedicated_table_only_inventory_total':191,'grouped_suite_definitions_unrun':0,'elapsed_seconds':time.time()-start,'results':results}
  (out/'results.json').write_text(json.dumps(report,indent=2)+'\n')
  print(f'{len(results)}/{len(tests)} {result["status"]} {result["test_id"]}',flush=True)
print(json.dumps(report['definitions_summary']),flush=True)
