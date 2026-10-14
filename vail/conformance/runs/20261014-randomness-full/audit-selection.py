import ast,json,subprocess,sys,os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
suite=Path('../hail/hail/python/test/hail').resolve();out=Path('/private/tmp/vail-randomness-upstream-audit');out.mkdir(exist_ok=True)
cases=[]
for source in sorted(suite.rglob('test_*.py')):
 tree=ast.parse(source.read_text());defs={}
 for n in tree.body:
  if isinstance(n,ast.FunctionDef):defs[n.name]=n
  elif isinstance(n,ast.ClassDef):
   for m in n.body:
    if isinstance(m,ast.FunctionDef):defs[n.name+'.'+m.name]=m
 random_names=set()
 for name,node in defs.items():
  if any(isinstance(n,ast.Attribute) and (n.attr.startswith('rand_') or n.attr in ('shuffle','sample_rows','sample_cols','sample')) for n in ast.walk(node)):random_names.add(name)
 changed=True
 while changed:
  changed=False
  for name,node in defs.items():
   if name in random_names:continue
   if any(isinstance(n,ast.Call) and ((isinstance(n.func,ast.Name) and n.func.id in random_names) or (isinstance(n.func,ast.Attribute) and (name.rsplit('.',1)[0]+'.'+n.func.attr) in random_names)) for n in ast.walk(node)):
    random_names.add(name);changed=True
 for name in sorted(random_names):
  if name.rsplit('.',1)[-1].startswith('test_'):cases.append((source,name))
(out/'inventory.json').write_text(json.dumps([{'module':str(s.relative_to(suite)),'name':n} for s,n in cases],indent=2))
print('Selected',len(cases),'definitions',flush=True)
def run(i,source,name):
 path=out/f'{i:04}.json'
 cmd=[sys.executable,'conformance/full_hail_bodies.py','--suite',str(suite),'--output',str(path),'--worker-source',str(source),'--worker-name',name]
 with path.with_suffix('.log').open('w') as log:
  try:r=subprocess.run(cmd,stdout=log,stderr=log,timeout=60);error=None if r.returncode==0 else f'exit {r.returncode}'
  except subprocess.TimeoutExpired:error='timeout 60s'
 result=json.loads(path.read_text()) if path.exists() else {'module':str(source.relative_to(suite)),'name':name}
 if error:result.update(status='worker_blocked',worker_error=error)
 path.write_text(json.dumps(result,indent=2));return i,result
results={}
with ThreadPoolExecutor(max_workers=3) as pool:
 for future in as_completed([pool.submit(run,i,s,n) for i,(s,n) in enumerate(cases)]):
  i,r=future.result();results[i]=r
  (out/'results.json').write_text(json.dumps([results[i] for i in sorted(results)],indent=2))
  print(len(results),'/',len(cases),r.get('status'),r['module'],r['name'],flush=True)
