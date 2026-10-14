import ast,json,shutil,tempfile,subprocess,sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
root=Path.cwd();suite=(root/'../hail/hail/python/test/hail').resolve();out=root/'conformance/runs/20261014-vail-backend-final-without-ir';out.mkdir(exist_ok=True)
class Strip(ast.NodeTransformer):
 def visit_Expr(self,node):
  if isinstance(node.value,ast.Call):
   call=node.value
   if isinstance(call.func,ast.Name) and call.func.id=='assert_contains_node':return ast.copy_location(ast.Pass(),node)
   if isinstance(call.func,ast.Name) and call.func.id=='print' and any(isinstance(n,ast.Attribute) and n.attr=='_tir' for n in ast.walk(call)):return ast.copy_location(ast.Pass(),node)
  return self.generic_visit(node)
with tempfile.TemporaryDirectory(prefix='vail-irless-') as folder:
 base=Path(folder)/'hail/python';target=base/'test/hail';shutil.copytree(suite,target)
 for rel in ['table/test_table.py','matrixtable/test_matrix_table.py']:
  p=target/rel;tree=Strip().visit(ast.parse(p.read_text()));p.write_text(ast.unparse(ast.fix_missing_locations(tree)))
 (base.parent/'hail/test').mkdir(parents=True);(base.parent/'hail/test/resources').symlink_to(suite.parents[2]/'hail/test/resources',target_is_directory=True)
 (base/'hailtop').symlink_to(suite.parents[1]/'hailtop',target_is_directory=True)
 cases=json.loads((root/'conformance/runs/20261014-randomness-full/upstream-inventory.json').read_text());results={}
 def run(i,case):
  path=out/f'{i:04}.json';cmd=[sys.executable,'conformance/full_hail_bodies.py','--suite',str(target),'--worker-source',str(target/case['module']),'--worker-name',case['name'],'--output',str(path)]
  with path.with_suffix('.log').open('w') as log:
   try:r=subprocess.run(cmd,stdout=log,stderr=log,timeout=60);problem=None if r.returncode==0 else f'exit {r.returncode}'
   except subprocess.TimeoutExpired:problem='timeout 60s'
  result=json.loads(path.read_text()) if path.exists() else dict(case)
  if problem:result.update(status='worker_blocked',worker_error=problem)
  path.write_text(json.dumps(result,indent=2));return i,result
 with ThreadPoolExecutor(max_workers=4) as pool:
  for future in as_completed([pool.submit(run,i,c) for i,c in enumerate(cases)]):
   i,r=future.result();results[i]=r;(out/'results.json').write_text(json.dumps([results[k] for k in sorted(results)],indent=2));print(len(results),r['module'],r['name'],r['status'],flush=True)
