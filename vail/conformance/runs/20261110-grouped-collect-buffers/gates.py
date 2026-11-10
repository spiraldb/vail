from pathlib import Path
import json,subprocess,os,time
r=Path(__file__).parent
assert json.loads((r/'rust-final.json').read_text())['returncode']==0
ready=json.loads((r/'ready.json').read_text());e=os.environ.copy();e.update(RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1');e.pop('PYTHONPATH',None)
tests=['test_grouped_collect_nested.py','test_grouped_aggregate.py','test_grouped_aggregate_execution.py','test_aggregate_execution.py','test_aggregate_initialization_lazy.py','test_aggregate_global_initialization.py','test_aggregate_nested_projection.py']
cmd=['nice','-n','10',ready['python'],'-m','pytest','-q',*[f'python/tests/{x}'for x in tests]]
t=time.monotonic()
with (r/'python-gates.log').open('w') as f:p=subprocess.run(cmd,cwd='/private/tmp/vail-list-flatmap-20261106/vail',env=e,stdout=f,stderr=subprocess.STDOUT,timeout=600)
(r/'python-gates.json').write_text(json.dumps({'command':cmd,'returncode':p.returncode,'seconds':time.monotonic()-t,'source':ready['source'],'native':ready['identity']},indent=2)+'\n');print('PYTHON GATES',p.returncode,flush=True);assert p.returncode==0
