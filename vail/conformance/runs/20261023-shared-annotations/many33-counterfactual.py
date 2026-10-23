"""Exact-query source counterfactual; no production switches/native copying."""
import ast,argparse,hashlib,json,resource,subprocess,time
from pathlib import Path
import vail as hl
import vail.matrixtable as module
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('--mode',choices=['before','after'],required=True);args=p.parse_args()
if args.mode=='before':
 source=subprocess.check_output(['git','show','3dfd7318:vail/python/vail/matrixtable.py'],cwd='/Users/danielking/.codex/worktrees/shared-annotation-aggregates/vail',text=True)
 klass=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='MatrixTable')
 method=next(n for n in klass.body if isinstance(n,ast.FunctionDef) and n.name=='_project_axis')
 scope=dict(vars(module));exec(compile(ast.Module(body=[method],type_ignores=[]),'baseline_project_axis.py','exec'),scope)
 module.MatrixTable._project_axis=scope['_project_axis']
source=Path('/Users/danielking/projects/vail/hail/hail/python/benchmark/hail/benchmark_matrix_table.py').read_text()
node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='many_aggs')
ns={'hl':hl};exec(compile(ast.Module(body=[node],type_ignores=[]),'upstream_many_aggs.py','exec'),ns)
m=hl.read_matrix_table('/tmp/entries-profile25.mt');m=m.annotate_cols(**ns['many_aggs'](m));t=m.cols()
start=time.perf_counter();r=resource.getrusage(resource.RUSAGE_SELF);count=t._force_count();z=resource.getrusage(resource.RUSAGE_SELF)
assert count==2535
print(json.dumps({'mode':args.mode,'wall':time.perf_counter()-start,'core_seconds':z.ru_utime+z.ru_stime-r.ru_utime-r.ru_stime,'peak_rss_bytes':z.ru_maxrss,'count':count,'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()}))
