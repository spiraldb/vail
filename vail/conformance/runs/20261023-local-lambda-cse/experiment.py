"""Exact upstream query; run serially in baseline and candidate normal envs."""
import ast,hashlib,json,resource,time
from pathlib import Path
import vail as hl
import vail._native as native
source=Path('/Users/danielking/projects/vail/hail/hail/python/benchmark/hail/benchmark_matrix_table.py')
node=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='many_aggs')
namespace={'hl':hl}
exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),namespace)
matrix=hl.read_matrix_table('/tmp/entries-profile25.mt')
matrix=matrix.annotate_cols(**namespace['many_aggs'](matrix))
table=matrix.cols()
before=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter()
count=table._force_count()
wall=time.perf_counter()-start;after=resource.getrusage(resource.RUSAGE_SELF)
assert count==2535
print(json.dumps({'wall':wall,'core_seconds':after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,
 'peak_rss_bytes':after.ru_maxrss,'count':count,'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()}))
