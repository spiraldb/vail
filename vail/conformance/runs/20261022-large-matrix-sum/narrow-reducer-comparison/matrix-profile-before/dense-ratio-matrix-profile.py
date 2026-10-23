import ast,time,json,resource
import vail as hl
source=open('/tmp/local-dense-reducers-paired-20261022/driver.py').read()
node=next(x for x in ast.parse(source).body if isinstance(x,ast.FunctionDef) and x.name=='query')
exec(compile(ast.Module(body=[node],type_ignores=[]),'<query>','exec'))
start=time.perf_counter();before=resource.getrusage(resource.RUSAGE_SELF)
result=query('representative_matrix_coordinate_reducers')
after=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps(dict(result=result,wall=time.perf_counter()-start,core=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime)))
