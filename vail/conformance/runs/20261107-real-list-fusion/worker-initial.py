import os,sys,ast,copy,json,time,resource,threading,hashlib
from pathlib import Path
root=Path('/private/tmp/vail-real-fusion-bench-20261107');out=Path(sys.argv[1]);case=sys.argv[2];out.mkdir(parents=True,exist_ok=True)
def guard():
 while True:
  if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>6*1024**3:os._exit(91)
  time.sleep(.1)
threading.Thread(target=guard,daemon=True).start()
import vail as hl
import vail._native as native
import pyarrow as pa
source=Path('/private/tmp/vail-full-benchmark-20261106/protocol/upstream')
module='benchmark_methods.py' if case=='test_split_multi_hts' else 'benchmark_matrix_table.py'
source=source/module
fn=next(node for node in ast.parse(source.read_text()).body if isinstance(node,ast.FunctionDef) and node.name==case)
fn.decorator_list=[]
validation=copy.deepcopy(fn);validation.name='validation_query'
last=validation.body[-1];assert isinstance(last,ast.Expr) and isinstance(last.value,ast.Call) and isinstance(last.value.func,ast.Attribute) and last.value.func.attr=='_force_count_rows'
validation.body[-1]=ast.Return(value=last.value.func.value)
module_ast=ast.Module(body=[fn,validation],type_ignores=[]);ast.fix_missing_locations(module_ast)
ns={'hl':hl};exec(compile(module_ast,str(source),'exec'),ns)
fixture='/private/tmp/vail-generic-list-20261104/inputs-1000/first-1000.mt'
meta={'case':case,'mode':os.environ['RUN_MODE'],'native':native.__file__,'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),'frontend':hl.__file__,'source_file':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'original_function':ast.unparse(fn),'fixture':fixture,'thread_env':{k:os.environ[k] for k in ['RAYON_NUM_THREADS','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS']}}
(out/'identity.json').write_text(json.dumps(meta,indent=2))
q=ns['validation_query'](fixture)
# Keep plans as diagnostics, not as performance proof.
(out/'logical-plan.txt').write_text(q._mir.core._native.logical_plan())
(out/'physical-plan.txt').write_text(q._mir.core._native.physical_plan())
actual=q.localize_entries('entries','columns').to_arrow()
assert actual.num_rows==1000 if case!='test_split_multi_hts' else actual.num_rows>0
reference=root/'references'/f'{case}.arrow';reference.parent.mkdir(exist_ok=True)
if os.environ.get('ENROLL_REFERENCE')=='1':
 assert not reference.exists()
 with pa.OSFile(str(reference),'wb') as f:
  with pa.ipc.new_file(f,actual.schema) as w:w.write_table(actual)
else:
 with pa.memory_map(str(reference),'r') as f:expected=pa.ipc.open_file(f).read_all()
 assert actual.equals(expected,check_metadata=True),'Full output differs from the pre-change build'
 del expected
if case=='test_split_multi_hts':
 with pa.memory_map('/private/tmp/vail-generic-list-20261104/inputs-1000/reference.arrow','r') as f:oracle=pa.ipc.open_file(f).read_all()
 assert actual.equals(oracle,check_metadata=True),'Split output differs from established full reference'
 del oracle
meta.update(validated_rows=actual.num_rows,validated_bytes=actual.nbytes,reference_sha256=hashlib.sha256(reference.read_bytes()).hexdigest(),result_evidence='complete Arrow values+metadata equality against pre-change output; split also established external reference')
del actual,q
# Timed function is the ORIGINAL upstream body, including read/construct/force-count.
for _ in range(2):assert ns[case](fixture) is None
samples=[]
for i in range(5):
 c=time.process_time();t=time.perf_counter();result=ns[case](fixture);wall=time.perf_counter()-t;cpu=time.process_time()-c
 assert result is None
 samples.append({'sample':i,'wall_seconds':wall,'cpu_seconds':cpu,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
meta.update(samples=samples,validated=True)
(out/'result.json').write_text(json.dumps(meta,indent=2));print(json.dumps({'case':case,'mode':meta['mode'],'validated':True,'samples':samples}),flush=True)
