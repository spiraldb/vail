from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,os,sys,time,psutil
r=Path(__file__).parent;mode,case,outpath=sys.argv[1:];out=Path(outpath);cfg=json.loads((r/'bench-config.json').read_text());pins=cfg['pins'];spec=cfg['cases'][case];fixture=pins['fixtures'][spec['fixture']]['root']
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
meta={'mode':mode,'case':case,'fixture':fixture,'python':sys.version,'cpu_scope':'Python process plus stable observed child endpoint CPU; may omit unobserved short-lived children'}
if mode=='hail':
 import hail as hl,pyspark
 assert hl.__version__==pins['hail_version'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 hl.init(master='local[4]',quiet=True,log=str(out/'hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.default.parallelism':'4','spark.sql.shuffle.partitions':'4','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
 meta.update(hail_version=hl.__version__,jar_sha256=sha(pins['hail_jar']))
else:
 import vail as hl,vail._native as native
 assert sha(native.__file__)==cfg[mode]['native_sha256'];meta.update(native_sha256=sha(native.__file__),allocator=native._benchmark_allocator)
api=SimpleNamespace(**vars(hl))
if mode!='hail' and spec['reader']=='legacy':api.read_matrix_table=hl.MatrixTable.read_legacy
source=Path('/private/tmp/vail-hail-slow-three-20261110/benchmark_table.py');assert sha(source)==pins['source_sha256']['benchmark_table.py']
if spec['function']=='collect_duplicates':
 def query(path):
  ht=api.read_matrix_table(path).localize_entries('e','c')
  ht.group_by(k=hl.int(ht.row_idx / 50)).aggregate(value=hl.agg.collect(ht.row_value))._force_count()
 meta['workload']='duplicate-key collect control: original group-by-take key expression with collect payload'
else:
 fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==spec['function']);fn.decorator_list=[];module=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(module);ns={'hl':api};exec(compile(module,str(source),'exec'),ns);query=ns[spec['function']];meta['source']=ast.unparse(fn)
def snapshot():
 result={}
 for q in psutil.Process().children(recursive=True):
  try:c=q.cpu_times();result[f'{q.pid}:{q.create_time()}']=c.user+c.system
  except psutil.NoSuchProcess:pass
 return result
try:
 assert query(fixture) is None
 known=set(snapshot());samples=[]
 for i in range(3):
  before=snapshot();cpu=time.process_time();start=time.perf_counter();assert query(fixture) is None;wall=time.perf_counter()-start;cpu=time.process_time()-cpu;after=snapshot();assert known==set(before)==set(after)
  child=sum(after[k]-before[k] for k in after);assert child>=0
  samples.append({'wall_seconds':wall,'cpu_seconds':cpu+child,'python_cpu_seconds':cpu,'observed_child_cpu_seconds':child,'threads_after_query':psutil.Process().num_threads(),'before':before,'after':after});print('SAMPLE',i,wall,cpu+child,flush=True)
 meta.update(samples=samples,passed=True)
finally:
 if mode=='hail':hl.stop()
(out/'result.json').write_text(json.dumps(meta,indent=2)+'\n')
