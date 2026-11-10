from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,math,os,sys,time
import psutil
r=Path(__file__).parent;mode,purpose,case,outpath=sys.argv[1:];out=Path(outpath);pins=json.loads((r/'pins.json').read_text());proto=json.loads((r/'protocol.json').read_text());spec=proto['cases'][case];fixture=pins['fixtures'][spec['fixture']]['root']
def sha(q):return hashlib.sha256(Path(q).read_bytes()).hexdigest()
meta={'mode':mode,'purpose':purpose,'case':case,'fixture':fixture,'python':sys.version,'cpu_scope':'Python process CPU plus stable observed live child endpoint CPU; may miss unobserved short-lived children; snapshots not atomic with wall','threads':'Hail Spark local[4]; Vail Rayon4/existingTokio32, not total-thread parity','reader':'Vail MatrixTable.read_legacy' if mode=='vail' else 'Hail read_matrix_table'}
if mode=='vail':
 import vail as hl,vail._native as native
 assert sha(native.__file__)==pins['native_sha256'];meta.update(native_sha256=sha(native.__file__),allocator=native._benchmark_allocator)
else:
 import hail as hl,pyspark
 assert hl.__version__==pins['hail_version'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 hl.init(master='local[4]',quiet=True,log=str(out/'hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.default.parallelism':'4','spark.sql.shuffle.partitions':'4','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
 meta.update(hail_version=hl.__version__,pyspark=pyspark.__version__,jar_sha256=pins['hail_jar_sha256'],spark_conf=dict(pyspark.SparkContext._active_spark_context.getConf().getAll()))
api=SimpleNamespace(**vars(hl))
if mode=='vail':api.read_matrix_table=hl.MatrixTable.read_legacy
source=r/spec['source'];assert sha(source)==pins['source_sha256'][spec['source']]
fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==case);fn.decorator_list=[];module=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(module);ns={'hl':api};exec(compile(module,str(source),'exec'),ns)
meta.update(source_sha256=sha(source),original_function=ast.unparse(fn),identity_verified=True)
(out/'identity.json').write_text(json.dumps(meta,indent=2)+'\n')
def canonical(x):
 if x is None or isinstance(x,(str,bool,int)):return x
 if isinstance(x,float):return {'float':x.hex() if math.isfinite(x) else repr(x)}
 if isinstance(x,hl.Call):return {'call':[x[i] for i in range(x.ploidy)],'phased':x.phased}
 if isinstance(x,hl.Locus):return {'locus':[x.contig,x.position,x.reference_genome.name]}
 if isinstance(x,(dict,hl.Struct)):return {k:canonical(v) for k,v in x.items()}
 if isinstance(x,(tuple,list)):return [canonical(v) for v in x]
 if isinstance(x,(set,frozenset)):return sorted((canonical(v) for v in x),key=lambda v:json.dumps(v,sort_keys=True))
 raise TypeError(type(x))
def snapshot():
 result={}
 for q in psutil.Process().children(recursive=True):
  try:c=q.cpu_times();result[f'{q.pid}:{q.create_time()}']=c.user+c.system
  except psutil.NoSuchProcess:pass
 return result
try:
 if purpose=='validate':
  result={}
  for name,d in pins['fixtures'].items():
   mt=api.read_matrix_table(d['root']);dims=mt.count();assert list(dims)==d['dimensions'];parts=mt.n_partitions();assert parts==d['partitions']
   result[name]={'dimensions':dims,'partitions':parts,'prefix':canonical(mt.head(3,3).localize_entries('e','c').take(3))}
  (out/'values.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n');meta['values_sha256']=sha(out/'values.json')
 else:
  for _ in range(proto['warmups']):assert ns[case](fixture) is None
  known=set(snapshot());samples=[]
  for i in range(proto['samples']):
   before=snapshot();cpu=time.process_time();start=time.perf_counter();result=ns[case](fixture);wall=time.perf_counter()-start;cpu=time.process_time()-cpu;after=snapshot();assert result is None
   assert known==set(before)==set(after),'Observed descendant endpoint set changed'
   child=sum(after[k]-before[k] for k in after);assert child>=0
   samples.append({'wall_seconds':wall,'cpu_seconds':cpu+child,'python_cpu_seconds':cpu,'observed_child_cpu_seconds':child,'descendants_before':before,'descendants_after':after});print('SAMPLE',i,wall,cpu+child,flush=True)
  meta['samples']=samples
finally:
 if mode=='hail':hl.stop()
meta['passed']=True;(out/'result.json').write_text(json.dumps(meta,indent=2)+'\n');print('PASS',mode,purpose,case,flush=True)
