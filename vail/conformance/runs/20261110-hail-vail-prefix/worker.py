from pathlib import Path
import ast,contextlib,hashlib,io,json,math,os,resource,sys,time
import psutil
r=Path(__file__).parent;mode,purpose,case,outpath=sys.argv[1:];out=Path(outpath);pins=json.loads((r/'pins.json').read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
fixture=pins[mode+'_fixture']['root'];meta={'mode':mode,'purpose':purpose,'case':case,'fixture':fixture,'python':sys.version,'cpu_scope':'Python process plus stable observed live child endpoints; lower bound if unobserved descendants exit','threads':'Hail Spark local[4]; Vail Rayon4 / existing Tokio32, not total-thread parity'}
if mode=='vail':
 import vail as hl,vail._native as native
 assert sha(native.__file__)==pins['native_sha256'];meta.update(native_sha256=sha(native.__file__),allocator=native._benchmark_allocator)
else:
 import hail as hl,pyspark
 assert hl.__version__==pins['hail_version'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 hl.init(master='local[4]',quiet=True,log=str(out/'hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.default.parallelism':'4','spark.sql.shuffle.partitions':'4','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
 meta.update(hail_version=hl.__version__,hail_jar_sha256=pins['hail_jar_sha256'],spark_conf=dict(pyspark.SparkContext._active_spark_context.getConf().getAll()))
source=r/'benchmark_matrix_table.py';assert sha(source)==pins['benchmark_source_sha256']
fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==case);fn.decorator_list=[]
module=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(module);ns={'hl':hl};exec(compile(module,str(source),'exec'),ns)
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
 records={}
 for p in psutil.Process().children(recursive=True):
  try:c=p.cpu_times();records[f'{p.pid}:{p.create_time()}']=c.user+c.system
  except psutil.NoSuchProcess:pass
 return records
try:
 if purpose=='validate':
  mt=hl.read_matrix_table(fixture)
  value={'rows101':canonical(mt.rows().take(101)),'entries11':canonical(mt.entries().take(11)),'GT100':canonical(mt.GT.take(100)),'AF100':canonical(mt.info.AF.take(100))}
  (out/'values.json').write_text(json.dumps(value,sort_keys=True,indent=2)+'\n');meta['values_sha256']=sha(out/'values.json')
 else:
  def query():
   buffer=io.StringIO()
   with contextlib.redirect_stdout(buffer):result=ns[case](fixture)
   assert result is None
   return buffer.getvalue()
  for _ in range(3):query()
  known=set(snapshot());samples=[];outputs=[]
  for _ in range(5):
   before=snapshot();cpu=time.process_time();start=time.perf_counter();text=query();wall=time.perf_counter()-start;cpu=time.process_time()-cpu;after=snapshot()
   assert known==set(before)==set(after),'Descendant set changed, CPU endpoints incomplete'
   child=sum(after[k]-before[k] for k in after);assert child>=0
   samples.append({'wall_seconds':wall,'cpu_seconds':cpu+child,'python_cpu_seconds':cpu,'observed_child_cpu_seconds':child,'descendants_before':before,'descendants_after':after})
   outputs.append({'characters':len(text),'sha256':hashlib.sha256(text.encode()).hexdigest()})
  if 'show' in case:assert all(x['characters']>0 for x in outputs)
  assert len({x['sha256'] for x in outputs})==1
  meta.update(samples=samples,outputs=outputs)
finally:
 if mode=='hail':hl.stop()
meta['passed']=True;(out/'result.json').write_text(json.dumps(meta,indent=2)+'\n');print('PASS',mode,purpose,case,flush=True)
