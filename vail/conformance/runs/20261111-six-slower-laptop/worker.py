from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,math,os,shutil,sys,tempfile,time
import psutil
r=Path(__file__).parent;mode,purpose,case,outpath=sys.argv[1:];out=Path(outpath);pins=json.loads((r/'pins.json').read_text());proto=json.loads((r/'protocol.json').read_text());spec=proto['cases'][case]
def sha(q):return hashlib.sha256(Path(q).read_bytes()).hexdigest()
meta={'mode':mode,'purpose':purpose,'case':case,'python':sys.version,'cpu_scope':'Worker process CPU plus stable observed live-child endpoint CPU; may omit unobserved short-lived children, snapshots not atomic with wall','threads':'Hail Spark local4, Vail Rayon4 with existingTokio32, not enforced total-core parity'}
if mode=='vail':
 import vail as hl,vail._native as native
 assert sha(native.__file__)==pins['native_sha256'];package=Path(native.__file__).parent;assert {str(q.relative_to(package)):sha(q)for q in package.rglob('*.py')}==pins['frontend_sha256'];meta.update(native_sha256=sha(native.__file__),allocator=native._benchmark_allocator)
else:
 import hail as hl,pyspark
 assert hl.__version__==pins['hail_version']and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 hl.init(master='local[4]',quiet=True,log=str(out/'hail.log'),tmp_dir=str(out/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.default.parallelism':'4','spark.sql.shuffle.partitions':'4','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
 meta.update(hail_version=hl.__version__,pyspark=pyspark.__version__,jar_sha256=sha(pins['hail_jar']))
api=SimpleNamespace(**vars(hl))
if mode=='vail'and spec['vail_reader']=='legacy':api.read_matrix_table=hl.MatrixTable.read_legacy
source=r/spec['source'];assert sha(source)==pins['source_sha256'][spec['source']];wanted=spec.get('helpers',[])+[case];tree=ast.parse(source.read_text());fns=[]
for name in wanted:
 fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name==name);fn.decorator_list=[];fns.append(fn)
m=ast.Module(body=fns,type_ignores=[]);ast.fix_missing_locations(m);ns={'hl':api};exec(compile(m,str(source),'exec'),ns);meta.update(original_function=ast.unparse(fns[-1]),source_sha256=sha(source),identity_verified=True)
fixture=pins['fixtures'][spec.get('vail_fixture',spec['fixture'])if mode=='vail'else spec['fixture']]['root'];meta['fixture']=fixture
(out/'identity.json').write_text(json.dumps(meta,indent=2)+'\n')
def canonical(x):
 if x is None or isinstance(x,(str,bool,int)):return x
 if isinstance(x,float):return {'float':x.hex()if math.isfinite(x)else repr(x)}
 if isinstance(x,hl.Call):return {'call':[x[i]for i in range(x.ploidy)],'phased':x.phased}
 if isinstance(x,hl.Locus):return {'locus':[x.contig,x.position,x.reference_genome.name]}
 if isinstance(x,(dict,hl.Struct)):return {k:canonical(v)for k,v in x.items()}
 if isinstance(x,(tuple,list)):return [canonical(v)for v in x]
 if isinstance(x,(set,frozenset)):return sorted((canonical(v)for v in x),key=lambda v:json.dumps(v,sort_keys=True))
 raise TypeError(type(x))
def snapshot():
 result={}
 for q in psutil.Process().children(recursive=True):
  try:c=q.cpu_times();result[f'{q.pid}:{q.create_time()}']=c.user+c.system
  except psutil.NoSuchProcess:pass
 return result
outputs=[]
def invoke(timed):
 assert shutil.disk_usage(r).free>=proto['minimum_free_bytes'],'Free-disk floor'
 tmp=Path(tempfile.mkdtemp(prefix='output-',dir=out))if spec.get('temporary_output')else None
 try:
  before=snapshot();cpu=time.process_time();start=time.perf_counter()
  result=ns[case](fixture,tmp)if tmp is not None else ns[case](fixture)
  wall=time.perf_counter()-start;cpu=time.process_time()-cpu;after=snapshot();assert result is None;assert set(before)==set(after),'Descendant endpoint set changed';child=sum(after[k]-before[k]for k in after);assert child>=0
  if tmp is not None:
   files=[q for q in tmp.rglob('*')if q.is_file()];assert files and sum(q.stat().st_size for q in files)>0;outputs.append({'files':len(files),'bytes':sum(q.stat().st_size for q in files),'timed':timed})
  return {'wall_seconds':wall,'cpu_seconds':cpu+child,'python_cpu_seconds':cpu,'observed_child_cpu_seconds':child,'descendants_before':before,'descendants_after':after,'threads_after_query':psutil.Process().num_threads()}
 finally:
  if tmp is not None:shutil.rmtree(tmp)
try:
 if purpose=='validate':
  values={}
  for name in ['profile','gnomad']+(['native_profile']if mode=='vail'else []):
   d=pins['fixtures'][name];reader=hl.MatrixTable.read_legacy if mode=='vail'and name!='native_profile'else hl.read_matrix_table;mt=reader(d['root']);dims=mt.count();assert list(dims)==d['dimensions'];assert mt.n_partitions()==d['partitions'];values[name]={'dimensions':list(dims),'partitions':mt.n_partitions(),'prefix':canonical(mt.head(3,3).localize_entries('e','c').take(3))}
  (out/'values.json').write_text(json.dumps(values,indent=2,sort_keys=True)+'\n');meta['values_sha256']=sha(out/'values.json')
 else:
  for _ in range(proto['warmups']):invoke(False)
  samples=[]
  for i in range(proto['samples']):
   sample=invoke(True);samples.append(sample);print('SAMPLE',i,sample['wall_seconds'],sample['cpu_seconds'],flush=True)
  meta.update(samples=samples,outputs=outputs)
finally:
 if mode=='hail':hl.stop()
meta['passed']=True;(out/'result.json').write_text(json.dumps(meta,indent=2)+'\n');print('PASS',mode,purpose,case,flush=True)
