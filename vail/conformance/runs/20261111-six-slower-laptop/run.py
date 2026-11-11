from pathlib import Path
import hashlib,json,os,shutil,statistics,sys,time
from owned import run
r=Path(__file__).parent;pins=json.loads((r/'pins.json').read_text());proto=json.loads((r/'protocol.json').read_text());phase=sys.argv[1];assert phase in ['validate','time'];began=time.monotonic();receipts=[]
def sha(q):return hashlib.sha256(Path(q).read_bytes()).hexdigest()
assert sha(r/'pins.json')==proto['pins_sha256']
for name,wanted in proto['scripts'].items():assert sha(r/name)==wanted

def check_inputs():
 count=0
 for d in pins['fixtures'].values():
  root=Path(d['root']);assert {str(q.relative_to(root))for q in root.rglob('*')if q.is_file()}==set(d['files'])
  for rel,w in d['files'].items():
   q=root/rel;assert q.stat().st_size==w['bytes']and sha(q)==w['sha256'],q;count+=1
 assert sha(pins['native'])==pins['native_sha256']and sha(pins['hail_jar'])==pins['hail_jar_sha256'];return count
check_inputs()
if phase=='time':
 v=json.loads((r/'validation-complete.json').read_text());assert v['passed']and v['protocol_sha256']==sha(r/'protocol.json')
 for name,w in v['results'].items():assert sha(r/name)==w
for case in ([next(iter(proto['cases']))]if phase=='validate'else list(proto['cases'])):
 for i,mode in enumerate(['vail','hail']if phase=='validate'else proto['schedule']):
  assert time.monotonic()-began+380<proto['phase_seconds'],'Phase deadline reserve';assert shutil.disk_usage(r).free>=proto['minimum_free_bytes'],'Free-disk floor'
  env={k:v for k,v in os.environ.items()if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc'))and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};env.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=pins['hail_python'],PYSPARK_DRIVER_PYTHON=pins['hail_python'],RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc')
  out=r/phase/f'{case}-{i}-{mode}';print('RUN',phase,case,i,mode,flush=True);rec=run(['nice','-n','10',pins[mode+'_python'],str(r/'worker.py'),mode,phase,case,str(out)],out,env,proto['worker_seconds']);result=json.loads((out/'result.json').read_text());assert result['passed']and result['identity_verified'];receipts.append({'case':case,'mode':mode,'index':i,'out':str(out),'exit':rec});(r/(phase+'-receipts.json')).write_text(json.dumps(receipts,indent=2)+'\n');print('PASS',phase,case,i,mode,round(rec['seconds'],2),flush=True)
 if phase=='time':
  rows=[]
  for done in list(proto['cases'])[:list(proto['cases']).index(case)+1]:
   row={'case':done,'spec':proto['cases'][done]}
   for mode in ['hail','vail']:
    workers=[json.loads((Path(x['out'])/'result.json').read_text())for x in receipts if x['case']==done and x['mode']==mode];assert len(workers)==3
    medians=[{k:statistics.median(s[k]for s in w['samples'])for k in ['wall_seconds','cpu_seconds']}for w in workers];row[mode]={k:statistics.median(x[k]for x in medians)for k in ['wall_seconds','cpu_seconds']};row[mode]['process_medians']=medians
   row['vail_over_hail']={k:row['vail'][k]/row['hail'][k]for k in ['wall_seconds','cpu_seconds']};rows.append(row)
  (r/'summary.json').write_text(json.dumps(rows,indent=2)+'\n');print('CASE SUMMARY',json.dumps(rows[-1]),flush=True)
if phase=='validate':
 data={x['mode']:Path(x['out'])for x in receipts};v=json.loads((data['vail']/'values.json').read_text());h=json.loads((data['hail']/'values.json').read_text());assert {k:v[k]for k in ['profile','gnomad']}==h;assert {k:v['native_profile'][k]for k in ['dimensions','prefix']}=={k:v['profile'][k]for k in ['dimensions','prefix']}
 results={str(q.relative_to(r)):sha(q)for d in data.values()for q in d.iterdir()if q.name in ['values.json','identity.json','result.json','exit.json']};(r/'validation-complete.json').write_text(json.dumps({'passed':True,'protocol_sha256':sha(r/'protocol.json'),'results':results,'scope':'Full dimensions/partition counts and exact3x3inputprefix, includingnativeconversion; notfulloutputequivalence'},indent=2)+'\n')
else:
 count=check_inputs();(r/'complete.json').write_text(json.dumps({'workers':36,'samples':108,'post_input_files_hashed':count,'all_owned_groups_absent':True,'protocol_sha256':sha(r/'protocol.json'),'seconds':time.monotonic()-began},indent=2)+'\n')
print('PHASE COMPLETE',phase,flush=True)
