from pathlib import Path
import hashlib,json,os,statistics,sys,time
from owned import run
r=Path(__file__).parent;pins=json.loads((r/'pins.json').read_text());proto=json.loads((r/'protocol.json').read_text());phase=sys.argv[1];assert phase in ['validate','time'];began=time.monotonic();receipts=[]
def sha(q):return hashlib.sha256(Path(q).read_bytes()).hexdigest()
assert sha(r/'pins.json')==proto['pins_sha256']
for name,wanted in proto['scripts'].items():assert sha(r/name)==wanted
if phase=='time':
 val=json.loads((r/'validation-complete.json').read_text());assert val['passed'] and val['protocol_sha256']==sha(r/'protocol.json')
 for name,wanted in val['results'].items():assert sha(r/name)==wanted
for case in ([next(iter(proto['cases']))] if phase=='validate' else list(proto['cases'])):
 for i,mode in enumerate(['vail','hail'] if phase=='validate' else proto['schedule']):
  assert time.monotonic()-began+380<3600,'Preserve worker and cleanup reserve in phase limit'
  env={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};env.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=pins['hail_python'],PYSPARK_DRIVER_PYTHON=pins['hail_python'],RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc')
  out=r/phase/f'{case}-{i}-{mode}';rec=run(['nice','-n','10',pins[mode+'_python'],str(r/'worker.py'),mode,phase,case,str(out)],out,env,360)
  result=json.loads((out/'result.json').read_text());assert result['passed'] and result['identity_verified'];receipts.append({'case':case,'mode':mode,'index':i,'out':str(out),'exit':rec});(r/(phase+'-receipts.json')).write_text(json.dumps(receipts,indent=2)+'\n');print('PASS',phase,case,i,mode,round(rec['seconds'],2),flush=True)
 if phase=='time':
  summary=[]
  for done in list(proto['cases'])[:list(proto['cases']).index(case)+1]:
   row={'case':done,'fixture':proto['cases'][done]['fixture']}
   for mode in ['hail','vail']:
    workers=[json.loads((Path(x['out'])/'result.json').read_text()) for x in receipts if x['case']==done and x['mode']==mode];assert len(workers)==3
    row[mode]={k:statistics.median(statistics.median(s[k] for s in w['samples']) for w in workers) for k in ['wall_seconds','cpu_seconds']}
   row['vail_over_hail']={k:row['vail'][k]/row['hail'][k] for k in ['wall_seconds','cpu_seconds']};summary.append(row)
  (r/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
if phase=='validate':
 data={x['mode']:Path(x['out']) for x in receipts};assert json.loads((data['vail']/'values.json').read_text())==json.loads((data['hail']/'values.json').read_text())
 results={str(q.relative_to(r)):sha(q) for d in data.values() for q in d.iterdir() if q.name in ['values.json','identity.json','result.json','exit.json']}
 (r/'validation-complete.json').write_text(json.dumps({'passed':True,'protocol_sha256':sha(r/'protocol.json'),'results':results,'scope':'Full public dimensions/partitions and exact3x3inputprefix equality, not full output semantics'},indent=2)+'\n')
else:
 count=0
 for d in pins['fixtures'].values():
  root=Path(d['root']);assert {str(q.relative_to(root)) for q in root.rglob('*') if q.is_file()}==set(d['files'])
  for rel,wanted in d['files'].items():
   q=root/rel;assert {'bytes':q.stat().st_size,'sha256':sha(q)}==wanted;count+=1
 assert sha(pins['native'])==pins['native_sha256'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 (r/'complete.json').write_text(json.dumps({'workers':18,'samples':54,'all_owned_groups_absent':True,'post_full_input_file_hashes_verified':count,'protocol_sha256':sha(r/'protocol.json')},indent=2)+'\n');print(json.dumps(summary,indent=2),flush=True)
print('PHASE COMPLETE',phase,flush=True)
