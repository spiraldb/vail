from pathlib import Path
import hashlib,json,os,statistics,sys,time
from owned import run
r=Path(__file__).parent;pins=json.loads((r/'pins.json').read_text());phase=sys.argv[1];assert phase in ['validate','time'];began=time.monotonic();receipts=[]
cases=['test_matrix_table_show','test_matrix_table_rows_show','test_matrix_table_entries_show','test_matrix_table_take_entry','test_matrix_table_take_row']
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
lock=json.loads((r/'protocol.json').read_text());assert sha(r/'pins.json')==lock['pins_sha256']
for name,wanted in lock['scripts'].items():assert sha(r/name)==wanted
if phase=='time':
 v=json.loads((r/'validation-complete.json').read_text());assert v['passed'] and v['protocol_sha256']==sha(r/'protocol.json')
 for name,wanted in v['results'].items():assert sha(r/name)==wanted
for case in (['test_matrix_table_entries_show'] if phase=='validate' else cases):
 for i,mode in enumerate(['vail','hail'] if phase=='validate' else ['hail','vail','vail','hail','hail','vail','vail','hail']):
  assert time.monotonic()-began+200<1800
  env={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']}
  env.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=pins['hail_python'],PYSPARK_DRIVER_PYTHON=pins['hail_python'],RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc',COLUMNS='80',LINES='24')
  out=r/phase/f'{case}-{i}-{mode}';rec=run([pins[mode+'_python'],str(r/'worker.py'),mode,phase,case,str(out)],out,env,180)
  result=json.loads((out/'result.json').read_text());assert result['passed'] and result['identity_verified']
  receipts.append({'case':case,'mode':mode,'index':i,'out':str(out),'exit':rec});(r/(phase+'-receipts.json')).write_text(json.dumps(receipts,indent=2)+'\n');print('PASS',phase,case,i,mode,round(rec['seconds'],2),flush=True)
if phase=='validate':
 data={x['mode']:Path(x['out']) for x in receipts}
 assert json.loads((data['vail']/'values.json').read_text())==json.loads((data['hail']/'values.json').read_text()),'Logical prefix values mismatch'
 results={str(p.relative_to(r)):sha(p) for d in data.values() for p in d.iterdir() if p.name in ['values.json','identity.json','result.json','exit.json']}
 (r/'validation-complete.json').write_text(json.dumps({'passed':True,'protocol_sha256':sha(r/'protocol.json'),'results':results,'scope':'Exact logical values for101rows,11entries,100GTs,100AFarrays; not renderer string equality or a full dataset conformance comparison'},indent=2)+'\n')
else:
 summary=[]
 for case in cases:
  result={'case':case}
  for mode in ['hail','vail']:
   workers=[json.loads((Path(x['out'])/'result.json').read_text()) for x in receipts if x['case']==case and x['mode']==mode];assert len(workers)==4
   result[mode]={k:statistics.median(statistics.median(s[k] for s in w['samples']) for w in workers) for k in ['wall_seconds','cpu_seconds']}
  result['hail_divided_by_vail']={k:result['hail'][k]/result['vail'][k] for k in ['wall_seconds','cpu_seconds']};summary.append(result)
 (r/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2),flush=True)
 for mode in ['hail','vail']:
  d=pins[mode+'_fixture'];root=Path(d['root']);assert {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}==set(d['files'])
  for rel,wanted in d['files'].items():
   p=root/rel;assert {'bytes':p.stat().st_size,'sha256':sha(p)}==wanted
 assert sha(pins['native'])==pins['native_sha256'] and sha(pins['hail_jar'])==pins['hail_jar_sha256']
 (r/'complete.json').write_text(json.dumps({'workers':40,'samples':200,'all_groups_absent':True,'post_full_input_hashes_verified':83,'protocol_sha256':sha(r/'protocol.json')},indent=2)+'\n')
print('PHASE COMPLETE',phase,flush=True)
