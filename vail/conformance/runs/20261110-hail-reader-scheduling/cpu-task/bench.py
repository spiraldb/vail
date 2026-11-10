from pathlib import Path
import json,os,hashlib,statistics,shutil,time
from owned import run
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());old=Path('/private/tmp/vail-hail-slow-three-20261110');proto=json.loads((old/'protocol.json').read_text())
e={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc',RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
def inputs():
 for d in cfg['inputs']['fixtures'].values():
  root=Path(d['root']);assert {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}==set(d['files'])
  for name,m in d['files'].items():
   p=root/name;assert p.stat().st_size==m['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==m['sha256'],p
inputs();receipts=[];results=[];start=time.monotonic()
for case in proto['cases']:
 for i,mode in enumerate(['baseline','candidate','candidate','baseline']):
  assert time.monotonic()-start<1200
  out=r/'timings'/f'{case}-{i}-{mode}';print('RUN',case,i,mode,flush=True)
  receipt=run(['nice','-n','10',cfg[mode]['python'],str(r/'bench-worker.py'),mode,case,str(out)],out,e,180);receipts.append(receipt);results.append(json.loads((out/'result.json').read_text()));(r/'bench-progress.json').write_text(json.dumps({'completed':len(results),'receipts':receipts},indent=2)+'\n')
inputs();summary={}
for case in proto['cases']:
 row={}
 for mode in ['baseline','candidate']:
  workers=[x for x in results if x['case']==case and x['mode']==mode]
  row[mode]={k:statistics.median(statistics.median(s[k] for s in x['samples']) for x in workers) for k in ['wall_seconds','cpu_seconds']};row[mode]['max_threads_after_query']=max(s['threads_after_query'] for x in workers for s in x['samples'])
 row['wall_speedup']=row['baseline']['wall_seconds']/row['candidate']['wall_seconds'];row['cpu_change_percent']=100*(row['candidate']['cpu_seconds']/row['baseline']['cpu_seconds']-1);summary[case]=row
(r/'bench-results.json').write_text(json.dumps({'summary':summary,'results':results,'receipts':receipts,'all_inputs_verified_before_after':True,'scope':'2independent processes/version/case balancedB,C,C,B;3callsafter1fullwarmup;wholequery read+compute+forcecount. Baseline/candidate comparison, no new Hail timing.'},indent=2)+'\n');print(json.dumps(summary,indent=2))
