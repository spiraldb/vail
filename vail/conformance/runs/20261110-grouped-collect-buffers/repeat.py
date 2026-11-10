from pathlib import Path
import json,os,hashlib,statistics,time
from owned import run
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());ready=json.loads((r/'ready.json').read_text());cfg['candidate']['native_sha256']=ready['identity']['sha256'];cfg['candidate']['source']=ready['source'];(r/'bench-config.json').write_text(json.dumps(cfg,indent=2)+'\n')
e={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=cfg['pins']['hail_python'],PYSPARK_DRIVER_PYTHON=cfg['pins']['hail_python'],NUMEXPR_NUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc',RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
def inputs():
 for d in cfg['pins']['fixtures'].values():
  root=Path(d['root']);assert {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}==set(d['files'])
  for name,m in d['files'].items():
   p=root/name;assert p.stat().st_size==m['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==m['sha256'],p
inputs();receipts=[];results=[];start=time.monotonic()
for case in ['collect_hail','take_rekey']:
 schedule=['candidate','baseline','baseline','candidate']
 for i,mode in enumerate(schedule):
  assert time.monotonic()-start+195<1200
  out=r/'repeat-timings'/f'{case}-{i}-{mode}';print('RUN',case,i,mode,flush=True)
  receipt=run(['nice','-n','10',cfg[mode]['python'],str(r/'bench-worker.py'),mode,case,str(out)],out,e,180);receipts.append(receipt);results.append(json.loads((out/'result.json').read_text()));(r/'repeat-progress.json').write_text(json.dumps({'completed':len(results),'receipts':receipts},indent=2)+'\n')
inputs();summary={}
for case in ['collect_hail','take_rekey']:
 row={}
 for mode in ['baseline','candidate']:
  workers=[x for x in results if x['case']==case and x['mode']==mode]
  row[mode]={k:statistics.median(statistics.median(s[k] for s in x['samples']) for x in workers) for k in ['wall_seconds','cpu_seconds']}
  row[mode]['process_medians']=[{k:statistics.median(s[k] for s in x['samples']) for k in ['wall_seconds','cpu_seconds']} for x in workers]
 row['wall_change_percent']=100*(row['candidate']['wall_seconds']/row['baseline']['wall_seconds']-1);row['cpu_change_percent']=100*(row['candidate']['cpu_seconds']/row['baseline']['cpu_seconds']-1);summary[case]=row
(r/'repeat-results.json').write_text(json.dumps({'summary':summary,'results':results,'receipts':receipts,'all_inputs_verified_before_after':True,'scope':'2 fresh processes/engine/case; balanced H,B,C,C,B,H primary and B,C,C,B controls; median of3 warm calls per process; original upstream force_count actions; execution-only, not cross-engine value validation'},indent=2)+'\n');print(json.dumps(summary,indent=2))
