from pathlib import Path
import subprocess,os,json,statistics
r=Path(__file__).parent;out=r/'direct-matched-11c';out.mkdir(exist_ok=True)
for case in ['real-count','real-checksum','real-arrow','empty','short','wide']:
 for run,mode in enumerate(['baseline','direct','direct','baseline']):
  env=os.environ.copy()
  for f in list(env):
   if f.startswith('VAIL_EXPERIMENT_') or f.startswith('VAIL_TRACE_'):env.pop(f)
  env.update(PYTHONPATH=str(r/'direct-package-11c'),RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
  if mode=='direct':env['VAIL_EXPERIMENT_DIRECT_HTS']='1'
  script='probe-direct-arrow.py' if case=='real-arrow' else 'probe.py' if case in ['empty','short','wide'] else 'probe-direct-11.py'
  name=f'{run}-{mode}-{case}';print(name,flush=True)
  with (out/(name+'.jsonl')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:
   result=subprocess.run(['nice','-n','10','/Users/danielking/projects/vail/vail/.venv/bin/python',str(r/script),case],env=env,stdout=stdout,stderr=stderr,timeout=180)
  if result.returncode:raise SystemExit(f'{name} failed {result.returncode}')
summary={}
for case in ['real-count','real-checksum','real-arrow','empty','short','wide']:
 summary[case]={}
 for mode in ['baseline','direct']:
  processes=[]
  for p in sorted(out.glob('*-'+mode+'-'+case+'.jsonl')):
   xs=[json.loads(l) for l in p.read_text().splitlines() if '"sample"' in l]
   processes.append({k:statistics.median(x[k] for x in xs) for k in ['wall','cpu']+(['action','action_cpu','construction'] if case.startswith('real') else [])})
  summary[case][mode]={'process_medians':processes,'median':{k:statistics.median(x[k] for x in processes) for k in processes[0]}}
 summary[case]['speedup']={k:summary[case]['baseline']['median'][k]/summary[case]['direct']['median'][k] for k in summary[case]['baseline']['median']}
(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
