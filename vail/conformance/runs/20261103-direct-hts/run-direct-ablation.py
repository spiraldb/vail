from pathlib import Path
import subprocess,os,json,statistics
r=Path(__file__).parent;out=r/'direct-ablation-11c';out.mkdir(exist_ok=True)
for run,mode in enumerate(['early','eager','eager','early']):
 env=os.environ.copy()
 for f in list(env):
  if f.startswith('VAIL_EXPERIMENT_') or f.startswith('VAIL_TRACE_'):env.pop(f)
 env.update(PYTHONPATH=str(r/'direct-package-11c'),VAIL_EXPERIMENT_DIRECT_HTS='1',RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
 if mode=='eager':env['VAIL_ABLATE_EAGER_BRANCH']='1'
 name=f'{run}-{mode}';print(name,flush=True)
 with (out/(name+'.jsonl')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:
  result=subprocess.run(['nice','-n','10','/Users/danielking/projects/vail/vail/.venv/bin/python',str(r/'probe-direct-ablation.py'),'real-count'],env=env,stdout=stdout,stderr=stderr,timeout=180)
 if result.returncode:raise SystemExit(f'{name} failed')
s={}
for mode in ['early','eager']:
 xs=[]
 for p in sorted(out.glob('*-'+mode+'.jsonl')):
  records=[json.loads(l) for l in p.read_text().splitlines() if '"sample"' in l];xs.append({k:statistics.median(x[k] for x in records) for k in ['wall','cpu','action','action_cpu','construction']})
 s[mode]={'process_medians':xs,'median':{k:statistics.median(x[k] for x in xs) for k in xs[0]}}
(out/'summary.json').write_text(json.dumps(s,indent=2));print(json.dumps(s,indent=2))
