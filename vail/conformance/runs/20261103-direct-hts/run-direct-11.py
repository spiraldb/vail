from pathlib import Path
import subprocess,os,json,statistics
r=Path(__file__).parent;out=r/'direct-matched-11b';out.mkdir(exist_ok=True)
cases=['real-count','real-checksum','unchanged-count','moving-count','mixed-count','high-allele','absent-fields']
for case in cases:
 for run,mode in enumerate(['baseline','direct','direct','baseline']):
  env=os.environ.copy()
  for f in list(env):
   if f.startswith('VAIL_EXPERIMENT_') or f.startswith('VAIL_TRACE_'):env.pop(f)
  env.update(PYTHONPATH=str(r/'direct-package-11b'),RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
  if mode=='direct':env['VAIL_EXPERIMENT_DIRECT_HTS']='1'
  name=f'{run}-{mode}-{case}';print(name,flush=True)
  with (out/(name+'.jsonl')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:
   result=subprocess.run(['nice','-n','10','/Users/danielking/projects/vail/vail/.venv/bin/python',str(r/'probe-direct-11.py'),case],env=env,stdout=stdout,stderr=stderr,timeout=180)
  if result.returncode:raise SystemExit(f'{name} failed {result.returncode}')
summary={}
for case in cases:
 records={mode:[json.loads(line) for p in out.glob('*-'+mode+'-'+case+'.jsonl') for line in p.read_text().splitlines() if '"sample"' in line] for mode in ['baseline','direct']}
 expected={json.loads(p.read_text().splitlines()[0])['expected'] for p in out.glob('*-'+case+'.jsonl')};assert len(expected)==1
 summary[case]={mode:{k:statistics.median(x[k] for x in rows) for k in ['wall','cpu','construction','action','action_cpu']} for mode,rows in records.items()}
 summary[case]['speedup']={k:summary[case]['baseline'][k]/summary[case]['direct'][k] for k in ['wall','cpu','action','action_cpu']}
(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
