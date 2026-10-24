import json,os,pathlib,statistics,subprocess
root=pathlib.Path('/private/tmp/h045-local-gate')
env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
runs=[]
for i,label in enumerate(['baseline','candidate','candidate','baseline']):
 cmd=[str(root/f'{label}-env/bin/python'),str(root/'corr.py'),'6']
 p=subprocess.run(cmd,env=env,capture_output=True,text=True,check=True)
 (root/f'paired-{i}-{label}.jsonl').write_text(p.stdout)
 (root/f'paired-{i}-{label}.stderr').write_text(p.stderr)
 rows=[json.loads(s) for s in p.stdout.splitlines()]
 vals=rows[3:]
 runs.append(dict(label=label,wall=statistics.median(x['seconds'] for x in vals),cpu=statistics.median(x['cpu_seconds'] for x in vals),results=[x['result'] for x in vals]))
summary={'protocol':'fresh-process ABBA; 6iterations/process, discard2; median recorded4 then median process medians','runs':runs}
for metric in ['wall','cpu']:
 a=statistics.median(x[metric] for x in runs if x['label']=='baseline');b=statistics.median(x[metric] for x in runs if x['label']=='candidate')
 summary[metric]={'baseline':a,'candidate':b,'baseline_over_candidate':a/b}
(root/'paired-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
