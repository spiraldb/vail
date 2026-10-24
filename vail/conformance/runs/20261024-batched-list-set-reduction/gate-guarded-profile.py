import json,os,pathlib,subprocess
r=pathlib.Path('/private/tmp/h003-list-set-gate');d=r/'guarded-profiles';d.mkdir(exist_ok=False)
for label in ['candidate']:
 with open(d/f'{label}.stderr','w') as err:
  p=subprocess.Popen([str(r/f'{label}-env/bin/python'),str(r/'pipeline.py'),'dp','20'],stdout=subprocess.PIPE,stderr=err,text=True,env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
  first=p.stdout.readline();ready=json.loads(first);assert ready['phase']=='ready'
  sample=subprocess.run(['/usr/bin/sample',str(p.pid),'3','1','-file',str(d/f'{label}.sample.txt')],capture_output=True,text=True)
  (d/f'{label}.sample-command.log').write_text(sample.stdout+sample.stderr)
  rest=p.stdout.read();p.wait();(d/f'{label}.jsonl').write_text(first+rest);assert p.returncode==0 and sample.returncode==0
  print(label,ready,flush=True)
