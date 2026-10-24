import os,pathlib,subprocess
root=pathlib.Path('/private/tmp/h045-local-gate');env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
for label in ['baseline','candidate']:
 with (root/f'{label}-sample-query.jsonl').open('w') as out,(root/f'{label}-sample-query.stderr').open('w') as err:
  p=subprocess.Popen([str(root/f'{label}-env/bin/python'),str(root/'corr.py'),'30'],stdout=subprocess.PIPE,stderr=err,text=True,env=env)
  out.write(p.stdout.readline());out.flush()
  sampled=subprocess.run(['/usr/bin/sample',str(p.pid),'3','1','-file',str(root/f'{label}-sample.txt')],capture_output=True,text=True)
  (root/f'{label}-sample-command.log').write_text(sampled.stdout+sampled.stderr)
  out.write(p.stdout.read());p.wait()
  assert p.returncode==0,p.returncode
