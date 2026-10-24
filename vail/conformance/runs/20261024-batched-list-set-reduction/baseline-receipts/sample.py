import os,pathlib,subprocess
r=pathlib.Path('/private/tmp/h003-row-set-baseline');env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
for mode in ['native','distinct']:
 with (r/f'{mode}-sample-query.jsonl').open('w') as out,(r/f'{mode}-sample-query.stderr').open('w') as err:
  p=subprocess.Popen(['/private/tmp/h045-local-gate/candidate-env/bin/python','/private/tmp/h003-row-set-profile.py','20',mode],stdout=subprocess.PIPE,stderr=err,text=True,env=env)
  out.write(p.stdout.readline());out.flush()
  sample=subprocess.run(['/usr/bin/sample',str(p.pid),'3','1','-file',str(r/f'{mode}-sample.txt')],capture_output=True,text=True)
  (r/f'{mode}-sample-command.log').write_text(sample.stdout+sample.stderr)
  out.write(p.stdout.read());p.wait();assert p.returncode==0,p.returncode
