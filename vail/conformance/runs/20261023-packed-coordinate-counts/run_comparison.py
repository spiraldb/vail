import os,json,time,subprocess,hashlib,platform
from pathlib import Path
out=Path('/Users/danielking/.codex/worktrees/shared-annotation-aggregates/vail/vail/conformance/runs/20261023-packed-coordinate-counts')
deadline=time.monotonic()+900
while 'Installed vail-0.1.0a1' not in Path('/tmp/packed-count-baseline-build.log').read_text():
 if time.monotonic()>deadline: raise RuntimeError('baseline build deadline')
 time.sleep(2)
print('Baseline install complete',flush=True)
env=os.environ.copy();env['EXECUTION_CONCURRENCY']='32';env.pop('PYTHONPATH',None)
rows=[]
for rep in range(1,4):
 for variant in (['baseline','candidate'] if rep%2 else ['candidate','baseline']):
  python='/tmp/packed-count-baseline-env/bin/python' if variant=='baseline' else '/tmp/shared-annotation-env/bin/python'
  p=subprocess.run([python,str(out/'experiment.py')],env=env,text=True,capture_output=True)
  (out/f'{variant}-{rep}.log').write_text(p.stdout+p.stderr)
  p.check_returncode();result=json.loads(p.stdout.strip().splitlines()[-1]);result.update(variant=variant,repetition=rep);rows.append(result)
  (out/'pairs.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(result),flush=True)
with (out/'production-probe.csv').open('w') as f:subprocess.run(['/tmp/packed-coordinate-count'],stdout=f,check=True)
print('COMPARISON COMPLETE',flush=True)
