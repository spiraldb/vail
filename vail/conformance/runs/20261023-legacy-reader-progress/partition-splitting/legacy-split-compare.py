import os, subprocess, shutil
from pathlib import Path
root=Path('/Users/danielking/.codex/worktrees/legacy-layout/vail/vail')
py='/private/tmp/vail-legacy-review-env/bin/python'
env=dict(os.environ,PYTHONPATH='python')
def run(args,stem):
    print(stem,flush=True)
    with open('/private/tmp/'+stem+'.log','w') as log:
        subprocess.run([py,*args],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
try:
    for kind,phase in [('after','b1'),('after','b2'),('before','a2')]:
        shutil.copyfile('/private/tmp/legacy-split-'+kind+'.abi3.so',root/'python/vail/_native.abi3.so')
        stem='legacy-split-all-'+phase
        run(['benchmarks/legacy_paired.py','vail','--warmup','3','--runs','7','--reject-compilers','--output','/private/tmp/'+stem+'.json'],stem)
        for single in [False,True]:
            stem='legacy-split-linreg-'+('single-' if single else '')+phase
            run(['/private/tmp/legacy-split-linreg.py','--output','/private/tmp/'+stem+'.json']+(['--single-partition'] if single else []),stem)
finally:
    shutil.copyfile('/private/tmp/legacy-split-after.abi3.so',root/'python/vail/_native.abi3.so')
