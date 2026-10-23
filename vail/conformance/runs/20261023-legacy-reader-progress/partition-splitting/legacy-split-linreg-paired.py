import os,subprocess,shutil
from pathlib import Path
root=Path('/Users/danielking/.codex/worktrees/legacy-layout/vail/vail');py='/private/tmp/vail-legacy-review-env/bin/python';env=dict(os.environ,PYTHONPATH='python')
try:
    for i,kind in enumerate(['before','after','after','before']):
        shutil.copyfile('/private/tmp/legacy-split-'+kind+'.abi3.so',root/'python/vail/_native.abi3.so')
        for single in [False,True]:
            stem='legacy-split-linreg-paired-'+str(i)+('-single' if single else '')
            print(stem,flush=True)
            with open('/private/tmp/'+stem+'.log','w') as log:
                subprocess.run([py,'/private/tmp/legacy-split-linreg.py','--warmup','2','--runs','5','--output','/private/tmp/'+stem+'.json']+(['--single-partition'] if single else []),cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
finally:
    shutil.copyfile('/private/tmp/legacy-split-after.abi3.so',root/'python/vail/_native.abi3.so')
