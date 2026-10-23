import os, subprocess
from pathlib import Path
root=Path('/Users/danielking/.codex/worktrees/legacy-layout/vail/vail')
for i,engine in enumerate(['vail','hail','hail','vail']):
    stem=f'legacy-fresh-{i}-{engine}'
    env=dict(os.environ,PYTHONPATH='python',SPARK_LOCAL_IP='127.0.0.1',JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home')
    py='/private/tmp/'+('vail-legacy-review-env' if engine=='vail' else 'linreg-hail-pypi-latest')+'/bin/python'
    print(stem,flush=True)
    with open('/private/tmp/'+stem+'.log','w') as log:
        subprocess.run([py,'benchmarks/legacy_paired.py',engine,'--warmup','6','--runs','7','--reject-compilers','--output','/private/tmp/'+stem+'.json'],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
