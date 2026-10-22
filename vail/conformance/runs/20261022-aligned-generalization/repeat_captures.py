from pathlib import Path
import os,subprocess
p=Path(__file__).resolve().parent
packages=Path(os.environ.get('VAIL_GENERAL_PACKAGES','/Users/danielking/.codex/worktrees/vds-holes-experiment/packages'))
python='/Users/danielking/projects/vail/vail/.venv/bin/python'
for case in ['filter_row','filter_row_only','filter_global']:
 for repeat in range(3):
  for label in (['generalbaseline','generalbatch'] if repeat%2==0 else ['generalbatch','generalbaseline']):
   name=f'{label}-{case}-4096x128-repeat{repeat}'
   print(name,flush=True)
   with (p/(name+'.jsonl')).open('w') as out, (p/(name+'.log')).open('w') as err:
    subprocess.run([python,str(p/'benchmark.py'),'--label',label,'--case',case,'--rows','4096','--cols','128'],env=dict(os.environ,PYTHONPATH=str(packages/label)),stdout=out,stderr=err,check=True,timeout=60)
