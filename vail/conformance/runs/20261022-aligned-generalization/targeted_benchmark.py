"""Compare production global-only dispatch with an otherwise matched main package."""
from pathlib import Path
import os,subprocess
p=Path(__file__).resolve().parent
packages=Path('/Users/danielking/.codex/worktrees/vds-holes-experiment/packages')
python='/Users/danielking/projects/vail/vail/.venv/bin/python'
for case in ['filter_global','filter_row','filter_row_only','filter_entry','select']:
 for repeat in range(3):
  for label in (['targetedbaseline','targetedglobal'] if repeat%2==0 else ['targetedglobal','targetedbaseline']):
   name=f'{label}-{case}-repeat{repeat}'
   print(name,flush=True)
   with (p/(name+'.jsonl')).open('w') as out,(p/(name+'.log')).open('w') as err:
    subprocess.run([python,str(p/'benchmark.py'),'--label',label,'--case',case,'--rows','4096','--cols','128'],env=dict(os.environ,PYTHONPATH=str(packages/label)),stdout=out,stderr=err,check=True,timeout=60)
