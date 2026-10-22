from pathlib import Path
import json,os,subprocess,sys
p=Path(__file__).resolve().parent
python='/Users/danielking/projects/vail/vail/.venv/bin/python'
packages=Path(os.environ.get('VAIL_GENERAL_PACKAGES','/Users/danielking/.codex/worktrees/vds-holes-experiment/packages'))
cases=[('annotate',512,32,0),('select',4096,128,0),('transmute',16384,8,0),('strings',4096,128,0),('nested',512,32,0),('filter_entry',512,32,0),('filter_entry',4096,128,0),('filter_row',512,32,0),('filter_row',4096,128,0),('filter_col',512,32,0),('filter_missing',512,32,0),('annotate',128,16,4096),('filter_col',128,16,4096),('control_row',4096,128,0)]
def command(case,r,c,width):return [python,str(p/'benchmark.py'),'--case',case,'--rows',str(r),'--cols',str(c),'--payload',str(width)]
env=dict(os.environ,PYTHONPATH=str(packages/'generalbaseline'))
for r,c,width in sorted({(r,c,w) for _,r,c,w in cases}):
 print('Preparing',r,c,width,flush=True)
 subprocess.run(command('annotate',r,c,width)+['--label','prepare','--prepare'],env=env,check=True,stdout=subprocess.DEVNULL)
for i,(case,r,c,width) in enumerate(cases):
 for label in (['generalbaseline','generalbatch'] if i%2==0 else ['generalbatch','generalbaseline']):
  name=f'{label}-{case}-{r}x{c}-w{width}'
  print('Starting',name,flush=True)
  env=dict(os.environ,PYTHONPATH=str(packages/label))
  with (p/(name+'.jsonl')).open('w') as output,(p/(name+'.log')).open('w') as errors:
   try:result=subprocess.run(command(case,r,c,width)+['--label',label],env=env,stdout=output,stderr=errors,timeout=60)
   except subprocess.TimeoutExpired:print('TIMEOUT',name,flush=True);continue
  print('Finished',name,'exit',result.returncode,flush=True)
