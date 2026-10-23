import subprocess,os,json,time,pathlib
root=pathlib.Path('/tmp/aligned-map-mode-experiment');root.mkdir(exist_ok=True)
shapes=[('exact',10000,500000,2500),('width1000',250000,1000,250),('width10000',25000,10000,250)]
results=[]
for name,rows,cols,parts in shapes:
 for rep in range(1,4):
  modes=['scalar','batch'] if rep%2 else ['batch','scalar']
  for mode in modes:
   env=os.environ.copy();env.pop('VAIL_EXPERIMENT_ALIGNED_MAP_BATCH',None);env.pop('VAIL_EXPERIMENT_REE_STATS',None)
   if mode=='batch':env['VAIL_EXPERIMENT_ALIGNED_MAP_BATCH']='1'
   t=time.time();p=subprocess.run(['/private/tmp/large-matrix-sum-env/bin/python','/tmp/aligned-map-mode-query.py',str(rows),str(cols),str(parts)],capture_output=True,text=True,env=env)
   (root/f'{name}-{mode}-{rep}.log').write_text(p.stdout+p.stderr)
   if p.returncode:raise RuntimeError(p.stderr)
   r=json.loads(p.stdout.strip().splitlines()[-1]);r.update(case=name,mode=mode,rep=rep,start_unix=t);results.append(r)
   (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
   print(json.dumps(r),flush=True)
