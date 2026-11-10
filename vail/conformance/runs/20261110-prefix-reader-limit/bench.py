from pathlib import Path
import sys,os,subprocess,json,statistics
r=Path(__file__).parent
baseline='/private/tmp/vail-direct-vortex-session-20261109/env/bin/python';candidate=str(r/'env/bin/python')
e=os.environ.copy();e.update(RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc')
for k in list(e):
 if k.startswith('VAIL_') or k in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']:e.pop(k)
cases=['entries_show','entries_prefix','take_entry','entries_count','entries_count_no_key','explode_prefix','explode_full','filter_entries','split_multi_hts','read_rows','read_prefix_all','explode_prefix_all']
if len(sys.argv)>1:cases=sys.argv[1:]
out=r/'bench';out.mkdir(exist_ok=True)
for case in cases:
 for i,mode in enumerate(['baseline','candidate','candidate','baseline','baseline','candidate']):
  p=out/f'{case}-{i}-{mode}.json'
  with p.with_suffix('.log').open('w') as f:subprocess.run([baseline if mode=='baseline' else candidate,str(r/'worker.py'),mode,case,str(p)],env=e,stdout=f,stderr=subprocess.STDOUT,timeout=180,check=True)
  print(case,i,mode,flush=True)
 rows=[]
 for c in cases[:cases.index(case)+1]:
  row={'case':c};hashes=set()
  for mode in ['baseline','candidate']:
   ds=[json.loads(p.read_text()) for p in out.glob(f'{c}-*-{mode}.json')];assert len(ds)==3
   assert all(d['allocator']=='mimalloc' for d in ds)
   hashes.update(d['result_sha256'] for d in ds)
   row[mode]={k:statistics.median(statistics.median(s[k] for s in d['samples']) for d in ds) for k in ['wall','cpu']}
  assert len(hashes)==1,(c,hashes)
  for k in ['wall','cpu']:row[k+'_speedup']=row['baseline'][k]/row['candidate'][k]
  rows.append(row)
 (r/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
print(json.dumps(rows,indent=2))
