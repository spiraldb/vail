import sys,os,pathlib,subprocess,json,statistics
r=pathlib.Path(__file__).parent
base,candidate,fixture=sys.argv[1:]
env=os.environ.copy()
env.update({'RAYON_NUM_THREADS':'4','VAIL_EXECUTION_CONCURRENCY':'4','OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','PYTHONMALLOC':'default','ARROW_DEFAULT_MEMORY_POOL':'mimalloc'})
for case in ['unique_show','unknown_show','unknown_take','duplicate_show','unknown_count']:
    for i,mode in enumerate(['baseline','candidate','candidate','baseline','baseline','candidate']):
        output=r/f'{case}-{i}-{mode}.json'
        with (r/f'{case}-{i}-{mode}.log').open('w') as f:
            subprocess.run([base if mode=='baseline' else candidate,str(r/'worker.py'),mode,case,fixture,str(output)],check=True,stdout=f,stderr=subprocess.STDOUT,env=env,timeout=120)
        print(case,i,mode,flush=True)
rows=[]
for case in ['unique_show','unknown_show','unknown_take','duplicate_show','unknown_count']:
    row={'case':case}
    for mode in ['baseline','candidate']:
        data=[json.loads(p.read_text()) for p in r.glob(f'{case}-*-{mode}.json')]
        assert len(data)==3
        assert all(d['allocator']=='mimalloc' for d in data)
        row[mode]={k:statistics.median(statistics.median(s[k] for s in d['samples']) for d in data) for k in ['wall','cpu']}
    row['wall_speedup']=row['baseline']['wall']/row['candidate']['wall'];row['cpu_speedup']=row['baseline']['cpu']/row['candidate']['cpu'];rows.append(row)
(r/'summary.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(rows,indent=2))
