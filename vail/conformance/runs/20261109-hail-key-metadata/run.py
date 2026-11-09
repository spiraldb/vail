from pathlib import Path
import json,os,statistics,subprocess
r=Path(__file__).parent
ready=json.loads((r/'ready.json').read_text())
versions={'baseline':'/private/tmp/vail-allocators-20261109/pinned-sdk/mimalloc/env/bin/python','candidate':ready['python']}
fixtures={'legacy':'/private/tmp/vail-hail-comparison-59/first-1000.hail.mt','native':'/private/tmp/vail-hail-four-20261109/inputs/native.mt'}
e=os.environ.copy()
for key in list(e):
    if key.startswith(('VAIL_','MIMALLOC_','JEMALLOC_','MALLOC_','_RJEM_','Malloc')) or key in ('PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD'):
        e.pop(key,None)
e.update(RAYON_NUM_THREADS='4',VAIL_EXECUTION_CONCURRENCY='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc')
out=r/'metadata-timings';out.mkdir(exist_ok=False)
records=[]
# Three independent processes/version/case, balanced alternation; no retries.
order=('baseline','candidate','candidate','baseline','baseline','candidate')
for storage,fixture in fixtures.items():
    for case in ('grouped','read'):
        for index,version in enumerate(order):
            target=out/f'{storage}-{case}-{index}-{version}.json'
            cmd=[versions[version],str(r/'metadata-bench.py'),case,storage,fixture,str(target)]
            with target.with_suffix('.log').open('w') as log:
                subprocess.run(cmd,cwd='/private/tmp',env=e,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
            record=json.loads(target.read_text());record['version']=version;records.append(record)
            (out/'records.json').write_text(json.dumps(records,indent=2)+'\n')
            print(storage,case,version,record['wall_median'],record['cpu_median'],flush=True)
summary=[]
for storage in fixtures:
    for case in ('grouped','read'):
        row={'storage':storage,'case':case}
        for version in versions:
            values=[v for v in records if v['storage']==storage and v['case']==case and v['version']==version]
            row[version]={'wall_s':statistics.median(v['wall_median'] for v in values),'cpu_s':statistics.median(v['cpu_median'] for v in values),'output_counts':sorted(set(s['output_count'] for v in values for s in v['samples'])),'worker_wall_s':[v['wall_median'] for v in values],'worker_cpu_s':[v['cpu_median'] for v in values]}
        assert row['baseline']['output_counts']==row['candidate']['output_counts']
        row['wall_speedup']=row['baseline']['wall_s']/row['candidate']['wall_s'];row['cpu_speedup']=row['baseline']['cpu_s']/row['candidate']['cpu_s'];summary.append(row)
(out/'summary.json').write_text(json.dumps({'source':ready['source'],'workers':len(records),'timed_samples':sum(len(v['samples']) for v in records),'results':summary},indent=2)+'\n')
print('COMPLETE',json.dumps(summary),flush=True)
