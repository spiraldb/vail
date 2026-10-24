"""One predeclared trial. Run only in the granted exclusive measurement slot."""
import datetime,json,math,os,pathlib,random,statistics,subprocess
root=pathlib.Path('/private/tmp/h045-local-gate');out=root/'controlled';out.mkdir(exist_ok=False)
rng=random.Random(45020261024)
orders=['AB','BA']*4;rng.shuffle(orders)
blocks=[{'kind':'AB','order':order} for order in orders]+[{'kind':'AA','order':order} for order in ['AB','BA','AB','BA']]
rng.shuffle(blocks)
protocol={'seed':45020261024,'blocks':blocks,'iterations':20,'discard_warmups':4,'primary':'arithmetic mean of16steady queries/process; paired candidate/baseline ratios','secondary':'median latency/process; CPU seconds; faults/RSS','controls':'AA nominal A/B both use the identical baseline env/native','no_further_trials':True}
(out/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n')
env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
results=[]
for block_id,block in enumerate(blocks):
 pair={}
 for nominal in block['order']:
  label='baseline' if block['kind']=='AA' or nominal=='A' else 'candidate'
  prefix=f'{block_id:02d}-{block["kind"]}-{nominal}-{label}'
  health=subprocess.run(['/usr/sbin/sysctl','vm.loadavg','vm.swapusage'],capture_output=True,text=True)
  (out/f'{prefix}.health.txt').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n'+health.stdout+health.stderr)
  p=subprocess.run([str(root/f'{label}-env/bin/python'),str(root/'corr.py'),'20'],env=env,capture_output=True,text=True,check=True)
  (out/f'{prefix}.jsonl').write_text(p.stdout);(out/f'{prefix}.stderr').write_text(p.stderr)
  data=[json.loads(line) for line in p.stdout.splitlines()];rows=data[1:];steady=rows[4:]
  assert len(rows)==20 and len(steady)==16
  assert all(row['result']==0.7536333631956934 for row in rows)
  expected={'baseline':'9ee229e8adb724fb3d06a676eaae5f407f07dd43668badc5824820021a52db8b','candidate':'ab991ed6abd678de4fecd9d787e1b4810beac853532d91efc64e0ba73e7b30bc'}
  assert data[0]['native_sha256']==expected[label]
  pair[nominal]={'artifact':label,'mean_wall':statistics.mean(r['seconds'] for r in steady),'median_wall':statistics.median(r['seconds'] for r in steady),'mean_cpu':statistics.mean(r['cpu_seconds'] for r in steady),'mean_minor_faults':statistics.mean(r['minor_faults'] for r in steady),'maxrss_bytes':max(r['maxrss_bytes'] for r in steady)}
 results.append({'block_id':block_id,**block,'processes':pair})
 (out/'completed.json').write_text(json.dumps(results,indent=2)+'\n')
 print(json.dumps(results[-1]),flush=True)
summary={'protocol':protocol,'blocks':results}
for kind in ['AB','AA']:
 group=[r for r in results if r['kind']==kind];metrics={}
 for metric in ['mean_wall','median_wall','mean_cpu']:
  logs=[math.log(r['processes']['B'][metric]/r['processes']['A'][metric]) for r in group]
  boot_rng=random.Random(45020261024);boot=sorted(math.exp(statistics.mean(boot_rng.choices(logs,k=len(logs)))) for _ in range(10000))
  metrics[metric]={'B_over_A_geomean':math.exp(statistics.mean(logs)),'paired_bootstrap_95_percentile_CI':[boot[249],boot[9749]],'pair_ratios':[math.exp(x) for x in logs]}
 summary[kind]=metrics
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:summary[k] for k in ['AB','AA']},indent=2))
