import json,math,os,pathlib,random,statistics,subprocess
r=pathlib.Path('/private/tmp/h003-list-set-gate');d=r/'paired';d.mkdir(exist_ok=False)
seed=30020261024;rng=random.Random(seed)
cases=[('dp',8,16,4,642162),('wide_struct',2,4,1,10240000),('float_control',2,8,2,642162)]
protocol={'seed':seed,'cases':[{'mode':m,'pairs':n,'iterations':i,'warmups':w,'expected':e} for m,n,i,w,e in cases],'primary':'paired process mean latency andCPU; process peakRSS; all rawqueries retained; no builds/tests overlap'}
(d/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n');results=[]
for mode,pairs,iterations,warmups,expected in cases:
 orders=['AB','BA']*(pairs//2);rng.shuffle(orders)
 for block,order in enumerate(orders):
  outputs={}
  for nominal in order:
   label={'A':'baseline','B':'candidate'}[nominal];prefix=f'{mode}-{block}-{nominal}'
   p=subprocess.run([str(r/f'{label}-env/bin/python'),str(r/'pipeline.py'),mode,str(iterations)],capture_output=True,text=True,env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
   (d/f'{prefix}.jsonl').write_text(p.stdout);(d/f'{prefix}.stderr').write_text(p.stderr);p.check_returncode()
   data=[json.loads(x) for x in p.stdout.splitlines()];rows=data[1:];steady=rows[warmups:];assert len(rows)==iterations and all(x['result']==expected for x in rows)
   outputs[nominal]={'native':data[0]['native_sha256'],'wall':statistics.mean(x['wall_seconds'] for x in steady),'cpu':statistics.mean(x['cpu_seconds'] for x in steady),'peakrss':max(x['maxrss_bytes'] for x in rows),'minor_faults':statistics.mean(x['minor_faults'] for x in steady)}
  results.append({'mode':mode,'block':block,'order':order,'processes':outputs});(d/'completed.json').write_text(json.dumps(results,indent=2)+'\n');print(json.dumps(results[-1]),flush=True)
summary={'protocol':protocol,'pairs':results,'modes':{}}
for mode,_,_,_,_ in cases:
 group=[x for x in results if x['mode']==mode];out={}
 for metric in ['wall','cpu','peakrss']:
  logs=[math.log(x['processes']['B'][metric]/x['processes']['A'][metric]) for x in group];br=random.Random(seed)
  boot=sorted(math.exp(statistics.mean(br.choices(logs,k=len(logs)))) for _ in range(10000))
  out[metric]={'baseline':statistics.mean(x['processes']['A'][metric] for x in group),'candidate':statistics.mean(x['processes']['B'][metric] for x in group),'B_over_A_geomean':math.exp(statistics.mean(logs)),'paired_bootstrap_95_CI':[boot[249],boot[9749]],'pair_ratios':[math.exp(x) for x in logs]}
 summary['modes'][mode]=out
(d/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary['modes'],indent=2))
