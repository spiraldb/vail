import hashlib,json,os,pathlib,random,re,resource,subprocess,time,statistics,signal,types
r=pathlib.Path('/private/tmp/vail-df-leaf-binding-gate');out=r/'resources-completion';out.mkdir(exist_ok=True)
art={label:json.loads((r/f'resource-{label}'/'artifact.json').read_text()) for label in ['baseline','candidate']}
assert art['baseline']['test_sha256']==art['candidate']['test_sha256']
cases=[{'mode':'many','phase':'plan','width':256,'rows':32,'iterations':2}]+[{'mode':mode,'phase':'execute','width':64,'rows':100000,'iterations':8} for mode in ['rename','wide']]
seed=94020261024;rng=random.Random(seed);order=[]
for i,case in enumerate(cases):
 for pair in range(4):
  labels=['baseline','candidate'];rng.shuffle(labels)
  for label in labels:order.append(dict(case,case=i,pair=pair,label=label))
protocol={'seed':seed,'pairs_per_case':4,'cases':cases,'order':order,'artifacts':art,'cpu_scope':'external process CPU includes fixture, warmup, planning/execution/validation; not kernel time','rss_scope':'/usr/bin/time maximum resident set size, whole child process','overall_budget_seconds':180}
(out/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n');results=[];start=time.monotonic()
with (out/'raw.jsonl').open('w') as raw:
 for index,c in enumerate(order):
  if time.monotonic()-start>180:raise SystemExit('Overall resource budget reached')
  env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',VAIL_DF_MODE=c['mode'],VAIL_DF_PHASE=c['phase'],VAIL_DF_WIDTH=str(c['width']),VAIL_DF_ROWS=str(c['rows']),VAIL_DF_ITERATIONS=str(c['iterations']))
  before=resource.getrusage(resource.RUSAGE_CHILDREN);t=time.monotonic()
  proc=subprocess.Popen(['/usr/bin/time','-l',art[c['label']]['binary'],'--exact','public_valid_projection_resource_control','--nocapture','--test-threads=1'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
  try: stdout,stderr=proc.communicate(timeout=15)
  except subprocess.TimeoutExpired:
   os.killpg(proc.pid,signal.SIGTERM);stdout,stderr=proc.communicate()
   (out/f'{index:03d}.stdout').write_text(stdout);(out/f'{index:03d}.stderr').write_text(stderr)
   raise SystemExit(f'Completion-only child cap reached case{index}; exact group stopped')
  p=types.SimpleNamespace(stdout=stdout,stderr=stderr,returncode=proc.returncode)
  after=resource.getrusage(resource.RUSAGE_CHILDREN)
  (out/f'{index:03d}.stdout').write_text(p.stdout);(out/f'{index:03d}.stderr').write_text(p.stderr)
  wall=re.search(r'wall_seconds=([0-9.eE+-]+)',p.stderr);peak=re.search(r'(\d+)\s+maximum resident set size',p.stderr)
  row=dict(c,index=index,returncode=p.returncode,process_wall_seconds=time.monotonic()-t,process_cpu_seconds=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,measured_wall_seconds=float(wall.group(1)) if wall else None,maxrss_bytes=int(peak.group(1)) if peak else None)
  results.append(row);raw.write(json.dumps(row)+'\n');raw.flush()
  if p.returncode or wall is None or peak is None:raise SystemExit(f'Public control failure case{index}; raw retained')
summaries=[]
for case in range(len(cases)):
 rows=[x for x in results if x['case']==case];summary=dict(cases[case])
 for metric in ['measured_wall_seconds','process_cpu_seconds','maxrss_bytes']:
  b=[x[metric] for x in rows if x['label']=='baseline'];c=[x[metric] for x in rows if x['label']=='candidate'];summary[metric]={'baseline_mean':statistics.mean(b),'candidate_mean':statistics.mean(c),'ratio':statistics.mean(c)/statistics.mean(b)}
 summaries.append(summary)
(out/'summary.json').write_text(json.dumps({'elapsed_seconds':time.monotonic()-start,'processes':len(results),'cases':summaries},indent=2)+'\n');print(json.dumps({'elapsed_seconds':time.monotonic()-start,'processes':len(results)}))
