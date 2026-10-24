import hashlib,json,os,pathlib,subprocess
r=pathlib.Path('/private/tmp/h003-list-set-gate');d=r/'guarded-hidden';d.mkdir(exist_ok=False)
b=pathlib.Path('/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target/release/examples/list_set_hidden_parent')
protocol={'binary':str(b),'sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'production_revision':'9213c594','example_revision':'5af3779a','hidden_sizes':[0,1000000,10000000],'routes':['physical','logical'],'owners':[1,3],'iterations':8,'expected':[[7],None,[9]],'note':'input construction/preparation excluded from query wall; OS process CPU/RSS includes input producer; no allocation or universal owner release claim'}
(d/'protocol.json').write_text(json.dumps(protocol,indent=2)+'\n')
for route in protocol['routes']:
 for owners in protocol['owners']:
  for hidden in protocol['hidden_sizes']:
   label=f'{route}-owners{owners}-hidden{hidden}'
   p=subprocess.run(['/usr/bin/time','-l',str(b),str(hidden),'8',route,str(owners)],capture_output=True,text=True,env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
   (d/f'{label}.jsonl').write_text(p.stdout);(d/f'{label}.time.txt').write_text(p.stderr);p.check_returncode()
   rows=[json.loads(x) for x in p.stdout.splitlines()];assert len(rows)==9 and all(x['result']==protocol['expected'] for x in rows[1:]);print(label,flush=True)
