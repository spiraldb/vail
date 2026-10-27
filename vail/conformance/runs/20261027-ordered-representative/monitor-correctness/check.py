import json,os,pathlib,subprocess,sys
sys.path.insert(0,sys.argv[1])
from monitor import observe
base=pathlib.Path('/private/tmp/h091-monitor-check')
for mode in ['success','output','slow']:
 r=observe([sys.executable,str(base/'child.py'),mode],[sys.executable,'-c','import time;time.sleep(.2)'],base/mode,os.environ.copy(),sample_limit=1,hard_limit=2,process_limit=3,action_limit=.1 if mode=='slow' else 1,output_limit=65536 if mode=='output' else 1024*1024)
 assert r['owned_groups_clean'] and r['query_final_status'] is not None
 if mode=='success':assert r['error'] is None and r['sampler_decision_status']==0 and r['completed_actions']>12
 else:assert r['error'] is not None and r['gates'].get('full_output_cap' if mode=='output' else 'inflight_deadline') is False
 print(json.dumps({'mode':mode,'result':r}),flush=True)
