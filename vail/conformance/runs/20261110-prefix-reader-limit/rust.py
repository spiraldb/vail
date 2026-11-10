from pathlib import Path
import subprocess,os,json
r=Path(__file__).parent;e=os.environ.copy()
for k,v in json.loads(Path('/private/tmp/vail-allocators-20261109/pinned-sdk/build-environment.json').read_text()).items():
 if v is None:e.pop(k,None)
 else:e[k]=v
args=['nice','-n','10','cargo','test','--release','--locked','--offline','-p','vail-table','--test','prefix_read','--test','reader_batches','--test','consumer_actions','--','--test-threads=1','--nocapture']
with (r/'rust.log').open('w') as f:p=subprocess.run(args,cwd='/private/tmp/vail-list-flatmap-20261106/vail',env=e,stdout=f,stderr=subprocess.STDOUT,timeout=900)
(r/'rust.json').write_text(json.dumps({'command':args,'exit_code':p.returncode},indent=2)+'\n');print('RUST GATE EXIT',p.returncode,flush=True)
