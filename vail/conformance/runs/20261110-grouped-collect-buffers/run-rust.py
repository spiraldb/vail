from pathlib import Path
import os,subprocess,json,time
r=Path(__file__).parent;e=os.environ.copy()
for k,v in json.loads(Path('/private/tmp/vail-allocators-20261109/pinned-sdk/build-environment.json').read_text()).items():
 if v is None:e.pop(k,None)
 else:e[k]=v
cmd=['nice','-n','10','cargo','test','--release','--locked','--offline','-p','vail-table','--test','collect_groups','--test','group_ranges','--test','aggregate_singleton','--','--test-threads=1','--nocapture']
t=time.time()
with (r/'rust-final.log').open('w') as f:p=subprocess.run(cmd,cwd='/private/tmp/vail-list-flatmap-20261106/vail',env=e,stdout=f,stderr=subprocess.STDOUT,timeout=1200)
(r/'rust-final.json').write_text(json.dumps({'command':cmd,'returncode':p.returncode,'seconds':time.time()-t},indent=2)+'\n');print(p.returncode)
