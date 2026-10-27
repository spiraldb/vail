import json,os,pathlib,sys
sys.path.insert(0,sys.argv[1])
from owned import execute
base=pathlib.Path('/private/tmp/metadata-owned-deadline-tests-20261027')
cases=[('fast',['/usr/bin/true'],2),('timeout',[sys.executable,'-c','import time;time.sleep(2)'],.15),('same_group',[sys.executable,'-c','import subprocess,time;subprocess.Popen(["/bin/sleep","2"]);time.sleep(2)'],.15),('exited_leader',[sys.executable,'-c','import subprocess;subprocess.Popen(["/bin/sleep",".3"])'],2)]
for name,cmd,limit in cases:
 result,_=execute(cmd,base/name,limit)
 assert result['owned_group_clean'],(name,result)
 if name in ('timeout','same_group'):assert result['error'] is not None
 else:assert result['error'] is None and result['returncode']==0
 print(json.dumps(dict(case=name,result=result)),flush=True)
