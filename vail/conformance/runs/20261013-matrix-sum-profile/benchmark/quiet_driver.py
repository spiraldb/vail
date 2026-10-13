import json,os,signal,subprocess,sys,time
from pathlib import Path
out=Path('/Users/danielking/projects/vail/vail/conformance/runs/20261013-matrix-sum-profile/benchmark')
out.mkdir(parents=True, exist_ok=True)
def processes():
    rows={}
    for line in subprocess.check_output(['ps','-axo','pid,ppid,command'],text=True).splitlines()[1:]:
        fields=line.strip().split(None,2)
        if len(fields)==3: rows[int(fields[0])]=(int(fields[1]),fields[2])
    return rows
rows=processes(); roots=set()
for pid,(parent,command) in rows.items():
    if '/worktrees/142f/vail/vail/target' in command and ('rustc ' in command or 'clang ' in command):
        current=pid
        while current in rows:
            if '/cargo ' in rows[current][1] or rows[current][1].startswith('cargo '):
                roots.add(current);break
            current=rows[current][0]
stopped={}
def stop(pid):
    if pid not in rows:return
    try: os.kill(pid,signal.SIGSTOP);stopped[pid]=rows[pid][1]
    except ProcessLookupError:pass
for pid in roots:stop(pid)
changed=True;desc=set(roots)
while changed:
    changed=False
    for pid,(parent,_) in rows.items():
        if parent in desc and pid not in desc:desc.add(pid);changed=True
for pid in desc:stop(pid)
# An independent watchdog restores paused jobs even if this driver dies.
restore="""import json,os,signal,subprocess,sys,time
time.sleep(300)
old=json.loads(sys.argv[1]); current={}
for line in subprocess.check_output(['ps','-axo','pid,command'],text=True).splitlines()[1:]:
 p=line.strip().split(None,1)
 if len(p)==2:current[p[0]]=p[1]
for pid,command in old.items():
 if current.get(pid)==command:
  try:os.kill(int(pid),signal.SIGCONT)
  except ProcessLookupError:pass
"""
watchdog=subprocess.Popen([sys.executable,'-c',restore,json.dumps(stopped)],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
control={'roots':list(roots),'stopped':stopped,'watchdog_seconds':300,'started':time.time()}
(out/'quiet-control.json').write_text(json.dumps(control,indent=2)+'\n')
try:
 for label,engine,python in [('before','vail','/private/tmp/vail-matrix-before/bin/python'),('after','vail','/private/tmp/vail-columnar-benchmark/bin/python'),('hail','hail','/private/tmp/vail-stock-hail-benchmark/bin/python')]:
  with (out/(label+'-quiet.log')).open('w') as log:
   subprocess.run(['ps','-axo','pid,ppid,%cpu,comm'],stdout=(out/(label+'-quiet-processes.txt')).open('w'),check=True)
   env=dict(os.environ,JAVA_HOME='/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home',SPARK_LOCAL_IP='127.0.0.1')
   command=[python,'/Users/danielking/projects/vail/vail/benchmarks/compare_hail.py','--engine',engine,'--case','matrix_entry_sum','--warmups','3','--iterations','5','--workdir','/private/tmp/'+label+'-bench-matrix-quiet','--output',str(out/(label+'-quiet.json'))]
   print('Running',label,engine,flush=True);subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=240)
finally:
 current=processes();resumed=[]
 for pid,command in stopped.items():
  if pid in current and current[pid][1]==command:
   try:os.kill(pid,signal.SIGCONT);resumed.append(pid)
   except ProcessLookupError:pass
 watchdog.terminate()
 control.update(resumed=resumed,ended=time.time());(out/'quiet-control.json').write_text(json.dumps(control,indent=2)+'\n')
 print('Resumed',len(resumed),'build processes',flush=True)
