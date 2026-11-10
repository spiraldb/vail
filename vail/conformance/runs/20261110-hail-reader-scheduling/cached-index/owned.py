from pathlib import Path
import subprocess,os,time,signal,json,psutil

def run(command,out,env,limit=180):
 signals=[signal.SIGTERM,signal.SIGHUP,signal.SIGINT];old_handlers={x:signal.getsignal(x) for x in signals}
 pending=[];spawning=True
 def interrupted(signum,frame):
  if spawning:pending.append(signum)
  else:raise KeyboardInterrupt(f'signal {signum}')
 for x in signals:signal.signal(x,interrupted)
 out=Path(out);out.mkdir(parents=True,exist_ok=False);started=time.monotonic();known={};peak=0;failure=None;cleanup_error=None;p=None
 def live():
  a=[]
  for pid,born in list(known.items()):
   try:
    x=psutil.Process(pid)
    if x.create_time()==born and x.uids().effective==os.geteuid() and x.status()!=psutil.STATUS_ZOMBIE:a.append(x)
   except psutil.NoSuchProcess:pass
  return a
 def absent():
  try:os.killpg(p.pid,0);return False
  except ProcessLookupError:return True
 def group_signal(sig):
  if absent():return
  assert any(os.getpgid(x.pid)==p.pid for x in live()),'Unknown group ownership; stop'
  os.killpg(p.pid,sig)
 try:
  with (out/'stdout').open('w') as stdout,(out/'stderr').open('w') as stderr:
   p=subprocess.Popen(command,env=env,cwd='/private/tmp',stdout=stdout,stderr=stderr,start_new_session=True);leader=psutil.Process(p.pid);known[p.pid]=leader.create_time();assert os.getpgid(p.pid)==p.pid
   spawning=False
   if pending:raise KeyboardInterrupt(f'deferred signal {pending[0]}')
   while p.poll() is None:
    for proc in live():
     try:
      for x in proc.children(recursive=True):known[x.pid]=x.create_time()
     except psutil.NoSuchProcess:pass
    rss=0
    for x in live():
     try:rss+=x.memory_info().rss
     except psutil.NoSuchProcess:pass
    peak=max(peak,rss)
    if rss>12*1024**3:failure='sampledcombinedRSS12GiB';break
    if time.monotonic()-started>limit:failure='worker deadline';break
    time.sleep(.05)
 finally:
  for x in signals:signal.signal(x,signal.SIG_IGN)
  if p is not None:
   try:
    if p.poll() is None:
     group_signal(signal.SIGTERM)
     try:p.wait(timeout=3)
     except subprocess.TimeoutExpired:group_signal(signal.SIGKILL);p.wait(timeout=3)
    else:p.wait()
    end=time.monotonic()+3
    while not absent() and time.monotonic()<end:time.sleep(.05)
    if not absent():
     group_signal(signal.SIGTERM);end=time.monotonic()+3
     while not absent() and time.monotonic()<end:time.sleep(.05)
    if not absent():
     group_signal(signal.SIGKILL);end=time.monotonic()+3
     while not absent() and time.monotonic()<end:time.sleep(.05)
    assert absent() and not live(),'Owned child/group survives; no next worker'
   except BaseException as e:cleanup_error=repr(e)
   receipt={'command':command,'leader':p.pid,'owned_identities':known,'seconds':time.monotonic()-started,'returncode':p.returncode,'failure':failure,'cleanup_error':cleanup_error,'peak_observed_combined_rss_bytes':peak,'group_absent':cleanup_error is None,'live_owned':len(live())}
   (out/'exit.json').write_text(json.dumps(receipt,indent=2)+'\n')
  for sig,handler in old_handlers.items():signal.signal(sig,handler)
 if cleanup_error or failure or p.returncode:raise RuntimeError({'receipt':receipt,'stderr_tail':(out/'stderr').read_text()[-2200:]})
 return receipt
