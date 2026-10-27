"""Only the recorded per-case worker session, bounded and positively retired."""
import json,os,signal,time
import psutil

def retire(process,directory):
 receipt=directory/'worker-identity.json';events=[];deadline=time.monotonic()+12
 final=directory/'worker-cleanup.json'
 try:
  if getattr(process,'_final_scan_retired',False):return
  if not receipt.exists():
   process.join(0)
   if process.exitcode is None:raise RuntimeError('Worker identity unavailable; no group signal')
   raise RuntimeError('Worker exited without session receipt; descendant ownership unknown')
  identity=json.loads(receipt.read_text());pid=process.pid
  if identity['pid']!=pid or identity['pgid']!=pid or identity['sid']!=pid or identity['uid']!=os.geteuid():raise RuntimeError('Worker session identity invalid')
  for sig in (signal.SIGTERM,signal.SIGKILL):
   phase=min(deadline,time.monotonic()+5);sent=False
   while True:
    if time.monotonic()>=deadline:raise TimeoutError('Worker retirement deadline')
    process.join(0);members=[]
    for candidate in psutil.process_iter(['pid','uids','create_time']):
     if time.monotonic()>=deadline:raise TimeoutError('Worker inventory deadline')
     try:
      if os.getpgid(candidate.pid)!=pid:continue
      if candidate.info['uids'].effective!=identity['uid'] or os.getsid(candidate.pid)!=pid:raise RuntimeError('Worker member ownership unknown')
      if candidate.pid==pid and candidate.info['create_time']!=identity['start']:raise RuntimeError('Worker leader reused')
      members.append(candidate.pid)
     except (psutil.NoSuchProcess,ProcessLookupError):continue
    events.append({'time':time.monotonic(),'members':members,'leader_exitcode':process.exitcode})
    if not members:
     if process.exitcode is None:raise RuntimeError('No worker group but child not reaped')
     process._final_scan_retired=True
     final.write_text(json.dumps({'clean':True,'observations':events})+'\n');return
    if time.monotonic()>=deadline:raise TimeoutError('Worker signal deadline')
    if not sent:
     # Read/check is not atomic with killpg; EPERM/races remain failures.
     os.killpg(pid,sig);events[-1]['signal']=int(sig);sent=True
    if time.monotonic()>=phase:break
    time.sleep(min(.1,max(0,phase-time.monotonic())))
  raise RuntimeError('Worker session remains after TERM/KILL')
 except BaseException as error:
  final.write_text(json.dumps({'clean':False,'error':repr(error),'observations':events})+'\n');raise
