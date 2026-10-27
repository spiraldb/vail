"""One metadata-only VM boot; bounded owned groups and proactive STOP first."""
import argparse,hashlib,json,os,re,shlex,signal,subprocess,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
GCLOUD='/Users/danielking/projects/google-cloud-sdk/bin/gcloud'
VM='vail-bench-20261021';IDENTITY=['--project=pyspiral-dev','--zone=us-east1-b']
REMOTE=Path('/home/danielking/supported-scan-metadata-20261027')
p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();out=Path(a.out).resolve()
if not out.is_relative_to('/private/tmp') or out.exists():raise RuntimeError('Fresh owned local /private/tmp output required')
out.mkdir();record={'error':None,'owned_vm':False,'guest_complete':False,'download_verified':False,'provider_stopped':False,'unresolved_host_groups':[]}
def digest(path):
 with Path(path).open('rb') as source:return hashlib.file_digest(source,'sha256').hexdigest()
def stop_owned(proc):
 if proc is None:return
 def complete():
  exited=proc.poll() is not None
  try:os.killpg(proc.pid,0)
  except ProcessLookupError:return exited
  return False
 for sig in (signal.SIGTERM,signal.SIGKILL):
  if complete():return
  try:os.killpg(proc.pid,sig)
  except ProcessLookupError:pass
  deadline=time.monotonic()+3
  while time.monotonic()<deadline:
   if complete():return
   time.sleep(.05)
 if not complete():raise RuntimeError(f'Owned group unresolved:{proc.pid}')
def run(name,command,seconds):
 folder=out/name;folder.mkdir();(folder/'command.json').write_text(json.dumps(command,indent=2)+'\n');proc=None;error=None;clean=False;started=time.monotonic()
 try:
  with (folder/'stdout').open('wb') as stdout,(folder/'stderr').open('wb') as stderr:
   proc=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True)
   while proc.poll() is None:
    if time.monotonic()-started>seconds:raise TimeoutError(f'{name}:{seconds}s')
    if (folder/'stdout').stat().st_size+(folder/'stderr').stat().st_size>2*1024**2:raise RuntimeError('2MiB command log cap')
    time.sleep(.05)
   if (folder/'stdout').stat().st_size+(folder/'stderr').stat().st_size>2*1024**2:raise RuntimeError('2MiB command log cap')
 except BaseException as e:error=repr(e)
 finally:
  # Interruptions stay failures; bounded cleanup completes before another command.
  handlers={sig:signal.signal(sig,signal.SIG_IGN) for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
  try:stop_owned(proc);clean=True
  except BaseException as e:error=f'{error}; cleanup:{e!r}';record['unresolved_host_groups'].append(None if proc is None else proc.pid)
  for sig,h in handlers.items():signal.signal(sig,h)
  result={'returncode':None if proc is None else proc.returncode,'error':error,'owned_group_clean':clean,'elapsed_seconds':time.monotonic()-started}
  (folder/'result.json').write_text(json.dumps(result,indent=2)+'\n')
 if error:raise RuntimeError(f'{name}:{error}')
 return result,(folder/'stdout').read_text()
def require(name,command,seconds):
 result,text=run(name,command,seconds)
 if result['returncode']!=0:raise RuntimeError(f'{name}:exit{result["returncode"]}')
 return text
def gcloud(*args):return [GCLOUD,'compute',*args,*IDENTITY]
def ssh(name,command,seconds):return require(name,gcloud('ssh',VM,'--quiet','--command',command),seconds)
def stop_vm():
 require('provider-stop',gcloud('instances','stop',VM,'--quiet'),120)
 provider=json.loads(require('provider-after',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),30))
 record['provider_stopped']=provider['status']=='TERMINATED'
 if not record['provider_stopped']:raise RuntimeError('Provider not verified TERMINATED')
def interrupted(sig,frame):raise InterruptedError(f'signal{sig}')
for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
try:
 assets=json.loads((HERE/'ASSETS.json').read_text())
 for name,sha in assets.items():
  if digest(HERE/name)!=sha:raise RuntimeError(f'Local protocol fingerprint differs:{name}')
 record['protocol_assets']=assets
 provider=json.loads(require('provider-initial',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),30))
 if provider['status']!='TERMINATED':raise RuntimeError('Exclusive TERMINATED primary required; no running workload takeover')
 require('provider-bound',gcloud('instances','set-scheduling',VM,'--max-run-duration=900s','--instance-termination-action=STOP','--quiet'),60)
 provider=json.loads(require('provider-before-boot',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),30))
 if provider['status']!='TERMINATED' or provider['scheduling']['instanceTerminationAction']!='STOP' or not 0<int(provider['scheduling']['maxRunDuration']['seconds'])<=900:raise RuntimeError('Provider STOP bound not verified before boot')
 record['owned_vm']=True # Set BEFORE start: timeout may still leave a running VM.
 require('provider-start',gcloud('instances','start',VM,'--quiet'),120)
 ready=False;deadline=time.monotonic()+60
 for attempt in range(6):
  if time.monotonic()>=deadline:break
  result,_=run(f'ssh-readiness-{attempt}',gcloud('ssh',VM,'--quiet','--command','true'),min(8,max(1,deadline-time.monotonic())))
  if result['returncode']==0:ready=True;break
  time.sleep(2)
 if not ready:raise RuntimeError('Boot SSH readiness deadline; no metadata workload retry')
 ssh('guest-guard','sudo -n systemd-run --unit=supported-scan-metadata-deadline --on-active=12m /usr/sbin/shutdown -h now',15)
 ssh('fresh-protocol',f'test ! -e {shlex.quote(str(REMOTE))} && mkdir -p {shlex.quote(str(REMOTE/"protocol"))}',10)
 require('upload',gcloud('scp',str(HERE/'guest.py'),f'{VM}:{REMOTE}/protocol/guest.py','--quiet'),30)
 text=ssh('guest-metadata',shlex.join(['python3',str(REMOTE/'protocol/guest.py'),'--out',str(REMOTE/'attempt-1'),'--expected-sha',assets['guest.py']]),150)
 packed=json.loads(text.strip().splitlines()[-1])
 if packed['archive']!=str(REMOTE/'attempt-1.tar.gz') or not 0<packed['bytes']<=20*1024**2 or not re.fullmatch('[0-9a-f]{64}',packed['sha256']):raise RuntimeError('Guest archive response contract differs')
 record['guest_complete']=True;record['guest_result']=packed
 require('download',gcloud('scp',f'{VM}:{packed["archive"]}',str(out/'metadata.tar.gz'),'--quiet'),60)
 if digest(out/'metadata.tar.gz')!=packed['sha256']:raise RuntimeError('Downloaded archive fingerprint differs')
 record['download_verified']=True
except BaseException as e:record['error']=repr(e)
finally:
 for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,signal.SIG_IGN)
 # Any SSH failure/timeout/unverified guest status stops provider FIRST.
 # There is no recovery/report/pack command on that path; guest disk is retained.
 if record['owned_vm']:
  try:stop_vm()
  except BaseException as e:record['error']=f'{record["error"]}; provider cleanup:{e!r}'
 (out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
if record['error']:raise SystemExit(1)
