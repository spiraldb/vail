"""One metadata-only VM boot; bounded owned groups and proactive STOP first."""
import argparse,hashlib,json,os,re,shlex,signal,subprocess,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
GCLOUD='/Users/danielking/projects/google-cloud-sdk/bin/gcloud'
VM='vail-bench-20261021';IDENTITY=['--project=pyspiral-dev','--zone=us-east1-b']
REMOTE=Path('/home/danielking/list-index-bench-20261030')
p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--stage',choices=['targeted'],required=True);a=p.parse_args();out=Path(a.out).resolve()
if not out.is_relative_to('/private/tmp') or out.exists():raise RuntimeError('Fresh owned local /private/tmp output required')
out.mkdir();record={'error':None,'owned_vm':False,'guest_complete':False,'stage':a.stage,'download_verified':False,'provider_stopped':False,'unresolved_host_groups':[]}
def digest(path):
 with Path(path).open('rb') as source:return hashlib.file_digest(source,'sha256').hexdigest()
from owned import execute
def run(name,command,seconds,*,deadline=None):
 result,text=execute(command,out/name,seconds,deadline=deadline)
 if not result['owned_group_clean']:record['unresolved_host_groups'].append(result.get('identity'))
 if result['error']:raise RuntimeError(f"{name}:{result['error']}")
 return result,text
def require(name,command,seconds,*,deadline=None):
 result,text=run(name,command,seconds,deadline=deadline)
 if result['returncode']!=0:raise RuntimeError(f'{name}:exit{result["returncode"]}')
 return text
def gcloud(*args):return [GCLOUD,'compute',*args,*IDENTITY]
SSHFLAGS=['--ssh-flag=-oConnectTimeout=10','--ssh-flag=-oConnectionAttempts=1','--ssh-flag=-oBatchMode=yes','--ssh-flag=-oControlMaster=no','--ssh-flag=-oControlPath=none']
def ssh(name,command,seconds):return require(name,gcloud('ssh',VM,'--quiet',*SSHFLAGS,'--command',command),seconds)
def stop_vm():
 started=time.monotonic();stop_error=None;describe_errors=[]
 try:require('provider-stop',gcloud('instances','stop',VM,'--async','--quiet'),18,deadline=started+30)
 except BaseException as error:
  stop_error=repr(error);record['provider_stop_error']=stop_error
 deadline=min(started+210,time.monotonic()+180);attempt=0
 while True:
  remaining=deadline-time.monotonic()
  if remaining<=12:break
  try:
   result,text=run(f'provider-after-{attempt}',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),min(18,remaining-12),deadline=min(deadline,time.monotonic()+30))
   if result['returncode']!=0:raise RuntimeError(f"describe exit{result['returncode']}")
   provider=json.loads(text);record['provider_final_status']=provider.get('status','UNKNOWN');record['provider_stopped']=provider.get('status')=='TERMINATED'
   if record['provider_stopped']:break
  except BaseException as error:
   describe_errors.append(repr(error));record['provider_final_status']='UNKNOWN'
  attempt+=1;time.sleep(min(2,max(0,deadline-time.monotonic()-12)))
 record['provider_describe_errors']=describe_errors
 if stop_error or describe_errors or not record['provider_stopped']:
  raise RuntimeError(f'Provider cleanup failed: stop={stop_error}; describe={describe_errors}; status={record.get("provider_final_status","UNKNOWN")}')
def interrupted(sig,frame):raise InterruptedError(f'signal{sig}')
for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
try:
 import tarfile
 assets=json.loads((HERE/'ASSETS.json').read_text())
 for name,sha in assets.items():
  if digest(HERE/name)!=sha:raise RuntimeError('Protocol asset changed:'+name)
 provider=json.loads(require('provider-initial',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),30))
 if provider['status']!='TERMINATED':raise RuntimeError('Exclusive stopped VM required')
 require('provider-bound',gcloud('instances','set-scheduling',VM,'--max-run-duration=2700s','--instance-termination-action=STOP','--quiet'),60)
 provider=json.loads(require('provider-before-boot',gcloud('instances','describe',VM,'--format=json(status,scheduling)'),30))
 if provider['status']!='TERMINATED' or provider['scheduling']['instanceTerminationAction']!='STOP' or int(provider['scheduling']['maxRunDuration']['seconds'])!=2700:raise RuntimeError('45min provider STOP guard unverified')
 record['owned_vm']=True
 require('provider-start',gcloud('instances','start',VM,'--quiet'),120)
 ready=False;deadline=time.monotonic()+90;attempt=0
 while time.monotonic()<deadline-12:
  result,_=run(f'ssh-readiness-{attempt}',gcloud('ssh',VM,'--quiet',*SSHFLAGS,'--command','true'),min(48,deadline-time.monotonic()-12),deadline=min(deadline,time.monotonic()+60))
  if result['returncode']==0:ready=True;break
  attempt+=1;time.sleep(2)
 if not ready:raise RuntimeError('90s SSH readiness exhausted')
 ssh('guest-guard','sudo -n systemd-run --unit=targeted-20261028-attempt3-deadline --on-active=2580s /usr/sbin/shutdown -h now',15)
 ssh('fresh-root',f'test ! -e {REMOTE} && mkdir -p {REMOTE}',10)
 bundle=out/'bundle.tar.gz'
 with tarfile.open(bundle,'w:gz') as target:
  for name in [*assets,'ASSETS.json']:target.add(HERE/name,arcname='protocol/'+name,recursive=False)
 if bundle.stat().st_size>128*1024**2:raise RuntimeError('128MiB upload cap')
 require('upload',gcloud('scp',str(bundle),f'{VM}:{REMOTE}/bundle.tar.gz','--quiet'),120)
 ssh('unpack',f'cd {REMOTE} && echo {shlex.quote(digest(bundle)+"  bundle.tar.gz")} | sha256sum -c - && tar -xzf bundle.tar.gz',30)
 python='/home/danielking/h012-matched-20261027/run-1/gate/candidate-env/bin/python'
 text=ssh('guest',shlex.join([python,str(REMOTE/'protocol/guest.py')]),2520)
 packed=json.loads(text.strip().splitlines()[-1])
 if packed['archive']!=str(REMOTE/'receipts.tar.gz') or not 0<packed['bytes']<=1024**3 or not re.fullmatch('[0-9a-f]{64}',packed['sha256']):raise RuntimeError('Receipt archive contract differs')
 record['guest_complete']=True;record['guest_result']=packed
 require('download',gcloud('scp',f'{VM}:{packed["archive"]}',str(out/'receipts.tar.gz'),'--quiet'),180)
 if digest(out/'receipts.tar.gz')!=packed['sha256']:raise RuntimeError('Receipt archive SHA differs')
 record['download_verified']=True
except BaseException as e:record['error']=repr(e)
finally:
 for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):signal.signal(sig,signal.SIG_IGN)
 if record['owned_vm']:
  try:stop_vm()
  except BaseException as e:record['error']=f'{record["error"]}; provider cleanup:{e!r}'
 (out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
if record['error']:raise SystemExit(1)
