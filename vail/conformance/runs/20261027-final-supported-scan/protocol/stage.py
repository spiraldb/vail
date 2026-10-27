"""One bounded guest stage; failure means provider STOP before collection."""
import argparse,hashlib,json,os,signal,subprocess,sys,tarfile,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
p=argparse.ArgumentParser();p.add_argument('name',choices=['enroll','preflight','rep1','rep2','rep3','final']);a=p.parse_args()
assets=json.loads((HERE/'ASSETS.json').read_text())
for name,expected in assets.items():
 digest=hashlib.sha256((HERE/name).read_bytes()).hexdigest()
 if digest!=expected:raise RuntimeError('Protocol asset mismatch:'+name)
lock=json.loads((HERE/'artifact-lock.json').read_text());python=lock['current']['python'];output=ROOT/'results';output.mkdir(exist_ok=True)
cap=7200 if a.name.startswith('rep') else 3600
folder=output/a.name
if folder.exists():raise RuntimeError('Fresh stage required; no rerun/resume substitution')
command=[python,str(HERE/'enroll.py'),'--out',str(output/'enrollment.json')] if a.name=='enroll' else ([python,str(HERE/'enroll.py'),'--out',str(output/'final-enrollment.json'),'--check',str(output/'enrollment.json'),'--full'] if a.name=='final' else [python,str(HERE/'phase.py'),'--phase',a.name,'--out',str(folder),'--enrollment',str(output/'enrollment.json')])
# stage wrapper and phase have independent finite ceilings, no dynamic extension.
signal.alarm(cap+60)
proc=subprocess.Popen(command,start_new_session=True);started=time.monotonic();status=None;success=False
try:
 while proc.poll() is None:
  if time.monotonic()-started>=cap:raise TimeoutError('Stage cap')
  if os.statvfs(ROOT).f_bavail*os.statvfs(ROOT).f_frsize<10*1024**3:raise RuntimeError('10GiB free-disk floor')
  time.sleep(.5)
 status=proc.returncode
 if status:raise RuntimeError('Stage failed; provider STOP before collection')
 try:os.killpg(proc.pid,0)
 except ProcessLookupError:pass
 else:raise RuntimeError('Stage command group survives; STOP first')
 # Only completed stages are packed; archive never contains input payloads/binaries.
 archive=ROOT/(a.name+'.tar.gz');bytes_added=0
 with tarfile.open(archive,'w:gz') as target:
  candidates=[output/'enrollment.json'] if a.name=='enroll' else ([output/'final-enrollment.json'] if a.name=='final' else sorted(folder.rglob('*')))
  for path in candidates:
   if path.is_file():
    bytes_added+=path.stat().st_size
    if bytes_added>1024**3:raise RuntimeError('1GiB stage evidence cap')
    target.add(path,arcname=str(path.relative_to(output)),recursive=False)
 digest=hashlib.sha256()
 with archive.open('rb') as stream:
  while True:
   data=stream.read(1024**2)
   if not data:break
   digest.update(data)
 success=True
 print(json.dumps({'archive':str(archive),'sha256':digest.hexdigest(),'bytes':archive.stat().st_size,'stage':a.name,'seconds':time.monotonic()-started}),flush=True)
finally:
 # Do not claim group cleanliness on any interrupted/failed stage. The host's
 # exception path immediately STOPs provider, with guest evidence left on disk.
 (output/(a.name+'-supervisor.json')).write_text(json.dumps({'returncode':status,'seconds':time.monotonic()-started,'success':success})+'\n')
