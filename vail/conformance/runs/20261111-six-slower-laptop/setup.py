from pathlib import Path
import json,os,hashlib,shutil
from owned import run
r=Path(__file__).parent;p=json.loads((r/'pins.json').read_text());assert shutil.disk_usage(r).free>8*1024**3
e={k:v for k,v in os.environ.items()if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc'))and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc')
for name in ['profile','gnomad']:
 d=p['fixtures'][name];root=Path(d['root']);assert {str(q.relative_to(root))for q in root.rglob('*')if q.is_file()}==set(d['files'])
 for rel,w in d['files'].items():
  q=root/rel;assert q.stat().st_size==w['bytes']and hashlib.sha256(q.read_bytes()).hexdigest()==w['sha256'],q
receipt=run(['nice','-n','10',p['vail_python'],str(r/'setup-worker.py')],r/'enroll-native',e,360);assert json.loads((r/'setup-result.json').read_text())['passed'];d=p['fixtures']['native_profile'];d['partitions']=json.loads((r/'setup-result.json').read_text())['partitions'];root=Path(d['root']);d['files']={str(q.relative_to(root)):{'bytes':q.stat().st_size,'sha256':hashlib.sha256(q.read_bytes()).hexdigest()}for q in root.rglob('*')if q.is_file()};assert shutil.disk_usage(r).free>5*1024**3
package=Path(p['native']).parent;p['frontend_sha256']={str(q.relative_to(package)):hashlib.sha256(q.read_bytes()).hexdigest()for q in package.rglob('*.py')}
(r/'pins.json').write_text(json.dumps(p,indent=2)+'\n');(r/'setup-complete.json').write_text(json.dumps({'passed':True,'receipt':receipt,'native_input_files':len(d['files']),'native_input_bytes':sum(x['bytes']for x in d['files'].values()),'pins_sha256':hashlib.sha256((r/'pins.json').read_bytes()).hexdigest()},indent=2)+'\n');print('SETUP COMPLETE',flush=True)
