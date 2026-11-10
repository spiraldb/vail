from pathlib import Path
import json,os
from owned import run
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());e={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc',RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
args=['/opt/homebrew/bin/samply','record','--save-only','--duration','8','--rate','1000','--unstable-presymbolicate','--profile-name','Vail grouped collect after partial-state copy removal','--output',str(r/'collect-profile.json.gz'),cfg['candidate']['python'],str(r/'profile-worker.py')]
receipt=run(args,r/'profile-capture',e,90);(r/'profile-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print('PROFILE COMPLETE',flush=True)
