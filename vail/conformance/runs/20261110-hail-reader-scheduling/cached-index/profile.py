from pathlib import Path
import json,os
from owned import run
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());d=Path('/Users/danielking/.codex/visualizations/2026/10/23/01a1666a-5c91-7043-b5a8-da5a82f930bd/hail-layout-async-io-20261110');d.mkdir(exist_ok=True)
e={k:v for k,v in os.environ.items() if not k.startswith(('VAIL_','MIMALLOC_','MALLOC_','JEMALLOC_','_RJEM_','Malloc')) and k not in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']};e.update(PYTHONMALLOC='default',ARROW_DEFAULT_MEMORY_POOL='mimalloc',RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
cmd=['/opt/homebrew/bin/samply','record','--save-only','--duration','8','--rate','1000','--unstable-presymbolicate','--profile-name','Vail group-by-collect: async I/O and worker decoding','--output',str(d/'group-by-collect.json.gz'),cfg['candidate']['python'],str(r/'profile-worker.py')]
receipt=run(cmd,r/'profile-capture',e,90);(r/'profile-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');(r/'profile-path.json').write_text(json.dumps({'durable':str(d)})+'\n');print('PROFILE saved',d)
