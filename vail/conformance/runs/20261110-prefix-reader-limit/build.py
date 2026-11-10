from pathlib import Path
import os,json,subprocess
r=Path(__file__).parent;repo=Path('/private/tmp/vail-list-flatmap-20261106/vail');py='/Users/danielking/projects/vail/vail/.venv/bin/python'
e=os.environ.copy()
for k,v in json.loads(Path('/private/tmp/vail-allocators-20261109/pinned-sdk/build-environment.json').read_text()).items():
 if v is None:e.pop(k,None)
 else:e[k]=v
e.update(UV_CACHE_DIR='/private/tmp/vail-uv-cache',RAYON_NUM_THREADS='2',OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
for k in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']:e.pop(k,None)
def run(args,name,limit):
 print(name,flush=True)
 with (r/(name+'.log')).open('w') as f:subprocess.run(args,cwd=repo,env=e,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=limit)
wheels=r/'wheels';wheels.mkdir(exist_ok=True)
run(['nice','-n','10',py,'-m','maturin','build','--release','--offline','--locked','--interpreter',py,'--features','pyo3/extension-module','--out',str(wheels),'--manifest-path','crates/vail-python/Cargo.toml'],'build',1200)
venv=r/'env';exe=venv/'bin/python'
run(['uv','venv','--clear','--python',py,str(venv)],'venv',30)
wheel=next(wheels.glob('*.whl'));run(['uv','pip','install','--offline','--python',str(exe),'--no-deps','--link-mode','copy',str(wheel)],'install',60)
site=next((venv/'lib').glob('python*/site-packages'));(site/'vail-test-dependencies.pth').write_text('/Users/danielking/projects/vail/vail/.venv/lib/python3.12/site-packages\n')
run([str(exe),'-c',"import vail,vail._native as n,json,pathlib,hashlib;print(json.dumps({'package':vail.__file__,'native':n.__file__,'allocator':n._benchmark_allocator,'sha256':hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))"],'identity',30)
(r/'ready.json').write_text(json.dumps({'source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'python':str(exe),'identity':json.loads((r/'identity.log').read_text())},indent=2)+'\n');print('NORMAL COMBINED WHEEL READY',flush=True)
