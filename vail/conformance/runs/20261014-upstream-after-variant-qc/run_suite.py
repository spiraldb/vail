"""Full upstream pytest modules, isolated parallel workers and timeout continuation."""
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

PROJECT = Path(__file__).resolve().parents[3]
ROOT = Path(__file__).resolve().parent
SOURCE = PROJECT.parent / 'hail/hail/python/test/hail'
ENV = dict(os.environ, PYTHONPATH=str(PROJECT/'python')+os.pathsep+str(PROJECT/'conformance'))
FILES = sorted(SOURCE.rglob('test_*.py'))

def save(path, data):
    temporary=path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(data,indent=2)+'\n'); temporary.replace(path)

def events(path):
    if not path.exists(): return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

def run(file):
    module=str(file.relative_to(SOURCE)); label=module.replace('/','__')
    attempts=[]; deselected=set(); attempt=0
    while True:
        directory=ROOT/'pytest'/label/f'attempt-{attempt:02d}'
        directory.mkdir(parents=True,exist_ok=True)
        manifest=directory/'modules.json'
        if not manifest.exists():
            cmd=[sys.executable,str(PROJECT/'conformance/full_hail_pytest.py'),
                 '--source',str(SOURCE),'--output',str(directory),'--include-backend',
                 '--module',module,'--per-test-timeout','180','--module-timeout','1800']
            for node in sorted(deselected):cmd.extend(['--deselect',node])
            with (directory/'driver.log').open('w') as log:
                proc=subprocess.run(cmd,env=ENV,cwd=PROJECT,stdout=log,stderr=log)
            if not manifest.exists():
                return dict(module=module,driver_error=f'runner exit {proc.returncode}',attempts=attempts)
        result=json.loads(manifest.read_text())[0]
        attempts.append(dict(directory=str(directory.relative_to(ROOT)),**result))
        ev=events(directory/(label+'.jsonl'))
        for e in ev:
            if e['kind']=='test' and e['when']=='teardown':deselected.add(e['nodeid'])
        if not result.get('timeout'):
            break
        active=result.get('nodeid')
        if active:deselected.add(active)
        inventory=next((e['items'] for e in ev if e['kind']=='inventory'),[])
        pending=set(inventory)-deselected
        if not active or not pending:break
        attempt+=1
        save(ROOT/'pytest'/label/'continuation.json',dict(deselected=sorted(deselected),pending=sorted(pending)))
    return dict(module=module,attempts=attempts)

if __name__=='__main__':
    inventory=[]
    for file in FILES:
        for node in ast.parse(file.read_text()).body:
            for member in (node.body if isinstance(node,ast.ClassDef) else [node]):
                if isinstance(member,(ast.FunctionDef,ast.AsyncFunctionDef)) and member.name.startswith('test_'):
                    inventory.append(dict(module=str(file.relative_to(SOURCE)),name=(node.name+'.' if isinstance(node,ast.ClassDef) else '')+member.name,line=member.lineno,async_test=isinstance(member,ast.AsyncFunctionDef)))
    save(ROOT/'inventory.json',inventory)
    save(ROOT/'provenance.json',dict(commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=PROJECT,text=True).strip(),
        upstream_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=PROJECT.parent/'hail',text=True).strip(),
        native_sha256=hashlib.sha256((PROJECT/'python/vail/_native.abi3.so').read_bytes()).hexdigest(),
        modules=len(FILES),definitions=len(inventory),workers=4,per_test_timeout=180,module_timeout=1800,started_at=time.time()))
    results=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(run,file):file for file in FILES}
        for future in as_completed(futures):
            result=future.result();results.append(result)
            save(ROOT/'pytest-modules.json',sorted(results,key=lambda x:x['module']))
            print(f'{len(results)}/{len(FILES)} {result["module"]} '+str(result.get('driver_error',[(a.get('exit_code'),a.get('timeout')) for a in result['attempts']])),flush=True)
