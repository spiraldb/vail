"""Supplement definitions which full pytest could not collect; keep assertions intact."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import subprocess
import sys
import time
from run_suite import PROJECT, ROOT, SOURCE, ENV, save

if __name__=='__main__':
    while not (ROOT/'pytest-modules.json').exists() or len(json.loads((ROOT/'pytest-modules.json').read_text()))<62:
        time.sleep(2)
    observed=set()
    for path in (ROOT/'pytest').rglob('*.jsonl'):
        module=path.parts[-3].replace('__','/')
        for line in path.read_text().splitlines():
            e=json.loads(line)
            if e['kind']=='inventory':
                for nodeid in e['items']:
                    parts=nodeid.split('::')[1:]
                    if parts:
                        parts[-1]=parts[-1].split('[')[0]
                        observed.add((module,'.'.join(parts)))
    inventory=json.loads((ROOT/'inventory.json').read_text())
    cases=[r for r in inventory if (r['module'],r['name']) not in observed]
    save(ROOT/'supplement-selection.json',cases)
    directory=ROOT/'bodies';directory.mkdir(exist_ok=True)
    def run(pair):
        i,row=pair;out=directory/f'{i:04d}.json'
        if not out.exists():
            command=[sys.executable,str(PROJECT/'conformance/full_hail_bodies.py'),
                '--suite',str(SOURCE),'--output',str(out),'--worker-source',str(SOURCE/row['module']),
                '--worker-name',row['name']]
            problem=None
            with out.with_suffix('.log').open('w') as log:
                try:
                    proc=subprocess.run(command,env=ENV,cwd=PROJECT,stdout=log,stderr=log,timeout=180)
                    if proc.returncode:problem=f'worker exited {proc.returncode}'
                except subprocess.TimeoutExpired:problem='worker exceeded 180 seconds'
            if problem:
                data=json.loads(out.read_text()) if out.exists() else dict(module=row['module'],name=row['name'])
                data.update(status='worker_blocked',worker_error=problem);save(out,data)
        return json.loads(out.read_text())
    results=[]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(run,pair) for pair in enumerate(cases)]
        for future in as_completed(futures):
            row=future.result();results.append(row);save(ROOT/'supplement-results.json',results)
            print(f'{len(results)}/{len(cases)} {row.get("status")} {row["module"]}:{row["name"]}',flush=True)
