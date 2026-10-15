"""Execute cases accidentally excluded by pytest's prefix-based --deselect."""
from collections import defaultdict
import json
import subprocess
import sys
from run_suite import ROOT, SOURCE, PROJECT, ENV, save

if __name__=='__main__':
    report=json.loads((ROOT/'categorized.json').read_text());groups=defaultdict(list)
    for row in report['cases']:
        if row['status']=='not_executed' and row['source_definition']:groups[row['module']].append(row)
    modules=json.loads((ROOT/'pytest-modules.json').read_text())
    for module,rows in groups.items():
        label=module.replace('/','__');result=next(r for r in modules if r['module']==module)
        directory=ROOT/'pytest'/label/f'attempt-{len(result["attempts"]):02d}'
        directory.mkdir(parents=True,exist_ok=True)
        selection=directory/'selection.json';save(selection,dict(included=[dict(module=module,name=r['definition']) for r in rows]))
        command=[sys.executable,str(PROJECT/'conformance/full_hail_pytest.py'),'--source',str(SOURCE),'--output',str(directory),'--include-backend','--module',module,'--selection',str(selection),'--per-test-timeout','180','--module-timeout','1800']
        with (directory/'driver.log').open('w') as log:subprocess.run(command,env=ENV,cwd=PROJECT,stdout=log,stderr=log)
        attempt=json.loads((directory/'modules.json').read_text())[0]
        result['attempts'].append(dict(directory=str(directory.relative_to(ROOT)),**attempt))
        save(ROOT/'pytest-modules.json',modules)
        print(module,len(rows),attempt,flush=True)
