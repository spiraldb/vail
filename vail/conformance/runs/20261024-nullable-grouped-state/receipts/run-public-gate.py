import json,os,pathlib,re,subprocess,sys
r=pathlib.Path('/private/tmp/vail-nullable-grouped-gate');source=pathlib.Path('/Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail');label=sys.argv[1]
python=r/'candidate-env/bin/python'
files=re.findall(r'python/tests/test_[a-z_]+\.py',(source/'conformance/runs/20261024-coordinate-current-main-integration/h050-main-gate.sh').read_text())+['python/tests/test_native_array_length_projection.py','python/tests/test_batched_list_sets.py']
cmd=[str(python),'-m','pytest','-q',*files,'--junitxml',str(r/f'{label}-public.xml')];(r/f'{label}-public-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
with open(r/f'{label}-public.log','w') as out:p=subprocess.run(cmd,cwd=source,stdout=out,stderr=subprocess.STDOUT,env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
(r/f'{label}-public-exit.txt').write_text(str(p.returncode)+'\n');sys.exit(p.returncode)
