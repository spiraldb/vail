from pathlib import Path
import json,os,subprocess
r=Path(__file__).parent
ready=json.loads((r/'ready.json').read_text()); assert ready['source']=='3fc119c73955537e5227b27875fcf125b3493484'
repo=Path('/private/tmp/vail-list-flatmap-20261106/vail')
files=['python/tests/test_table_show.py','python/tests/test_table_explode.py','python/tests/test_matrix_table.py','python/tests/test_randomness.py']
args=[str(r/'env/bin/python'),'-m','pytest','-q',*files,'-k','show or explode or head_tail or entries_reuses or entries_sorts or entries_column_permutation or entries_does_not_assume or entries_orders_duplicate or row_seeded_values']
e=os.environ.copy();e.update(RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
for k in ['PYTHONPATH','DYLD_INSERT_LIBRARIES','LD_PRELOAD']:e.pop(k,None)
with (r/'python-tests.log').open('w') as f:p=subprocess.run(args,cwd=repo,env=e,stdout=f,stderr=subprocess.STDOUT,timeout=180)
(r/'python-tests.json').write_text(json.dumps({'command':args,'source':ready['source'],'native_sha256':ready['identity']['sha256'],'exit_code':p.returncode},indent=2)+'\n')
print((r/'python-tests.log').read_text());raise SystemExit(p.returncode)
