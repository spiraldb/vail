from pathlib import Path
import os,time,subprocess,json
out=Path('/Users/danielking/.codex/worktrees/shared-annotation-aggregates/vail/vail/conformance/runs/20261023-offset-array-length')
project=out.parents[2]
deadline=time.monotonic()+900
while 'Installed vail-0.1.0a1' not in Path('/tmp/outer-length-build.log').read_text():
 if time.monotonic()>deadline:raise RuntimeError('build deadline')
 time.sleep(2)
python='/tmp/shared-annotation-env/bin/python'
tests=['outer_array_length','packed_coordinate_counts','local_lambda_cse','kernel_optimizer_contract','shared_matrix_annotations','shared_list_reducers','matrix_column_coordinate_callbacks','array_agg_coordinate_callbacks','array_agg_coordinate_dag','matrix_array_agg','coordinate_product_boolean','coordinate_extrema','coordinate_stddev','coordinate_correlation','coordinate_histogram','logical_nested_aggregation','array_methods','array_map','empty_array','empty_nested_list_storage']
with open('/tmp/outer-length-python-tests.log','w')as f:subprocess.run([python,'-m','pytest','-q',*[str(project/'python/tests'/('test_'+t+'.py'))for t in tests]],stdout=f,stderr=subprocess.STDOUT,check=True)
print('PYTHON TESTS PASS',flush=True)
while 'test result:' not in Path('/tmp/outer-length-native-tests.log').read_text() or 'array_methods-' not in Path('/tmp/outer-length-native-tests.log').read_text() or 'outer_array_length-' not in Path('/tmp/outer-length-native-tests.log').read_text():
 if time.monotonic()>deadline:raise RuntimeError('native tests deadline')
 time.sleep(2)
# Native tool must have completed: two test binaries, both summaries.
while Path('/tmp/outer-length-native-tests.log').read_text().count('test result: ok') < 2:
 if time.monotonic()>deadline:raise RuntimeError('native tests deadline')
 time.sleep(2)
assert 'Installed vail-0.1.0a1' in Path('/tmp/outer-length-baseline-build.log').read_text()
print('BUILDS AND TESTS COMPLETE',flush=True)
env=os.environ.copy();env['EXECUTION_CONCURRENCY']='32';env.pop('PYTHONPATH',None)
rows=[]
for rep in range(1,4):
 for variant in (['baseline','candidate']if rep%2 else ['candidate','baseline']):
  py='/tmp/packed-count-baseline-env/bin/python' if variant=='baseline'else python
  p=subprocess.run([py,str(out/'experiment.py')],env=env,text=True,capture_output=True)
  (out/f'{variant}-{rep}.log').write_text(p.stdout+p.stderr);p.check_returncode()
  result=json.loads(p.stdout.strip().splitlines()[-1]);result.update(variant=variant,repetition=rep);rows.append(result)
  (out/'pairs.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(result),flush=True)
for mode,py in [('before','/tmp/packed-count-baseline-env/bin/python'),('after',python)]:
 with open(f'/tmp/outer-length-{mode}-profile.log','w')as f:subprocess.run(['samply','record','--save-only','-r','200','-o',f'/tmp/outer-length-{mode}-profile.json.gz','--',py,str(out/'experiment.py')],env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
print('COMPARISON AND PROFILES COMPLETE',flush=True)
