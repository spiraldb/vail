import os,pathlib,subprocess,json,hashlib,time,signal,statistics
root=pathlib.Path('/private/tmp/vail-real-fusion-bench-20261107')
py={'before':root/'baseline-env/bin/python','after':pathlib.Path('/private/tmp/vail-flatmap-test-env-20261106/bin/python')}
pins={'before':'415b2f35ac2244b97d60eff3f4d0463704f0f9f93189cea5162e50c09112af6e','after':'7c1e347f74d6a3e8cc353f468961d607541973dd84255ec3b02bbb039fbf2694'}
cases=['test_matrix_table_filter_entries','test_matrix_table_filter_entries_unfilter','test_matrix_table_array_arithmetic','test_split_multi_hts']
records=[]
for case in cases:
 for i,mode in enumerate(['before','after','after','before','before','after','after','before']):
  out=root/'runs'/f'{case}-{i}-{mode}';out.mkdir(parents=True)
  env={k:v for k,v in os.environ.items() if not k.startswith('VAIL_') and k!='PYTHONPATH'}
  env.update(RUN_MODE=mode,RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
  if i==0:env['ENROLL_REFERENCE']='1'
  with (out/'stdout').open('w') as stdout,(out/'stderr').open('w') as stderr:
   p=subprocess.Popen([str(py[mode]),str(root/'worker.py'),str(out),case],env=env,cwd='/private/tmp',stdout=stdout,stderr=stderr,start_new_session=True)
   try:code=p.wait(timeout=120)
   except subprocess.TimeoutExpired:
    os.killpg(p.pid,signal.SIGTERM)
    try:p.wait(timeout=3)
    except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=3)
    code=124
  (out/'exit.json').write_text(json.dumps({'returncode':code,'reaped':p.poll() is not None,'pid':p.pid,'mode':mode,'case':case}))
  if code:
   print('FAILED',case,i,mode,(out/'stderr').read_text()[-2500:],flush=True);raise SystemExit(1)
  rec=json.loads((out/'result.json').read_text());assert rec['native_sha256']==pins[mode] and rec['validated'] and len(rec['samples'])==5
  rec['process']=i;records.append(rec)
  print(case,i,mode,'wall',statistics.median(s['wall_seconds'] for s in rec['samples']),'cpu',statistics.median(s['cpu_seconds'] for s in rec['samples']),flush=True)
  (root/'records.json').write_text(json.dumps(records,indent=2))
 print('CASE COMPLETE',case,flush=True)
