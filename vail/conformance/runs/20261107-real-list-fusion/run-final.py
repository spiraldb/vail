import os,pathlib,subprocess,json,hashlib,time,signal,statistics,psutil
root=pathlib.Path('/private/tmp/vail-real-fusion-bench-20261107')
py={'before':root/'baseline-env/bin/python','after':pathlib.Path('/private/tmp/vail-flatmap-test-env-20261106/bin/python')}
pins=json.loads((root/'pins.json').read_text())
cases=['test_matrix_table_filter_entries','test_matrix_table_filter_entries_unfilter','test_matrix_table_array_arithmetic','test_split_multi_hts']
records=[]
def interrupted(signum,frame):raise KeyboardInterrupt(f'signal {signum}')
for sig in [signal.SIGINT,signal.SIGTERM,signal.SIGHUP]:signal.signal(sig,interrupted)
def absent(pgid):
 try:os.killpg(pgid,0);return False
 except ProcessLookupError:return True
def cleanup(p,started):
 if p.poll() is None:
  process=psutil.Process(p.pid)
  assert process.create_time()==started and os.getpgid(p.pid)==p.pid and process.uids().effective==os.geteuid()
  os.killpg(p.pid,signal.SIGTERM)
  try:p.wait(timeout=3)
  except subprocess.TimeoutExpired:
   assert psutil.Process(p.pid).create_time()==started and os.getpgid(p.pid)==p.pid
   os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=3)
 end=time.monotonic()+3
 while not absent(p.pid) and time.monotonic()<end:time.sleep(.05)
 assert absent(p.pid),'Owned process group remains; stop rather than advance'
 return {'pid':p.pid,'returncode':p.returncode,'reaped':p.poll() is not None,'group_absent':True}
for case in cases:
 for i,mode in enumerate(['before','after','after','before','before','after','after','before']):
  out=root/'final'/f'{case}-{i}-{mode}';out.mkdir(parents=True)
  env={k:v for k,v in os.environ.items() if not k.startswith('VAIL_') and k!='PYTHONPATH'}
  env.update(RUN_MODE=mode,RAYON_NUM_THREADS='4',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',TOKIO_WORKER_THREADS='4')
  p=None;started=None;code=None;error=None
  try:
   with (out/'stdout').open('w') as stdout,(out/'stderr').open('w') as stderr:
    p=subprocess.Popen([str(py[mode]),str(root/'worker-final.py'),str(out),case],env=env,cwd='/private/tmp',stdout=stdout,stderr=stderr,start_new_session=True)
    started=psutil.Process(p.pid).create_time()
    try:code=p.wait(timeout=120)
    except subprocess.TimeoutExpired:code=124
  except BaseException as e:
   error=repr(e);raise
  finally:
   clean=cleanup(p,started) if p is not None else {'no_worker_started':True}
   (out/'exit.json').write_text(json.dumps({'code':code,'error':error,'cleanup':clean,'mode':mode,'case':case}))
  if code:
   print('FAILED',case,i,mode,(out/'stderr').read_text()[-2500:],flush=True);raise SystemExit(1)
  rec=json.loads((out/'result.json').read_text());assert rec['native_sha256']==pins['native'][mode] and rec['validated'] and rec['identity_verified_before_queries'] and len(rec['samples'])==5
  rec['process']=i;rec['cleanup']=clean;records.append(rec)
  print(case,i,mode,'wall',statistics.median(s['wall_seconds'] for s in rec['samples']),'cpu',statistics.median(s['cpu_seconds'] for s in rec['samples']),flush=True)
  (root/'final-records.json').write_text(json.dumps(records,indent=2))
 print('CASE COMPLETE',case,flush=True)
