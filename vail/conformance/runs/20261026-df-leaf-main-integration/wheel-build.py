import subprocess,time,os,signal,shutil,json,pathlib
out=pathlib.Path('/private/tmp/vail-df-leaf-main-integration/wheel');out.mkdir(exist_ok=True)
root=pathlib.Path('/Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail')
cmd=['/private/tmp/vail-encoded-projection-env/bin/maturin','build','--release','--locked','--interpreter','/private/tmp/vail-encoded-main-integration-env/bin/python','--out','/private/tmp/vail-df-leaf-main-integration/wheels']
env=dict(os.environ,RUSTFLAGS='-C target-cpu=native',RUSTC_WRAPPER='kache',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='2',CARGO_TARGET_DIR='/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target',MACOSX_DEPLOYMENT_TARGET='11.0',PYO3_PYTHON='/private/tmp/vail-encoded-main-integration-env/bin/python')
rev=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
assert rev=='f5f37d6e09d828d4438aa3afb0512c4e6335df66', rev
start=time.time(); initial=shutil.disk_usage(root).free; minimum=initial;reason=None
if initial<1.1*(1024**3):raise SystemExit("Below approved disk floor before build")
with (out/'build.log').open('wb') as log:
 p=subprocess.Popen(cmd,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 with (out/'build-disk.jsonl').open('w') as samples:
  while p.poll() is None:
   elapsed=time.time()-start;free=shutil.disk_usage(root).free;minimum=min(minimum,free)
   samples.write(json.dumps({'elapsed':elapsed,'free_bytes':free})+'\n');samples.flush()
   if elapsed>600:reason='600 second build budget'
   if free<1.1*(1024**3):reason='1.1 GiB free-space floor'
   if reason:
    os.killpg(p.pid,signal.SIGTERM)
    try:p.wait(timeout=10)
    except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
    break
   time.sleep(2)
 result={'source':rev,'command':cmd,'env':{k:env[k] for k in ['RUSTFLAGS','RUSTC_WRAPPER','CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_TARGET_DIR','MACOSX_DEPLOYMENT_TARGET','PYO3_PYTHON']},'pid':p.pid,'elapsed':time.time()-start,'returncode':p.returncode,'stop_reason':reason,'initial_free_bytes':initial,'minimum_free_bytes':minimum,'final_free_bytes':shutil.disk_usage(root).free}
(out/'build-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
