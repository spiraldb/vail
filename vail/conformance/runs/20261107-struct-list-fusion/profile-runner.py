import os,sys,subprocess,pathlib,json,signal,hashlib,time
root=pathlib.Path('/private/tmp/vail-struct-builder-20261106/profile-final');root.mkdir(exist_ok=True)
exe='/private/tmp/vail-split-sort-target/release/examples/struct_fusion'
env={k:v for k,v in os.environ.items() if not k.startswith('VAIL_')};env.update(OMP_NUM_THREADS='2',RAYON_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
receipt={'executable_sha256':hashlib.sha256(pathlib.Path(exe).read_bytes()).hexdigest(),'source':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'case':'maps-flatmap','evaluations':2001,'sample_duration_seconds':1,'sample_interval_ms':1}
children=[]
try:
    with (root/'worker.stdout').open('w') as out,(root/'worker.stderr').open('w') as err:
        p=subprocess.Popen([exe,'maps-flatmap','--profile'],env=env,stdout=out,stderr=err,start_new_session=True);children.append(p)
        sampler=subprocess.Popen(['/usr/bin/sample',str(p.pid),'1','1','-file',str(root/'sample.txt')],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True);children.append(sampler)
        stdout,stderr=sampler.communicate(timeout=15);receipt['sample_returncode']=sampler.returncode
        (root/'sampler.stdout').write_text(stdout);(root/'sampler.stderr').write_text(stderr)
        receipt['worker_returncode']=p.wait(timeout=20)
    receipt['validated']=json.loads((root/'worker.stdout').read_text()).get('validated') is True
finally:
    receipt['cleanup']=[]
    for p in children:
        if p.poll() is None:
            os.killpg(p.pid,signal.SIGTERM)
            try:p.wait(timeout=2)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=2)
        receipt['cleanup'].append({'pid':p.pid,'returncode':p.returncode,'reaped':p.poll() is not None})
    (root/'receipt.json').write_text(json.dumps(receipt,indent=2))
print(json.dumps(receipt))
assert receipt['worker_returncode']==0 and receipt['validated']
assert receipt['sample_returncode']==0
