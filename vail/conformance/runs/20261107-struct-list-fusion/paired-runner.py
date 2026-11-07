import os,subprocess,json,hashlib,time,statistics,pathlib,sys,platform,datetime
exe=pathlib.Path('/private/tmp/vail-split-sort-target/release/examples/struct_fusion')
root=pathlib.Path(sys.argv[1]);root.mkdir(exist_ok=True)
(root/'executable.sha256').write_text(hashlib.sha256(exe.read_bytes()).hexdigest()+'\n')
source=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert not subprocess.check_output(['git','status','--porcelain'],text=True)
removed=sorted(k for k in os.environ if k.startswith('VAIL_'))
(root/'cohort.json').write_text(json.dumps({'source':source,'host':platform.platform(),'machine':platform.machine(),'logical_cpus':os.cpu_count(),'workload':{'rows':64,'width':1024,'warmups':3,'samples_per_process':7},'order':['off','on','on','off','off','on','on','off'],'removed_inherited_VAIL_variables':removed,'thread_env':{'OMP_NUM_THREADS':'2','RAYON_NUM_THREADS':'2','OPENBLAS_NUM_THREADS':'2'},'measure':'evaluation wall only; output validation/preparation untimed','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2))
allrows=[]
for case in ['maps','filters','maps-flatmap','filters-flatmap','flatmaps','fold','maps-flatmap-null','filters-flatmap-null','flatmaps-null','fold-null','simple']:
    for i,disabled in enumerate([True,False,False,True,True,False,False,True]):
        env={k:v for k,v in os.environ.items() if not k.startswith('VAIL_')}
        env.update(OMP_NUM_THREADS='2',RAYON_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
        env.pop('VAIL_DISABLE_LIST_FLATMAP',None)
        if disabled: env['VAIL_DISABLE_LIST_FLATMAP']='1'
        result=subprocess.run([str(exe),case],env=env,capture_output=True,text=True,timeout=60)
        (root/f'{case}-{i}.stdout').write_text(result.stdout)
        (root/f'{case}-{i}.stderr').write_text(result.stderr)
        if result.returncode: print(case,i,'FAILED',result.stderr[-1500:],flush=True);sys.exit(1)
        row=json.loads(result.stdout);row['process']=i;allrows.append(row)
    off=[statistics.median(r['samples']) for r in allrows if r['case']==case and r['disabled']]
    on=[statistics.median(r['samples']) for r in allrows if r['case']==case and not r['disabled']]
    print(case, 'off_ms',statistics.median(off)*1000,'on_ms',statistics.median(on)*1000,'speedup',statistics.median(off)/statistics.median(on),flush=True)
(root/'results.json').write_text(json.dumps(allrows,indent=2))
assert (root/'executable.sha256').read_text().strip()==hashlib.sha256(exe.read_bytes()).hexdigest()

assert source==subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert not subprocess.check_output(['git','status','--porcelain'],text=True)
