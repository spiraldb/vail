import pathlib,json,subprocess,hashlib,zipfile
r=pathlib.Path.home()/'aggregate-input-projection';s=pathlib.Path.home()/'finite-stddev-20261024/source/vail';h=lambda b:hashlib.sha256(b).hexdigest()
d={}
for mode,env,wheels in [('baseline',pathlib.Path.home()/'h089-boolean-integration/env',pathlib.Path.home()/'h089-boolean-integration/wheels'),('candidate',r/'env',r/'wheels')]:
 code='import vail,vail._native as n,json,pathlib,hashlib;print(json.dumps({"python":vail.__file__,"native":n.__file__,"native_sha256":hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))'
 d[mode]=json.loads(subprocess.check_output([str(env/'bin/python'),'-c',code],text=True))
 w=next(wheels.glob('*.whl'))
 with zipfile.ZipFile(w) as z:
  name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'));d[mode]['wheel_native_sha256']=h(z.read(name))
 assert d[mode]['native_sha256']==d[mode]['wheel_native_sha256']
 d[mode]['wheel_sha256']=h(w.read_bytes())
paths=['crates/vail-table/src/nested_projection.rs','crates/vail-table/src/logical.rs','python/tests/test_aggregate_nested_projection.py','crates/vail-execution/src/list_aggregate/direct.rs','Cargo.lock']
d['source']='06defddc30de799d7b16592afb6c41155c91b552';d['baseline_source']='a47b8f170462c95d81c68f23ade08938acc769e1';d['source_sha256']={p:h((s/p).read_bytes()) for p in paths}
(r/'results/native-provenance.json').write_text(json.dumps(d,indent=2)+'\n')
