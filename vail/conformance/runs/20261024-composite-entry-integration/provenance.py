import pathlib,json,subprocess,hashlib,zipfile
r=pathlib.Path.home()/'h089-boolean-integration';s=pathlib.Path.home()/'finite-stddev-20261024/source/vail';h=lambda b:hashlib.sha256(b).hexdigest()
d={}
for mode,env,wheels in [('baseline',pathlib.Path.home()/'h092-boolean-reducers/env',pathlib.Path.home()/'h092-boolean-reducers/wheels'),('candidate',r/'env',r/'wheels')]:
 code='import vail,vail._native as n,json,pathlib,hashlib;print(json.dumps({"python":vail.__file__,"native":n.__file__,"native_sha256":hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))'
 d[mode]=json.loads(subprocess.check_output([str(env/'bin/python'),'-c',code],text=True))
 w=next(wheels.glob('*.whl'))
 with zipfile.ZipFile(w) as z:
  name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'));d[mode]['wheel_native_sha256']=h(z.read(name))
 assert d[mode]['native_sha256']==d[mode]['wheel_native_sha256']
 d[mode]['wheel_sha256']=h(w.read_bytes())
paths=['crates/vail-python/src/native_ir.rs','python/vail/matrix_list_aggregates.py','python/vail/matrixtable.py','python/tests/test_composite_entry_totals.py','crates/vail-execution/src/list_aggregate/direct.rs','Cargo.lock']
d['source']='2c1d47a8a7fd15990a64ddc3f18ff71770a403c7';d['baseline_source']='8c7a0cf181610f43be1379509de9bd2c0436e346';d['source_sha256']={p:h((s/p).read_bytes()) for p in paths}
(r/'results/native-provenance.json').write_text(json.dumps(d,indent=2)+'\n')
