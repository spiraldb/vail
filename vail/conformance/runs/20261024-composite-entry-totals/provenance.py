import pathlib,json,subprocess,hashlib,zipfile
r=pathlib.Path.home()/'h089-composite-totals';s=pathlib.Path.home()/'finite-stddev-20261024/source/vail';h=lambda b:hashlib.sha256(b).hexdigest()
d={}
for mode,env,wheels in [('baseline',pathlib.Path.home()/'h092-segmented-reducers/env',pathlib.Path.home()/'h092-segmented-reducers/wheels'),('candidate',r/'env',r/'wheels')]:
 code='import vail,vail._native as n,json,pathlib,hashlib;print(json.dumps({"python":vail.__file__,"native":n.__file__,"native_sha256":hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))'
 d[mode]=json.loads(subprocess.check_output([str(env/'bin/python'),'-c',code],text=True))
 w=next(wheels.glob('*.whl'))
 with zipfile.ZipFile(w) as z:
  name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'));d[mode]['wheel_native_sha256']=h(z.read(name))
 assert d[mode]['native_sha256']==d[mode]['wheel_native_sha256']
 d[mode]['wheel_sha256']=h(w.read_bytes())
paths=['crates/vail-python/src/native_ir.rs','python/vail/matrix_list_aggregates.py','python/vail/matrixtable.py','python/tests/test_composite_entry_totals.py','crates/vail-execution/src/list_aggregate/direct.rs','Cargo.lock']
d['source']='24d4f6218020f74a261944e4030cc57b25703a83';d['baseline_source']='de71b2898e419aa59b42f4aa64775c82c6a6fe5c';d['source_sha256']={p:h((s/p).read_bytes()) for p in paths}
(r/'results/native-provenance.json').write_text(json.dumps(d,indent=2)+'\n')
