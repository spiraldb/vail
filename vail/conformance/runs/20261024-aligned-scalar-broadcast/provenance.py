import pathlib,json,subprocess,hashlib,zipfile
r=pathlib.Path.home()/'aligned-scalar-broadcast';s=pathlib.Path.home()/'finite-stddev-20261024/source/vail';h=lambda b:hashlib.sha256(b).hexdigest()
d={}
for mode,env,wheels in [('baseline',pathlib.Path.home()/'aggregate-input-projection/env',pathlib.Path.home()/'aggregate-input-projection/wheels'),('candidate',r/'env',r/'wheels')]:
 code='import vail,vail._native as n,json,pathlib,hashlib;print(json.dumps({"python":vail.__file__,"native":n.__file__,"native_sha256":hashlib.sha256(pathlib.Path(n.__file__).read_bytes()).hexdigest()}))'
 d[mode]=json.loads(subprocess.check_output([str(env/'bin/python'),'-c',code],text=True))
 w=next(wheels.glob('*.whl'))
 with zipfile.ZipFile(w) as z:
  name=next(x for x in z.namelist() if x.endswith('_native.abi3.so'));d[mode]['wheel_native_sha256']=h(z.read(name))
 assert d[mode]['native_sha256']==d[mode]['wheel_native_sha256']
 d[mode]['wheel_sha256']=h(w.read_bytes())
paths=['crates/vail-execution/src/aligned_matrix_map.rs','crates/vail-execution/src/numeric_lists.rs','crates/vail-execution/src/expressions.rs','python/tests/test_aligned_scalar_broadcast.py','crates/vail-table/src/nested_projection.rs','crates/vail-table/src/logical.rs','crates/vail-execution/src/list_aggregate/direct.rs','Cargo.lock']
d['source']='3f95200b24c3b0efa0122377aa848110e33fe991';d['baseline_source']='1af71dc161f47e3bc30751ac671d9e2de80ff7b8';d['source_sha256']={p:h((s/p).read_bytes()) for p in paths}
(r/'results/native-provenance.json').write_text(json.dumps(d,indent=2)+'\n')
