from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,time
import vail as hl,vail._native as native
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());assert hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()==cfg['candidate']['native_sha256'];source=Path('/private/tmp/vail-hail-slow-three-20261110/benchmark_table.py');name='test_group_by_collect_per_row';fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==name);fn.decorator_list=[];m=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(m);api=SimpleNamespace(**vars(hl));api.read_matrix_table=hl.MatrixTable.read_legacy;ns={'hl':api};exec(compile(m,str(source),'exec'),ns);fixture=cfg['pins']['fixtures']['gnomad']['root'];assert ns[name](fixture) is None
start=time.monotonic();warm=time.time();count=0;cpu=time.process_time()
while time.monotonic()-start<6:
 assert ns[name](fixture) is None;count+=1
(r/'profile-worker-result.json').write_text(json.dumps({'native_sha256':cfg['candidate']['native_sha256'],'warm_epoch':warm,'end_epoch':time.time(),'repeated_calls':count,'loop_wall':time.monotonic()-start,'loop_cpu':time.process_time()-cpu},indent=2)+'\n')
