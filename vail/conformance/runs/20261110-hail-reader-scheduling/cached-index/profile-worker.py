from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,time,psutil
import vail as hl,vail._native as native
r=Path(__file__).parent;cfg=json.loads((r/'bench-config.json').read_text());assert hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()==cfg['candidate']['native_sha256'];source=Path('/private/tmp/vail-hail-slow-three-20261110/benchmark_table.py');assert hashlib.sha256(source.read_bytes()).hexdigest()==cfg['inputs']['source_sha256']['benchmark_table.py'];name='test_group_by_collect_per_row'
fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==name);fn.decorator_list=[];m=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(m);api=SimpleNamespace(**vars(hl));api.read_matrix_table=hl.MatrixTable.read_legacy;ns={'hl':api};exec(compile(m,str(source),'exec'),ns)
fixture=cfg['inputs']['fixtures']['gnomad']['root'];assert ns[name](fixture) is None
start=time.monotonic();warm=time.time();cpu=time.process_time();calls=[];proc=psutil.Process()
while time.monotonic()-start<6:
 begin=time.time();assert ns[name](fixture) is None;calls.append({'start_epoch':begin,'end_epoch':time.time(),'threads_after_query':proc.num_threads()})
result={'native_sha256':cfg['candidate']['native_sha256'],'production':cfg['candidate_source'],'input':fixture,'warm_epoch':warm,'end_epoch':time.time(),'calls':calls,'loop_wall':time.monotonic()-start,'loop_cpu':time.process_time()-cpu,'scope':'One warmup then repeat original groupcollect for6seconds. Diagnostic sampledrun, not comparable timings; queryboundaryepochs retained.'}
(r/'profile-worker-result.json').write_text(json.dumps(result,indent=2)+'\n')
