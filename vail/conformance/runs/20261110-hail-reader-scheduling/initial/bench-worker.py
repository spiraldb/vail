from pathlib import Path
from types import SimpleNamespace
import ast,hashlib,json,sys,time,psutil
import vail as hl,vail._native as native
r=Path(__file__).parent;mode,case,outpath=sys.argv[1:];out=Path(outpath);cfg=json.loads((r/'bench-config.json').read_text());pins=cfg['inputs'];old=Path('/private/tmp/vail-hail-slow-three-20261110');proto=json.loads((old/'protocol.json').read_text());spec=proto['cases'][case];fixture=pins['fixtures'][spec['fixture']]['root'];source=old/spec['source']
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert sha(native.__file__)==cfg[mode]['native_sha256'];assert sha(source)==pins['source_sha256'][spec['source']]
fn=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef) and n.name==case);fn.decorator_list=[];module=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(module);api=SimpleNamespace(**vars(hl));api.read_matrix_table=hl.MatrixTable.read_legacy;ns={'hl':api};exec(compile(module,str(source),'exec'),ns)
proc=psutil.Process();assert ns[case](fixture) is None;samples=[]
for i in range(3):
 assert not proc.children(recursive=True)
 cpu=time.process_time();start=time.perf_counter();assert ns[case](fixture) is None;wall=time.perf_counter()-start;cpu=time.process_time()-cpu
 assert not proc.children(recursive=True)
 samples.append({'wall_seconds':wall,'cpu_seconds':cpu,'threads_after_query':proc.num_threads()});print('SAMPLE',i,wall,cpu,samples[-1]['threads_after_query'],flush=True)
result={'mode':mode,'case':case,'fixture':fixture,'native_sha256':sha(native.__file__),'source_sha256':sha(source),'samples':samples,'passed':True,'scope':'one full warmup plus3 original force-count calls, fixed full-sized input; process CPU; not cross-engine correctness comparison'}
(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
