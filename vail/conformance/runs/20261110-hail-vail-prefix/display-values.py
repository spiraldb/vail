from pathlib import Path
import ast,hashlib,json,math,sys
r=Path(__file__).parent;mode,outpath=sys.argv[1:];out=Path(outpath);p=json.loads((r/'pins.json').read_text())
if mode=='vail':
 import vail as hl,vail._native as native
 assert hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()==p['native_sha256']
else:
 import hail as hl
 assert hl.__version__==p['hail_version']
 hl.init(master='local[4]',quiet=True,log=str(out/'hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
try:
 fn=next(n for n in ast.parse((r/'worker.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='canonical');ns={'hl':hl,'math':math,'json':json};exec(compile(ast.Module(body=[fn],type_ignores=[]),str(r/'worker.py'),'exec'),ns)
 rows=hl.read_matrix_table(p[mode+'_fixture']['root']).head(100,10).entries().take(1000);assert len(rows)==1000
 (out/'values.json').write_text(json.dumps(ns['canonical'](rows),sort_keys=True,indent=2)+'\n')
 print('DISPLAYED MATRIX VALUES',mode,len(rows),flush=True)
finally:
 if mode=='hail':hl.stop()
