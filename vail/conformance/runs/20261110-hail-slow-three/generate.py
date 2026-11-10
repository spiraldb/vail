from pathlib import Path
import ast,json
import hail as hl
r=Path(__file__).parent;p=json.loads((r/'pins-initial.json').read_text());(r/'inputs').mkdir()
assert not Path(p['gnomad_fixture']).exists()
hl.init(master='local[4]',global_seed=p['gnomad_seed'],quiet=True,log=str(r/'generation-run/hail.log'),tmp_dir=str(r/'hail-tmp'),spark_conf={'spark.driver.memory':'2g','spark.executor.memory':'2g','spark.ui.enabled':'false','spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1'})
try:
 fn=next(n for n in ast.parse((r/'fixtures.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='gnomad_dp_sim');fn.decorator_list=[];m=ast.Module(body=[fn],type_ignores=[]);ast.fix_missing_locations(m);ns={'hl':hl};exec(compile(m,str(r/'fixtures.py'),'exec'),ns)
 path=ns['gnomad_dp_sim'](r/'inputs');mt=hl.read_matrix_table(str(path));dims=mt.count();assert dims==(250000,1000);assert mt.n_partitions()==32
 (r/'generation.json').write_text(json.dumps({'dimensions':dims,'partitions':mt.n_partitions(),'seed':p['gnomad_seed'],'fixture_body':ast.unparse(fn),'note':'Exact original upstream generation shape and distribution with explicit seed; fresh random input, not historical cloud-byte identity.'},indent=2)+'\n');print('GENERATED',dims,flush=True)
finally:hl.stop()
