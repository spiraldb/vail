import argparse, ast, hashlib, json, pathlib, resource, time
import vail as hl
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('case');p.add_argument('--repeat',type=int,default=3);a=p.parse_args()
base=pathlib.Path('/home/danielking/cloud-scan-20261023')
source=base/'upstream/hail/python/benchmark/hail/benchmark_matrix_table.py'
tree=ast.parse(source.read_text()); names={'many_aggs','test_matrix_table_many_aggs_row_wise','test_matrix_table_many_aggs_col_wise','test_gnomad_coverage_stats_optimized','test_matrix_table_array_arithmetic'}
ns={'hl':hl};exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),str(source),'exec'),ns)
paths=base/'data/vail-from-hail'
profile=str(paths/'profile25_mt.mt');coverage=str(paths/'gnomad_dp_sim.mt')
def query():
 if a.case=='rows': return ns['test_matrix_table_many_aggs_row_wise'](profile)
 if a.case=='cols': return ns['test_matrix_table_many_aggs_col_wise'](profile)
 if a.case=='coverage': return ns['test_gnomad_coverage_stats_optimized'](coverage)
 if a.case=='dosage': return ns['test_matrix_table_array_arithmetic'](profile)
 m=hl.read_matrix_table(profile)
 if a.case=='entry_clamp_sum': return m.aggregate_entries(hl.agg.sum(hl.min(40, hl.max(0, m.DP))))
 if a.case=='row_clamp_sum':
  r=m.annotate_rows(value=hl.agg.sum(hl.min(40, hl.max(0, m.DP)))).rows()
  return r.aggregate(hl.agg.sum(r.value))
 if a.case in ('entry_sum_entryonly','entry_sum_rowonly','entry_sum_colonly'):
  if a.case=='entry_sum_entryonly': m=m.select_entries('DP')
  elif a.case=='entry_sum_rowonly': m=m.select_rows()
  else: m=m.select_cols()
  t=m.entries();return t.aggregate(hl.agg.sum(t.DP))
 if a.case=='direct_sum': return m.aggregate_entries(hl.agg.sum(m.DP))
 if a.case=='entry_sum_narrow':
  t=m.select_entries('DP').select_rows().select_cols().entries();return t.aggregate(hl.agg.sum(t.DP))
 if a.case=='entry_sum_unkeyed':
  t=m.key_rows_by().key_cols_by().entries();return t.aggregate(hl.agg.sum(t.DP))
 if a.case=='entry_sum':
  t=m.entries();return t.aggregate(hl.agg.sum(t.DP))
 if a.case=='stats': return m.aggregate_entries(hl.agg.stats(m.GQ))
 raise ValueError(a.case)
print(json.dumps({'case':a.case,'native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest()}),flush=True)
for i in range(a.repeat):
 b=resource.getrusage(resource.RUSAGE_SELF);t=time.perf_counter();value=query();after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'wall':time.perf_counter()-t,'cpu':after.ru_utime+after.ru_stime-b.ru_utime-b.ru_stime,'maxrss_kib':after.ru_maxrss,'result':str(value)}),flush=True)
