from collections.abc import Mapping
import json
import vail as h
m=h.utils.range_matrix_table(8,3,n_partitions=2)
m=m.annotate_rows(locus=h.locus('1',m.row_idx+1),__h047_x=h.float64(m.row_idx))
m=m.annotate_entries(DP=h.int32(2+3*m.row_idx+m.col_idx))
m=m.annotate_cols(__h047_fit=h.agg.linreg(h.float64(h.or_else(m.DP,0)),[1.0,m['__h047_x']]))
c=m.cols();c=c.select(fit=c['__h047_fit'])
keys=tuple(c.key.dtype)
assert keys==('col_idx',)
rows=c.collect()
assert sorted(row['col_idx'] for row in rows)==[0,1,2]
fields={'beta','standard_error','t_stat','p_value','multiple_standard_error','multiple_r_squared','adjusted_r_squared','f_stat','multiple_p_value','n'}
for row in rows:
 assert isinstance(row.fit,Mapping)
 assert set(row.fit)==fields and row.fit.n==8
 assert abs(row.fit.beta[0]-(2+row.col_idx))<1e-8 and abs(row.fit.beta[1]-3)<1e-8
 for name in ('beta','standard_error','t_stat','p_value'):
  assert row.fit[name] is None or len(row.fit[name])==2
print(json.dumps({'status':'PASS','rows':len(rows),'keys':keys,'complete_fit_fields':sorted(fields)}))
