import vail as hl
m=hl.utils.range_matrix_table(3,5,2)
m=m.annotate_entries(x=hl.or_missing(m.col_idx % 3 != 0,m.row_idx+m.col_idx))
r=m.annotate_rows(**{f'f{i}':hl.agg.fraction(m.x > i) for i in [1,3,5,7]}).rows()
print(r._native.physical_plan())
print(r.collect())
