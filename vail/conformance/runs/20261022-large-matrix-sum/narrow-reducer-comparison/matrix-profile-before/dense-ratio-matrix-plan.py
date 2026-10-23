import vail as hl
m=hl.utils.range_matrix_table(10000,1000,n_partitions=250)
m=m.annotate_entries(x=hl.or_missing(m.row_idx%5!=0,m.row_idx+m.col_idx))
m=m.annotate_cols(result=hl.struct(total=hl.agg.sum(m.x),n=hl.agg.count(),mean=hl.agg.mean(m.x),fraction=hl.agg.fraction(m.x>5000)))
print(m.cols()._native.physical_plan())
