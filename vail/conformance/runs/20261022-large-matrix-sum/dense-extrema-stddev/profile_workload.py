import time
import vail as hl
for repetition in range(8):
    matrix=hl.utils.range_matrix_table(10000,1000,n_partitions=250)
    matrix=matrix.annotate_entries(x=hl.or_missing(matrix.row_idx%5!=0,matrix.row_idx+matrix.col_idx))
    matrix=matrix.annotate_cols(result=hl.struct(lo=hl.agg.min(matrix.x),hi=hl.agg.max(matrix.x),sd=hl.agg.stats(matrix.x).stdev,n=hl.agg.count_where(matrix.x>5000)))
    values=matrix.result.collect()
    assert len(values)==1000 and values[0].lo==1 and values[0].hi==9999
    print(repetition,values[0],flush=True)
