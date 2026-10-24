"""Population moments keep original integer/float value semantics."""
import math
import statistics
import pytest
import vail as hl

@pytest.mark.parametrize('dtype',[hl.tint32,hl.tint64,hl.tfloat32,hl.tfloat64])
@pytest.mark.parametrize('partitions',[1,3])
def test_native_stats_filtered_grouped_and_full_result(dtype,partitions,tmp_path):
    values=[1,None,7,-2,9,None,4]
    table=hl.Table.parallelize([hl.Struct(x=x,g=i%3) for i,x in enumerate(values)],schema=hl.tstruct(x=dtype,g=hl.tint32),n_partitions=partitions)
    path=str(tmp_path/'values.ht');table.write(path);table=hl.read_table(path)
    expected=statistics.pstdev([float(x) for x in values if x is not None])
    assert table.aggregate(hl.agg.stats(table.x).stdev)==pytest.approx(expected)
    full=table.aggregate(hl.struct(stats=hl.agg.stats(table.x),total=hl.agg.sum(hl.float64(table.x))))
    assert full.stats.stdev==pytest.approx(expected)
    assert full.stats.n==5 and full.stats.sum==full.total==19
    selected=[float(x) for x in values if x is not None and x>2]
    assert table.aggregate(hl.agg.filter(table.x>2,hl.agg.stats(table.x)).stdev)==pytest.approx(statistics.pstdev(selected))
    groups=table.group_by(table.g).aggregate(sd=hl.agg.stats(table.x).stdev).collect()
    for row in groups:
        group=[float(x) for i,x in enumerate(values) if i%3==row.g and x is not None]
        assert row.sd==pytest.approx(statistics.pstdev(group))

@pytest.mark.parametrize('dtype,values',[(hl.tint32,[-2**31,2**31-1]),(hl.tint64,[-2**63,2**63-1])])
def test_integer_extrema_and_empty_null_scopes(dtype,values):
    table=hl.Table.parallelize([hl.Struct(x=x) for x in values],schema=hl.tstruct(x=dtype),n_partitions=2)
    assert table.aggregate(hl.agg.stats(table.x).stdev)==pytest.approx(statistics.pstdev([float(x) for x in values]))
    empty=table.filter(False)
    assert math.isnan(empty.aggregate(hl.agg.stats(empty.x).stdev))
    missing=table.annotate(x=hl.missing(dtype))
    assert math.isnan(missing.aggregate(hl.agg.stats(missing.x).stdev))

@pytest.mark.parametrize('dtype',[hl.tfloat32,hl.tfloat64])
def test_float_nonfinite_values_still_poison_and_filters_exclude_them(dtype):
    table=hl.Table.parallelize([hl.Struct(x=x) for x in [float('inf'),float('-inf'),float('nan'),2.,6.,None]],schema=hl.tstruct(x=dtype),n_partitions=3)
    assert math.isnan(table.aggregate(hl.agg.stats(table.x).stdev))
    finite=(table.x>float('-inf'))&(table.x<float('inf'))
    assert table.aggregate(hl.agg.filter(finite,hl.agg.stats(table.x)).stdev)==2.


def test_sparse_matrix_global_and_row_moments_keep_entry_scope():
    matrix=hl.utils.range_matrix_table(5,7,3)
    matrix=matrix.annotate_entries(x=hl.or_missing(matrix.col_idx%3!=0,hl.int32(matrix.row_idx+matrix.col_idx)))
    matrix=matrix.filter_entries((matrix.row_idx!=1)&(matrix.col_idx%2==0))
    values=[float(row+col) for row in range(5) for col in range(7) if row!=1 and col%2==0 and col%3!=0]
    assert matrix.aggregate_entries(hl.agg.stats(matrix.x).stdev)==pytest.approx(statistics.pstdev(values))
    rows=matrix.annotate_rows(sd=hl.agg.stats(matrix.x).stdev).rows().collect()
    for row in rows:
        defined=[float(row.row_idx+col) for col in range(7) if row.row_idx!=1 and col%2==0 and col%3!=0]
        if defined: assert row.sd==pytest.approx(statistics.pstdev(defined))
        else: assert math.isnan(row.sd)
