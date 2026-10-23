"""Aligned histogram counts share the native boundary and partial-state rules."""
import pytest
import vail as hl


@pytest.mark.parametrize('dtype',[hl.tint32,hl.tint64,hl.tfloat32,hl.tfloat64])
@pytest.mark.parametrize('partitions',[1,3])
def test_coordinate_histogram_boundaries_missing_and_filter(dtype,partitions):
    values=[-2,-1,0,1,2,3,4,5,None]
    table=hl.Table.parallelize([{'xs':[value,None,2], 'keep':index%2==0} for index,value in enumerate(values)],
        schema=hl.tstruct(xs=hl.tarray(dtype),keep=hl.tbool),n_partitions=partitions)
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        all=hl.agg.hist(x,0,4,2), selected=hl.agg.filter(table.keep,hl.agg.hist(x,0,4,2))),table.xs))
    assert result[0].all.bin_edges==[0.,2.,4.]
    assert result[0].all.bin_freq==[2,3]
    assert result[0].all.n_smaller==2 and result[0].all.n_larger==1
    assert result[0].selected.bin_freq==[1,2]
    assert result[0].selected.n_smaller==1 and result[0].selected.n_larger==0
    assert result[1].all.bin_freq==[0,0]
    assert result[2].all.bin_freq==[0,9]


@pytest.mark.parametrize('partitions',[1,3])
def test_coordinate_histogram_nonfinite_and_native_empty_identity(partitions):
    table=hl.Table.parallelize([{'xs':[value]} for value in [float('-inf'),float('inf'),float('nan'),None]],
        schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)),n_partitions=partitions)
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        all=hl.agg.hist(x,0,4,2),empty=hl.agg.filter(False,hl.agg.hist(x,0,4,2))),table.xs))[0]
    assert result.all.bin_freq==[0,0] and result.all.n_smaller==result.all.n_larger==1
    assert result.empty.bin_edges==[0.,2.,4.] and result.empty.bin_freq==[0,0]
    assert result.empty.n_smaller==result.empty.n_larger==0
