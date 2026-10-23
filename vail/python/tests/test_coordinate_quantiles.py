"""Coordinate sketches reuse native KLL updates and its serialized state."""
import pytest
import vail as hl


@pytest.mark.parametrize('partitions',[1,3])
@pytest.mark.parametrize('dtype',[hl.tint32,hl.tint64,hl.tfloat32,hl.tfloat64])
def test_coordinate_quantile_exact_small_sketches_and_filters(partitions,dtype):
    t=hl.Table.parallelize([{'xs':[i,None,i+10],'keep':i%2==0} for i in range(5)],
        schema=hl.tstruct(xs=hl.tarray(dtype),keep=hl.tbool),n_partitions=partitions)
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        qs=hl.agg.approx_quantiles(x,[0.,.5,1.],k=128),median=hl.agg.approx_median(x,k=128),
        filtered=hl.agg.filter(t.keep,hl.agg.approx_quantiles(x,[0.,.5,1.],k=128)),
        empty=hl.agg.filter(False,hl.agg.approx_quantiles(x,[0.,.5,1.],k=128))),t.xs))
    assert result[0].qs==[0,2,4] and result[0].median==2
    assert result[0].filtered==[0,2,4] and result[0].empty==[None,None,None]
    assert result[1].qs==[None,None,None] and result[1].median is None
    assert result[2].qs==[10,12,14] and result[2].median==12
    bare=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.approx_quantiles(x,[0.,.5,1.],k=128),t.xs))
    assert bare==[x.qs for x in result]


def test_coordinate_quantile_nan_queries_and_scalar_output():
    t=hl.Table.parallelize([{'xs':[float('nan'),1.]},{'xs':[None,2.]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)),n_partitions=1)
    assert t.aggregate(hl.agg.array_agg(lambda x:hl.agg.approx_median(x),t.xs))==[None,2.]
    assert t.aggregate(hl.agg.array_agg(lambda x:hl.agg.approx_quantiles(x,[None,0.,1.]),t.xs))==[[None,None,None],[None,1.,2.]]


def test_coordinate_quantile_preserves_native_int64_exactness_guard():
    t=hl.Table.parallelize([{'xs':[2**53+1]}],schema=hl.tstruct(xs=hl.tarray(hl.tint64)),n_partitions=1)
    with pytest.raises(Exception,match='cannot be represented exactly'):
        t.aggregate(hl.agg.array_agg(lambda x:hl.agg.approx_median(x),t.xs))
    assert t.aggregate(hl.agg.array_agg(lambda x:hl.agg.filter(False,hl.agg.approx_median(x)),t.xs))==[None]
