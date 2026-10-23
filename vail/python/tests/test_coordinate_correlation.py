"""Coordinate correlation produces the existing native partial-state protocol."""
import math
import pytest
import vail as hl


def same(actual,expected):
    if expected is None:
        assert actual is None
    elif math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual==pytest.approx(expected,rel=1e-14,abs=1e-14)


@pytest.mark.parametrize('partitions',[1,3])
@pytest.mark.parametrize('dtype',[hl.tint32,hl.tfloat64])
def test_coordinate_corr_matches_native_scalar_for_nulls_filters_and_constants(partitions,dtype):
    rows=[]
    for i in range(9):
        xs=[{'x':i,'y':2*i+1},{'x':i,'y':-3*i}, {'x':1,'y':i},
            {'x':i if i==0 else None,'y':i}, {'x':None,'y':None},
            None if i%3==0 else {'x':i,'y':i*i}]
        rows.append({'xs':xs,'keep':i%2==0})
    item=hl.tstruct(x=dtype,y=dtype)
    t=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(item),keep=hl.tbool),n_partitions=partitions)
    actual=t.aggregate(hl.agg.array_agg(lambda pair:hl.struct(
        all=hl.agg.corr(pair.x,pair.y),selected=hl.agg.filter(t.keep,hl.agg.corr(pair.x,pair.y)),
        empty=hl.agg.filter(False,hl.agg.corr(pair.x,pair.y))),t.xs))
    bare=t.aggregate(hl.agg.array_agg(lambda pair:hl.agg.corr(pair.x,pair.y),t.xs))
    for column,result in enumerate(actual):
        reference=t.aggregate(hl.agg.corr(t.xs[column].x,t.xs[column].y))
        filtered=t.aggregate(hl.agg.filter(t.keep,hl.agg.corr(t.xs[column].x,t.xs[column].y)))
        same(result.all,reference)
        same(bare[column],reference)
        same(result.selected,filtered)
        assert result.empty is None


def test_coordinate_corr_nonfinite_matches_native_scalar():
    rows=[{'xs':[{'x':x,'y':y}]} for x,y in [(float('nan'),1.),(1.,2.),(2.,float('inf'))]]
    t=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(hl.tstruct(x=hl.tfloat64,y=hl.tfloat64))),n_partitions=1)
    result=t.aggregate(hl.agg.array_agg(lambda p:hl.agg.corr(p.x,p.y),t.xs))
    same(result[0],t.aggregate(hl.agg.corr(t.xs[0].x,t.xs[0].y)))
