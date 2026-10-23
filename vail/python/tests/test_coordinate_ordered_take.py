"""Native prepared selection preserves ordered TAKE's values and state bounds."""
import pytest
import vail as hl


@pytest.mark.parametrize('partitions',[1,3])
@pytest.mark.parametrize('limit',[0,1,2,20,2**31-1])
def test_coordinate_ordered_take_nested_values_missing_keys_and_filters(partitions,limit):
    rows=[]
    keys=[[None,2],[3,None],[1,4],[2,1]]
    for i,key in enumerate(keys):
        rows.append({'xs':[{'value':None if (i,j)==(2,0) else {'s':str(i),'n':j},'key':k} for j,k in enumerate(key)],'keep':i%2==0})
    value=hl.tstruct(s=hl.tstr,n=hl.tint32)
    item=hl.tstruct(value=value,key=hl.tint32)
    t=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(item),keep=hl.tbool),n_partitions=partitions)
    result=t.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        taken=hl.agg.take(x.value,limit,ordering=x.key),
        filtered=hl.agg.filter(t.keep,hl.agg.take(x.value,limit,ordering=x.key)),
        empty=hl.agg.filter(False,hl.agg.take(x.value,limit,ordering=x.key))),t.xs))
    for column,actual in enumerate(result):
        assert actual.taken==t.aggregate(hl.agg.take(t.xs[column].value,limit,ordering=t.xs[column].key))
        assert actual.filtered==t.aggregate(hl.agg.filter(t.keep,hl.agg.take(t.xs[column].value,limit,ordering=t.xs[column].key)))
        assert actual.empty==[]


def test_coordinate_ordered_take_native_float_ordering_and_struct_keys():
    rows=[{'xs':[{'value':i,'key':k}]} for i,k in enumerate([float('nan'),float('-inf'),0.,float('inf'),None])]
    t=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(hl.tstruct(value=hl.tint32,key=hl.tfloat64))),n_partitions=1)
    actual=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x.value,4,ordering=x.key),t.xs))
    assert actual[0]==t.aggregate(hl.agg.take(t.xs[0].value,4,ordering=t.xs[0].key))
    actual=t.aggregate(hl.agg.array_agg(lambda x:hl.agg.take(x.value,3,ordering=hl.struct(first=x.key,second=-x.value)),t.xs))
    assert actual[0]==t.aggregate(hl.agg.take(t.xs[0].value,3,ordering=hl.struct(first=t.xs[0].key,second=-t.xs[0].value)))
