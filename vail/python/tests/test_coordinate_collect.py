"""Collect's coordinate layout preserves native values, filters and identities."""
import pytest
import vail as hl


def test_coordinate_collect_nested_values_and_filtered_missing_elements():
    element=hl.tstruct(s=hl.tstr,xs=hl.tarray(hl.tint32))
    rows=[{'xs':[{'s':'a','xs':[1,None]},None], 'keep':True},
          {'xs':[None,{'s':'b','xs':[]}], 'keep':False},
          {'xs':[{'s':None,'xs':None},None], 'keep':True}]
    table=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(element),keep=hl.tbool),n_partitions=1)
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        all=hl.agg.collect(x),selected=hl.agg.filter(table.keep,hl.agg.collect(x)),
        empty=hl.agg.filter(False,hl.agg.collect(x))),table.xs))
    assert len(result[0].all)==3 and result[0].all[0].s=='a'
    assert result[0].all[0].xs==[1,None] and result[0].all[1] is None
    assert result[0].all[2].s is None and result[0].all[2].xs is None
    assert [x.s if x is not None else None for x in result[0].selected]==['a',None]
    assert result[1].selected==[None,None]
    assert result[0].empty==result[1].empty==[]


@pytest.mark.parametrize('partitions',[1,3])
def test_coordinate_collect_missing_lists_do_not_add_hidden_values(partitions):
    table=hl.Table.parallelize([{'xs':[1,None]},{'xs':None},{'xs':[3,4]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)),n_partitions=partitions)
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.agg.collect(x),table.xs))
    assert sorted(result[0])==[1,3]
    assert sorted(result[1],key=lambda x:(x is not None,x or 0))==[None,4]


@pytest.mark.parametrize('partitions',[1,3])
def test_matrix_coordinate_collect_present_slots_and_missing_values(partitions):
    matrix=hl.utils.range_matrix_table(6,3,n_partitions=partitions)
    matrix=matrix.annotate_entries(x=hl.or_missing((matrix.row_idx+matrix.col_idx)%3!=0,matrix.row_idx+matrix.col_idx))
    matrix=matrix.filter_entries((matrix.row_idx+matrix.col_idx)%4!=0)
    matrix=matrix.annotate_cols(collected=hl.agg.collect(matrix.x))
    for column,values in enumerate(matrix.collected.collect()):
        expected=[None if (row+column)%3==0 else row+column for row in range(6) if (row+column)%4!=0]
        key=lambda value:(value is not None,value or 0)
        assert sorted(values,key=key)==sorted(expected,key=key)
    empty=hl.utils.range_matrix_table(0,3,n_partitions=partitions)
    empty=empty.annotate_cols(collected=hl.agg.collect(empty.row_idx))
    assert empty.collected.collect()==[[],[],[]]


@pytest.mark.parametrize('partitions',[1,3])
@pytest.mark.parametrize('limit',[0,1,3,20])
def test_coordinate_take_bounded_order_nulls_and_filters(partitions,limit):
    rows=[{'xs':[{'s':str(i),'v':[i,None]},None], 'keep':i%2==0} for i in range(9)]
    dtype=hl.tstruct(s=hl.tstr,v=hl.tarray(hl.tint32))
    table=hl.Table.parallelize(rows,schema=hl.tstruct(xs=hl.tarray(dtype),keep=hl.tbool),n_partitions=partitions)
    result=table.aggregate(hl.agg.array_agg(lambda x:hl.struct(
        taken=hl.agg.take(x,limit),filtered=hl.agg.filter(table.keep,hl.agg.take(x,limit)),
        absent=hl.agg.filter(False,hl.agg.take(x,limit))),table.xs))
    assert result[0].taken==[hl.Struct(s=str(i),v=[i,None]) for i in range(min(limit,9))]
    assert result[1].taken==[None]*min(limit,9)
    assert result[0].filtered==[hl.Struct(s=str(i),v=[i,None]) for i in range(0,9,2)][:limit]
    assert result[1].filtered==[None]*min(limit,5)
    assert result[0].absent==result[1].absent==[]
