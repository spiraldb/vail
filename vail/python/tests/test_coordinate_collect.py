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
