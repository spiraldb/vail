import vail as hl
import pytest
from vail.types import tint32, tstruct


def _saved_table(path, rows, key, schema=None):
    table = hl.Table.parallelize(rows, schema=schema, key=key)
    table.write(str(path), overwrite=True)
    return str(path)


def test_query_table_scalar_point_prefix_and_range(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0, "sub": 0, "value": "a"},
        {"k": 1, "sub": 1, "value": "b"},
        {"k": 2, "sub": 0, "value": "c"},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(path, 1)) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.struct(k=1))) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.struct(other=1))) == [hl.Struct(k=1, sub=1, value="b")]
    assert hl.eval(hl.query_table(path, hl.interval(hl.struct(point=0), hl.struct(point=2)))) == [
        hl.Struct(k=0, sub=0, value="a"), hl.Struct(k=1, sub=1, value="b")
    ]
    assert hl.eval(hl.query_table(path, 9)) == []
    assert hl.eval(hl.query_table(path, hl.missing(tint32))) == []
    compound = _saved_table(tmp_path / "compound.vail", [
        {"k": 1, "sub": 2, "value": "compound"},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(compound, hl.struct(second=1, first=2))) == [
        hl.Struct(k=1, sub=2, value="compound")
    ]


def test_query_table_duplicate_matches_across_partitions(tmp_path):
    path = hl.Table.parallelize([
        {"k": 1, "value": "first"}, {"k": 1, "value": "second"},
        {"k": 2, "value": "third"},
    ], key="k", n_partitions=2)
    path.write(str(tmp_path / "dups.vail"), overwrite=True)
    matches = hl.eval(hl.query_table(str(tmp_path / "dups.vail"), 1))
    assert matches == [hl.Struct(k=1, value="first"), hl.Struct(k=1, value="second")]


def test_query_table_row_varying_and_global_queries(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0, "value": "a"}, {"k": 2, "value": "b"},
    ], "k")
    outer = hl.Table.parallelize([{"q": 0}, {"q": 1}, {"q": 2}], key="q")
    result = outer.annotate(matches=hl.query_table(path, outer.q)).select("matches").collect()
    assert [row.matches for row in result] == [
        [hl.Struct(k=0, value="a")], [], [hl.Struct(k=2, value="b")]
    ]
    global_query = outer.annotate_globals(g=2)
    assert global_query.annotate(matches=hl.query_table(path, global_query.g)).collect()[0].matches == [
        hl.Struct(k=2, value="b")
    ]


def test_query_table_row_varying_range_and_missing(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": 0}, {"k": 1}, {"k": 2}, {"k": 3},
    ], "k")
    outer = hl.Table.parallelize([
        {"start": 0, "end": 2}, {"start": 2, "end": 4},
    ], schema=tstruct(start=tint32, end=tint32))
    query = hl.interval(outer.start, outer.end, includes_end=True)
    got = outer.annotate(matches=hl.query_table(path, query)).select("matches").collect()
    assert [row.matches for row in got] == [
        [hl.Struct(k=0), hl.Struct(k=1), hl.Struct(k=2)],
        [hl.Struct(k=2), hl.Struct(k=3)],
    ]


def test_query_table_missing_range_endpoints(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [
        {"k": None}, {"k": 0}, {"k": 1},
    ], "k")
    assert hl.eval(hl.query_table(path, hl.interval(None, 1, includes_end=True))) == []
    assert hl.eval(hl.query_table(path, hl.interval(1, None, includes_end=True))) == [
        hl.Struct(k=1), hl.Struct(k=None)
    ]
    outer = hl.Table.parallelize([
        {"start": None, "end": 1}, {"start": 1, "end": None},
    ], schema=tstruct(start=tint32, end=tint32))
    query = hl.interval(outer.start, outer.end, includes_end=True)
    results = outer.annotate(matches=hl.query_table(path, query)).select("matches").collect()
    assert [row.matches for row in results] == [[], [hl.Struct(k=1), hl.Struct(k=None)]]


def test_query_table_missing_row_query_and_validation(tmp_path):
    path = _saved_table(tmp_path / "query.vail", [{"k": None}, {"k": 0}, {"k": 1}], "k")
    outer = hl.Table.parallelize([{"q": None}, {"q": 1}], schema=tstruct(q=tint32))
    result = outer.annotate(matches=hl.query_table(path, outer.q)).select("matches").collect()
    assert [row.matches for row in result] == [[hl.Struct(k=None)], [hl.Struct(k=1)]]
    assert hl.eval(hl.query_table(path, hl.missing(tint32))) == [hl.Struct(k=None)]
    assert hl.eval(hl.query_table(path, hl.missing(tstruct(k=tint32)))) == [hl.Struct(k=None)]
    compound = _saved_table(tmp_path / "compound.vail", [
        {"k": None, "sub": 0}, {"k": None, "sub": 1}, {"k": 1, "sub": 0},
    ], ["k", "sub"])
    assert hl.eval(hl.query_table(compound, hl.struct(first=hl.missing(tint32)))) == [
        hl.Struct(k=None, sub=0), hl.Struct(k=None, sub=1)
    ]
    with pytest.raises(ValueError, match="empty struct"):
        hl.query_table(path, hl.struct())
    with pytest.raises(ValueError, match="has 2 field"):
        hl.query_table(path, hl.struct(a=1, b=2))
    with pytest.raises(ValueError, match="key mismatch"):
        hl.query_table(path, "wrong type")
    unkeyed = hl.Table.parallelize([{"k": 0}])
    unkeyed_path = tmp_path / "unkeyed.vail"
    unkeyed.write(str(unkeyed_path), overwrite=True)
    with pytest.raises(ValueError, match="unkeyed"):
        hl.query_table(str(unkeyed_path), 0)


def test_query_table_interval_key_range_uses_key_order(tmp_path):
    path = _saved_table(tmp_path / "interval.vail", [
        {"k": 0, "value": "a"}, {"k": 1, "value": "b"},
    ], "k")
    # The outer interval ranges over interval-valued keys, so it is a range
    # lookup. The exact interval key itself remains a point lookup.
    interval_path = _saved_table(tmp_path / "interval_keys.vail", [
        {"key": hl.Interval(0, 2), "value": "a"},
        {"key": hl.Interval(4, 6), "value": "b"},
    ], "key")
    expected = [hl.Struct(key=hl.Interval(0, 2), value="a")]
    assert hl.eval(hl.query_table(interval_path, hl.interval(hl.Interval(0, 1), hl.Interval(4, 5)))) == expected
    assert hl.eval(hl.query_table(interval_path, hl.Interval(0, 2))) == [
        hl.Struct(key=hl.Interval(0, 2), value="a")
    ]
    outer = hl.Table.parallelize([
        {"start": hl.Interval(0, 1), "end": hl.Interval(4, 5)},
    ])
    result = outer.annotate(matches=hl.query_table(interval_path, hl.interval(outer.start, outer.end)))
    assert result.collect()[0].matches == [hl.Struct(key=hl.Interval(0, 2), value="a")]


def test_query_table_source_remains_lazy_until_action(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(20, n_partitions=4).write(str(path))
    assert hl.eval(hl.query_table(str(path), 1)) == [hl.Struct(idx=1)]
    lazy_query = hl.query_table(str(path), 2)
    moved = tmp_path / 'moved_source'
    path.rename(moved)
    with pytest.raises((FileNotFoundError, RuntimeError, ValueError)):
        hl.eval(lazy_query)


def test_query_table_computed_scalar_and_global_inputs_are_lazy(tmp_path):
    path = _saved_table(tmp_path / 'right', [{'k':0},{'k':1},{'k':2}], 'k')
    source = hl.range_table(1).annotate_globals(g=hl.int32('bad'))
    query = hl.query_table(path, source.g)
    with pytest.raises(RuntimeError, match='bad|parse|cast'):
        hl.eval(query)
    query = hl.query_table(path, hl.if_else(False, hl.int32('bad'), 2))
    assert hl.eval(query) == [hl.Struct(k=2)]
    # Selecting another result must not execute a captured failing query.
    unused = hl.query_table(path, source.g)
    assert hl.eval(hl.if_else(False, unused, hl.empty_array(hl.tstruct(k=hl.tint32)))) == []


def test_query_table_lazy_scalar_source_is_not_read_while_building(tmp_path):
    path = _saved_table(tmp_path / 'right', [{'k':0},{'k':1}], 'k')
    source_path = tmp_path / 'arguments'
    hl.range_table(1).write(str(source_path))
    source = hl.read_table(str(source_path))
    argument = source.idx.collect(_localize=False)[0]
    moved = tmp_path / 'arguments_moved'
    source_path.rename(moved)
    query = hl.query_table(path, argument)
    with pytest.raises((FileNotFoundError, RuntimeError, ValueError)):
        hl.eval(query)
    moved.rename(source_path)
    assert hl.eval(query) == [hl.Struct(k=0)]


@pytest.mark.parametrize('include_start,include_end', [(True,False),(True,True),(False,True),(False,False)])
def test_query_table_dynamic_ranges_preserve_boundaries_duplicate_order_and_null_keys(tmp_path, include_start, include_end):
    rows = [{'k':k,'id':i} for i,k in enumerate([0,1,1,2,3,None])]
    path = _saved_table(tmp_path / 'right', rows, 'k', hl.tstruct(k=hl.tint32,id=hl.tint32))
    outer = hl.Table.parallelize([{'a':1,'b':2},{'a':2,'b':1},{'a':None,'b':None},{'a':0,'b':None}],
                                 schema=hl.tstruct(a=hl.tint32,b=hl.tint32),n_partitions=3)
    result = outer.annotate(matches=hl.query_table(path,hl.interval(outer.a,outer.b,includes_start=include_start,includes_end=include_end)))
    def order(value): return (value is None, value or 0)
    expected = []
    for a,b in [(1,2),(2,1),(None,None),(0,None)]:
        expected.append([hl.Struct(**row) for row in rows
                         if (order(row['k'])>order(a) or include_start and order(row['k'])==order(a))
                         and (order(row['k'])<order(b) or include_end and order(row['k'])==order(b))])
    assert [row.matches for row in result.collect()] == expected
    assert [row.matches for row in result.collect()] == expected


def test_query_table_dynamic_float_key_total_order_matches_boundaries(tmp_path):
    values = [float('-inf'),-0.,0.,1.,float('inf'),float('nan'),None]
    path = _saved_table(tmp_path/'right',[{'k':value,'id':i} for i,value in enumerate(values)],
                        'k',hl.tstruct(k=hl.tfloat64,id=hl.tint32))
    outer = hl.Table.parallelize([{'a':a,'b':b} for a,b in [(float('-inf'),float('inf')),(-0.,0.),(float('nan'),float('nan')),(1.,None)]],
                                 schema=hl.tstruct(a=hl.tfloat64,b=hl.tfloat64))
    for inclusive,expected in [(False,[[0,1,2,3],[1],[],[3,4,5]]),(True,[[0,1,2,3,4],[1,2],[5],[3,4,5,6]])]:
        result = outer.annotate(matches=hl.query_table(path,hl.interval(outer.a,outer.b,includes_end=inclusive)))
        assert [[value.id for value in row.matches] for row in result.collect()] == expected


def test_query_table_empty_right_and_whole_missing_interval(tmp_path):
    path = _saved_table(tmp_path/'empty',[],'k',hl.tstruct(k=hl.tint32))
    outer = hl.range_table(3)
    result = outer.annotate(matches=hl.query_table(path,hl.interval(outer.idx,outer.idx+1)))
    assert [row.matches for row in result.collect()] == [[],[],[]]
    interval = hl.missing(hl.tinterval(hl.tint32))
    assert hl.eval(hl.query_table(path,interval)) is None
    outer = outer.annotate(q=interval)
    assert [row.matches for row in outer.annotate(matches=hl.query_table(path,outer.q)).collect()] == [None,None,None]


def test_all_missing_dynamic_intervals_do_not_read_right_source(tmp_path):
    path = tmp_path/'right'
    hl.range_table(3).write(str(path))
    outer = hl.range_table(3).annotate(q=hl.missing(hl.tinterval(hl.tint32)))
    result = outer.annotate(matches=hl.query_table(str(path),outer.q))
    path.rename(tmp_path/'moved')
    assert [row.matches for row in result.collect()] == [None,None,None]


def test_query_reserved_names_and_matrix_global_arguments(tmp_path):
    table = hl.range_table(3).annotate_globals(__vail_query_key=7,__vail_query_payload=9)
    path = tmp_path/'right';table.write(str(path))
    outer = hl.range_table(3)
    result = outer.annotate(matches=hl.query_table(str(path),outer.idx))
    assert [row.matches for row in result.collect()] == [[hl.Struct(idx=i)] for i in range(3)]
    matrix = hl.range_matrix_table(2,0).annotate_globals(g=1)
    query = hl.query_table(str(path),matrix.g)
    assert hl.eval(query) == [hl.Struct(idx=1)]


@pytest.mark.parametrize('value', [False,True,None])
@pytest.mark.parametrize('missing_false', [False,True])
def test_query_table_global_condition_selects_only_needed_query(tmp_path,value,missing_false):
    path = _saved_table(tmp_path/'right',[{'k':1}],'k')
    source = hl.range_table(0).annotate_globals(flag=hl.literal(value,hl.tbool), bad=hl.int32('bad'))
    # A separate valid globals source prevents defining the selector itself
    # from executing the intentionally invalid query argument.
    flag_source = hl.range_table(0).annotate_globals(flag=hl.literal(value,hl.tbool))
    bad_query = hl.query_table(path,source.bad)
    safe = hl.empty_array(hl.tstruct(k=hl.tint32))
    result = hl.if_else(flag_source.flag,bad_query,safe,missing_false=missing_false)
    if value is True:
        with pytest.raises(RuntimeError,match='bad|parse|cast'):
            hl.eval(result)
    else:
        assert hl.eval(result) == (None if value is None and not missing_false else [])


def test_query_table_computed_condition_and_unselected_source(tmp_path):
    path = tmp_path/'right'
    hl.range_table(2).write(str(path))
    query = hl.query_table(str(path),hl.int32('1'))
    result = hl.if_else(hl.len(hl.range(3))==7,query,hl.empty_array(hl.tstruct(idx=hl.tint32)))
    path.rename(tmp_path/'moved')
    assert hl.eval(result) == []


def test_query_table_nested_conditionals_and_action_freshness(tmp_path):
    path = _saved_table(tmp_path/'right',[{'k':1}],'k')
    flag_path = tmp_path/'flag.txt';flag_path.write_text('0\n')
    flags = hl.import_lines(str(flag_path))
    condition = flags.aggregate(hl.agg.any(flags.text=='1'),_localize=False)
    source = hl.range_table(0).annotate_globals(bad=hl.int32('bad'))
    bad = hl.query_table(path,source.bad)
    good = hl.query_table(path,1)
    nested = hl.if_else(condition,bad,hl.if_else(hl.len(hl.range(2))>0,good,bad))
    assert hl.eval(nested) == [hl.Struct(k=1)]
    flag_path.write_text('1\n')
    with pytest.raises(RuntimeError,match='bad|parse|cast'):
        hl.eval(nested)


def test_query_table_dynamic_composite_and_unicode_prefixes(tmp_path):
    rows=[{'a':a,'b':b} for a,b in [('a',0),('a',1),('b',0),('α',1),(None,0)]]
    path=_saved_table(tmp_path/'right',rows,['a','b'],hl.tstruct(a=hl.tstr,b=hl.tint32))
    outer=hl.Table.parallelize([{'start':hl.Struct(x='a',y=1),'end':hl.Struct(x='α',y=1)}])
    result=outer.annotate(matches=hl.query_table(path,hl.interval(outer.start,outer.end)))
    assert result.collect()[0].matches==[hl.Struct(a='a',b=1),hl.Struct(a='b',b=0)]
    prefix=hl.Table.parallelize([{'start':'a','end':'α'}])
    result=prefix.annotate(matches=hl.query_table(path,hl.interval(prefix.start,prefix.end)))
    assert result.collect()[0].matches==[hl.Struct(**row) for row in rows[:3]]


@pytest.mark.parametrize('dtype,values', [
    (hl.ttuple(hl.tint32,hl.tint32),[(1,0),(1,1),(2,0)]),
    (hl.tarray(hl.tint32),[[1],[1,1],[2]]),
    (hl.tlocus('GRCh37'),[hl.Locus('1',1,'GRCh37'),hl.Locus('1',2,'GRCh37'),hl.Locus('2',1,'GRCh37')]),
])
def test_query_table_dynamic_nested_key_types(tmp_path,dtype,values):
    path=_saved_table(tmp_path/'right',[{'k':value} for value in values],'k',hl.tstruct(k=dtype))
    outer=hl.Table.parallelize([{'start':values[0],'end':values[2]}],schema=hl.tstruct(start=dtype,end=dtype))
    result=outer.annotate(matches=hl.query_table(path,hl.interval(outer.start,outer.end)))
    assert result.collect()[0].matches==[hl.Struct(k=value) for value in values[:2]]
    assert hl.eval(hl.query_table(path,hl.literal(values[1],dtype)))==[hl.Struct(k=values[1])]


def test_query_table_multiple_left_batches_and_partition_counts(tmp_path):
    path=_saved_table(tmp_path/'right',[{'k':k,'id':i} for i,k in enumerate([0,0,1,1,2,2])], 'k')
    for partitions in [1,4,17]:
        outer=hl.range_table(17000,n_partitions=partitions)
        result=outer.annotate(matches=hl.query_table(path,hl.interval(outer.idx%3,outer.idx%3+1)))
        assert result.n_partitions()==partitions
        assert result.aggregate(hl.agg.sum(hl.len(result.matches)))==34000


def test_query_scalar_dependencies_preserve_lambda_random_and_row_context(tmp_path):
    path=_saved_table(tmp_path/'right',[{'k':1}],'k')
    query=hl.query_table(path,1)
    empty=hl.empty_array(hl.tstruct(k=hl.tint32))
    masks=hl.eval(hl.range(8).map(lambda _:hl.rand_bool(.5,seed=9)))
    random_results=hl.range(8).map(lambda _:hl.if_else(hl.rand_bool(.5,seed=9),query,empty))
    assert hl.eval(random_results)==[[hl.Struct(k=1)] if mask else [] for mask in masks]
    assert hl.eval(hl.range(3).map(lambda i:hl.if_else(i%2==0,query,empty)))==[[hl.Struct(k=1)],[],[hl.Struct(k=1)]]
    table=hl.range_table(4)
    assert hl.if_else(True,hl.len(query),table.idx).collect()==[1,1,1,1]


@pytest.mark.parametrize('dtype,values,start,end', [
    (hl.ttuple(hl.tint32,hl.tint32),[(1,None),(1,0),(1,2),(2,0)],(1,0),(2,0)),
    (hl.tarray(hl.tint32),[[1,None],[1,0],[1,2],[2,0]],[1,0],[2,0]),
    (hl.tstruct(x=hl.tint32,y=hl.tint32),[hl.Struct(x=1,y=None),hl.Struct(x=1,y=0),hl.Struct(x=1,y=2),hl.Struct(x=2,y=0)],hl.Struct(x=1,y=0),hl.Struct(x=2,y=0)),
])
@pytest.mark.parametrize('boundary',['defined','missing_start','missing_end'])
def test_query_table_nested_missing_bounds_match_public_interval_contains(tmp_path,dtype,values,start,end,boundary):
    if boundary == 'missing_start':
        start=values[0]
    elif boundary == 'missing_end':
        end=values[0]
    table=hl.Table.parallelize([{'k':value,'id':i} for i,value in enumerate(values)],schema=hl.tstruct(k=dtype,id=hl.tint32),key='k')
    path=str(tmp_path/'right');table.write(path)
    for include_start,include_end in [(True,False),(True,True),(False,False),(False,True)]:
        interval=hl.interval(hl.literal(start,dtype),hl.literal(end,dtype),includes_start=include_start,includes_end=include_end)
        expected=table.filter(interval.contains(table.k)).collect()
        outer=hl.Table.parallelize([{'start':start,'end':end}],schema=hl.tstruct(start=dtype,end=dtype))
        query=hl.interval(outer.start,outer.end,includes_start=include_start,includes_end=include_end)
        actual=outer.annotate(matches=hl.query_table(path,query)).collect()[0].matches
        assert actual==expected


def test_query_scalar_shared_between_selector_and_selected_branch(tmp_path):
    path=_saved_table(tmp_path/'right',[{'k':0},{'k':1},{'k':2}],'k')
    query=hl.query_table(path,hl.interval(0,3))
    empty=hl.empty_array(hl.tstruct(k=hl.tint32))
    value=hl.if_else(hl.len(query)>0,query,empty)
    assert hl.eval(value)==[hl.Struct(k=i) for i in range(3)]
    assert hl.eval(hl.len(value)+hl.len(value))==6


def test_query_scalar_condition_projects_only_used_globals(tmp_path):
    path=_saved_table(tmp_path/'right',[{'k':1}],'k')
    source=hl.range_table(0).annotate_globals(flag=False,bad=hl.int32('bad'))
    bad=hl.query_table(path,source.bad)
    empty=hl.empty_array(hl.tstruct(k=hl.tint32))
    assert hl.eval(hl.if_else(source.flag,bad,empty))==[]
    condition=hl.coalesce(source.flag,hl.len(bad)>0)
    assert hl.eval(hl.if_else(condition,bad,empty))==[]
