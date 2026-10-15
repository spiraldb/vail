import pytest
import vail as hl


def test_switch_values_and_missingness():
    x = hl.literal('1')
    assert hl.eval(hl.switch(x).when('123', 5).when('1', 6).when('0', 2).or_missing()) == 6
    assert hl.eval(hl.switch(x).when('123', 5).when('0', 2).or_missing()) is None
    assert hl.eval(hl.switch(x).when('123', 5).when('0', 2).default(100)) == 100
    na = hl.missing(hl.tint32)
    assert hl.eval(hl.switch(na).when(5, 0).when(na, 3).default(4)) is None
    assert hl.eval(hl.switch(na).when(5, 0).when(na, 3).when_missing(-1).default(4)) == -1
    assert hl.eval(hl.switch(na).when_missing(7).default(9)) == 7
    assert hl.eval(hl.switch(0).when_missing(7).default(9)) == 9
    assert hl.eval(hl.switch(0).default(9)) == 9
    assert hl.eval(hl.switch(1).when(1, 3).when(1, 4).default(5)) == 3
    assert hl.eval(hl.switch(0).when(hl.missing(hl.tint32), 3).when(0, 4).default(5)) is None


def test_switch_types_and_validation():
    expr = hl.switch(0).when(0, hl.int64(3)).default(hl.float32(4))
    assert expr.dtype == hl.tfloat32
    assert hl.eval(expr) == 3.0
    with pytest.raises(TypeError, match='cannot compare'):
        hl.switch(1).when('1', 3)
    with pytest.raises(TypeError, match='same type'):
        hl.switch(1).when(1, [1]).default([1.0])
    with pytest.raises(hl.ExpressionException, match='only be called once'):
        hl.switch(1).when_missing(0).when_missing(1)
    for finish in ('or_missing', 'or_error'):
        with pytest.raises(hl.ExpressionException, match='at least one'):
            getattr(hl.switch(1).when_missing(0), finish)(*(['oops'] if finish == 'or_error' else []))
    with pytest.raises(TypeError, match='str type'):
        hl.switch(1).when(1, 2).or_error(3)


def test_switch_lazy_errors_and_indexed_source():
    assert hl.eval(hl.switch(1).when(1, 9).or_error('unmatched')) == 9
    with pytest.raises(RuntimeError, match='unmatched'):
        hl.eval(hl.switch(2).when(1, 9).or_error('unmatched'))
    table = hl.range_table(3)
    table = table.annotate(value=hl.switch(table.idx).when(0, 'a').when(1, 'b').default('c'))
    assert [row.value for row in table.collect()] == ['a', 'b', 'c']
    other = hl.range_table(3)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.switch(table.idx).when(1, other.idx).default(0)


def test_switch_bound_selector_keeps_globals_missingness_and_lazy_errors():
    table=hl.Table.parallelize([{'s':'0','use':True},{'s':None,'use':True},{'s':'bad','use':False}],
                               schema=hl.tstruct(s=hl.tstr,use=hl.tbool)).annotate_globals(g=7)
    value=(hl.switch(hl.int32(table.s)).when(0,table.g).when(1,hl.int32('unselected'))
           .when_missing(-1).default(2))
    assert hl.if_else(table.use,value,-9).collect()==[7,-1,-9]
    assert hl.eval(hl.switch(0).when(0,7).when(hl.int32('invalid'),9).default(1))==7


def test_switch_binding_preserves_seeded_random_selector_context():
    table=hl.range_table(64,n_partitions=3)
    selector=hl.rand_int32(0,3,seed=77)
    value=hl.switch(selector).when(0,10).when(1,11).when(2,12).when_missing(-1).default(-2)
    rows=table.select(actual=value,expected=selector+10).collect()
    assert [row.actual for row in rows]==[row.expected for row in rows]


def test_switch_result_branches_keep_array_random_context():
    actual=hl.range(8).map(lambda i:hl.switch(i%2).when(0,hl.rand_int32(100,seed=77)).default(0))
    expected=hl.range(8).map(lambda i:hl.if_else(i%2==0,hl.rand_int32(100,seed=77),0))
    values=hl.eval(actual)
    assert values==hl.eval(expected)
    assert len(set(values[::2]))>1


def test_switch_result_branches_lower_matrix_row_and_column_fields():
    matrix=hl.range_matrix_table(3,4,2)
    result=(hl.switch(matrix.row_idx%2).when(0,matrix.row_idx+matrix.col_idx)
            .default(matrix.row_idx-matrix.col_idx))
    matrix=matrix.annotate_entries(result=result)
    rows=matrix.entries().collect()
    assert [(r.row_idx,r.col_idx,r.result) for r in rows]==[
        (i,j,i+j if i%2==0 else i-j) for i in range(3) for j in range(4)]


def test_switch_nested_results_keep_public_arrow_types_and_values():
    dtype=hl.ttuple(hl.tcall,hl.tlocus('GRCh37'),hl.tarray(hl.tint32))
    one=(hl.Call([0,1]),hl.Locus('1',1),[1,None])
    two=(hl.Call([1,0],True),hl.Locus('1',2),[])
    table=hl.range_table(3)
    value=hl.switch(table.idx).when(0,hl.literal(one,dtype)).default(hl.literal(two,dtype))
    result=table.select(value=value)
    assert result.value.dtype==dtype
    assert result.value.collect()==[one,two,two]
    assert hl.Table.parallelize(result.to_arrow()).collect()==result.collect()


def test_uniform_switch_nested_result_shares_public_arrow_value_buffers():
    import pyarrow as pa
    values=pa.array([[1,None],[2,3]],type=pa.list_(pa.int32()))
    address=values.values.buffers()[1].address
    table=hl.Table.parallelize(pa.table({'choice':pa.array([0,0],type=pa.int32()),'values':values}))
    selected=hl.switch(table.choice).when(0,table['values']).default(hl.literal([9],hl.tarray(hl.tint32)))
    exported=table.select(selected=selected).to_arrow()
    assert exported['selected'].to_pylist()==[[1,None],[2,3]]
    assert exported['selected'].chunk(0).values.buffers()[1].address==address


def test_nested_switch_and_array_lambda_keep_independent_selector_scopes():
    table=hl.Table.parallelize([{'choice':0,'a':[1,2,3]},{'choice':1,'a':[4,5]}])
    mapped=table.a.map(lambda x:hl.switch(x).when(1,x+table.choice).default(x*2))
    value=hl.switch(table.choice).when(0,mapped).default(table.a)
    assert value.collect()==[[1,4,6],[4,5]]
