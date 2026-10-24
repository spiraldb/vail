"""Collecting each list retains its elements without scalar accumulator semantics."""
import pytest
import vail as hl

@pytest.mark.parametrize('partitions',[1,3])
def test_row_collect_keeps_nested_missing_and_filtered_entries(partitions,tmp_path):
    m=hl.utils.range_matrix_table(4,7,partitions)
    m=m.annotate_entries(value=hl.or_missing(m.col_idx%3!=0,hl.struct(x=m.row_idx+m.col_idx, ys=hl.array([m.col_idx,hl.missing(hl.tint32)]))))
    m=m.filter_entries((m.row_idx!=1)&(m.col_idx%2==0))
    result=m.annotate_rows(values=hl.agg.collect(m.value),empty=hl.agg.collect(hl.struct())).rows().collect()
    for r in result:
        cols=[c for c in range(7) if r.row_idx!=1 and c%2==0]
        assert r['values']==[None if c%3==0 else hl.Struct(x=r.row_idx+c,ys=[c,None]) for c in cols]
        assert r.empty==[hl.Struct() for _ in cols]
    path=str(tmp_path/'roundtrip.ht')
    m.annotate_rows(values=hl.agg.collect(m.value)).rows().write(path)
    assert hl.read_table(path)['values'].collect()==[r['values'] for r in result]


def test_array_collect_preserves_null_empty_and_sliced_elements():
    values=hl.array([1,hl.missing(hl.tint32),3,4])
    assert hl.eval(values[1:3].aggregate(lambda x:hl.agg.collect(x)))==[None,3]
    assert hl.eval(hl.empty_array(hl.tint32).aggregate(lambda x:hl.agg.collect(x)))==[]
    assert hl.eval(hl.missing(hl.tarray(hl.tint32)).aggregate(lambda x:hl.agg.collect(x))) is None


def test_collect_evaluates_value_once_and_keeps_error_scope():
    values=hl.range(8)
    expected=hl.eval(values.map(lambda x:hl.rand_int32(100,seed=7)+x))
    actual=hl.eval(values.aggregate(lambda x:hl.agg.collect(hl.rand_int32(100,seed=7)+x)))
    assert actual==expected
    assert hl.eval(hl.array([0,1,2]).aggregate(lambda x:hl.agg.filter(x>0,hl.agg.collect(hl.int32(hl.str(x))))))==[1,2]
