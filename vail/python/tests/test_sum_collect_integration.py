"""Collection identity composes with typed nullable field reduction."""
import pytest
import vail as hl

@pytest.mark.parametrize('strict',[False,True])
def test_collected_nullable_records_keep_parent_and_field_missingness(strict):
    values=hl.array([hl.struct(dp=7),hl.missing(hl.tstruct(dp=hl.tint32)),hl.struct(dp=hl.missing(hl.tint32)),hl.struct(dp=-2)])
    collected=values.aggregate(lambda value:hl.agg.collect(value))
    assert hl.eval(hl.sum(collected.map(lambda value:value.dp),filter_missing=not strict))==(None if strict else 5)
    assert hl.eval(hl.sum(collected[3:].map(lambda value:value.dp),filter_missing=not strict))==-2

@pytest.mark.parametrize('partitions',[1,3])
def test_stored_sparse_entry_collect_then_field_sum(partitions,tmp_path):
    matrix=hl.utils.range_matrix_table(4,9,partitions)
    matrix=matrix.annotate_entries(dp=hl.or_missing(matrix.col_idx%3!=0,hl.int32(matrix.row_idx+matrix.col_idx)))
    matrix=matrix.filter_entries((matrix.row_idx!=1)&(matrix.col_idx%2==0))
    table=matrix.annotate_rows(values=hl.agg.collect(hl.struct(dp=matrix.dp))).rows()
    path=str(tmp_path/'records.ht');table.write(path);table=hl.read_table(path)
    table=table.annotate(total=hl.sum(table['values'].map(lambda record:record.dp)))
    assert [(r.row_idx,r.total) for r in table.collect()]==[(row,sum(row+col for col in range(9) if row!=1 and col%2==0 and col%3!=0)) for row in range(4)]
