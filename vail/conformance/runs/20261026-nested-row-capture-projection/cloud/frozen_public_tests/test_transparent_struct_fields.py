"""Public field-selection semantics; no optimizer topology assertions."""
import pytest
import vail as hl


def test_selected_field_over_pure_row_constructor():
    table = hl.range_table(9, n_partitions=3).key_by()
    table = table.select(record=hl.struct(kept=table.idx * 3, sibling=table.idx + 40))
    table = table.select(value=table.record.kept)
    assert sorted(row.value for row in table.collect()) == list(range(0, 27, 3))


def test_nullable_ancestor_hides_defined_child():
    table = hl.range_table(6, n_partitions=2).key_by()
    table = table.annotate(record=hl.or_missing(table.idx % 2 == 0,
        hl.struct(child=hl.struct(value=table.idx + 10))))
    table = table.select(value=table.record.child.value)
    assert sorted((row.value for row in table.collect()), key=lambda x: (-1 if x is None else x)) == [None, None, None, 10, 12, 14]


def test_shared_scalar_global_struct_keeps_collection_types():
    table = hl.range_table(4, n_partitions=2).key_by().annotate_globals(
        config=hl.struct(values=hl.set([2, 3]), nested=hl.struct(label='ready')))
    table = table.select(values=table.config["values"], label=table.config.nested.label)
    assert all(row["values"] == {2, 3} and row.label == 'ready' for row in table.collect())
    assert table["values"].dtype == hl.tset(hl.tint32)


def test_array_struct_fields_keep_null_elements():
    table = hl.range_table(3, n_partitions=2).key_by()
    table = table.select(values=hl.array([hl.struct(x=table.idx), hl.missing(hl.tstruct(x=hl.tint32))]).map(lambda item: item.x))
    assert sorted((row["values"] for row in table.collect()), key=lambda xs: xs[0]) == [[0, None], [1, None], [2, None]]


def test_consumed_error_preserves_failure():
    table = hl.range_table(2, n_partitions=1).key_by()
    table = table.select(record=hl.struct(kept=table.idx, invalid=hl.int32('not-an-integer')))
    table = table.select(value=table.record.invalid)
    with pytest.raises(Exception, match='[Cc]ast|pars|[Ii]nvalid'):
        table.collect()


def test_virtual_call_and_interval_fields_preserve_contract():
    assert hl.eval(hl.call(0, 1).ploidy) == 2
    assert hl.eval(hl.interval(1, 4, includes_start=False).includes_start) is False
    assert hl.eval(hl.interval(1, 4).end) == 4


@pytest.mark.parametrize('width', [16, 64, 256])
def test_many_fields_share_pure_base_without_changing_values(width):
    table = hl.range_table(2, n_partitions=1).key_by()
    base = hl.struct(**{f'f{i}': table.idx + i for i in range(width)})
    table = table.select(**{f'v{i}': base[f'f{i}'] for i in range(width)})
    rows = table.collect()
    assert sorted(tuple(row[f'v{i}'] for i in range(width)) for row in rows) == [
        tuple(i + row for i in range(width)) for row in range(2)
    ]


@pytest.mark.parametrize('extend', [False, True])
def test_replacing_struct_schema_then_selecting_new_field(extend):
    table = hl.range_table(2, n_partitions=1).key_by()
    table = table.annotate(record=hl.struct(original=table.idx))
    replacement = table.record.annotate(added=table.idx + 1) if extend else hl.struct(added=table.idx + 1)
    table = table.annotate(record=replacement)
    table = table.select(value=table.record.added)
    assert [row.value for row in table.collect()] == [1, 2]
