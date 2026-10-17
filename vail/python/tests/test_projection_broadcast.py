"""Public Table outcomes for nested constants and row-dependent expressions."""
import vail as hl


def test_nested_projection_values_across_partitions_selections_and_actions():
    table = hl.range_table(16391, n_partitions=2)
    nested = hl.range(3).map(
        lambda i: hl.struct(value=i, maybe=hl.missing(hl.tint32), items=hl.array([i, i + 1]))
    )
    result = table.annotate(
        nested=nested,
        selected=hl.if_else(table.idx % 2 == 0, nested, hl.empty_array(nested.dtype.element_type)),
        dynamic=hl.array([table.idx, hl.missing(hl.tint32)]),
    )
    expected = [hl.Struct(value=i, maybe=None, items=[i, i + 1]) for i in range(3)]
    rows = result.collect()
    assert len(rows) == 16391
    for row in rows:
        assert row.nested == expected
        assert row.selected == (expected if row.idx % 2 == 0 else [])
        assert row.dynamic == [row.idx, None]
    assert result._force_count() == len(rows)
    assert result.take(3) == rows[:3]
    assert result.head(0).collect() == []


def test_nested_projection_global_captures_remain_bound_to_each_table():
    def values(offset):
        table = hl.range_table(17, n_partitions=3).annotate_globals(offset=offset)
        return table.annotate(values=hl.range(3).map(lambda i: i + table.offset))

    first = values(7)
    second = values(29)
    assert [row["values"] for row in first.collect()] == [[7, 8, 9]] * 17
    assert [row["values"] for row in second.collect()] == [[29, 30, 31]] * 17
    assert [row["values"] for row in first.collect()] == [[7, 8, 9]] * 17


def test_failing_nested_projection_is_lazy_until_selected_rows_are_demanded():
    import pytest

    failing = hl.range(1).map(lambda _: hl.int32("vail-projection-invalid"))
    empty = hl.range_table(0).annotate(values=failing)
    assert empty.collect() == []
    source = hl.range_table(3, n_partitions=2)
    unselected = source.annotate(
        values=hl.if_else(source.idx < 0, failing, hl.empty_array(hl.tint32))
    )
    assert [row["values"] for row in unselected.collect()] == [[], [], []]
    selected = hl.range_table(1).annotate(values=failing)
    for _ in range(2):
        with pytest.raises(Exception, match="vail-projection-invalid"):
            selected.collect()


def test_nested_random_projection_keeps_per_row_context():
    table = hl.range_table(32, n_partitions=3)
    result = table.annotate(samples=hl.range(2).map(lambda _: hl.rand_unif(seed=7)))
    rows = result.collect()
    samples = [row.samples for row in rows]
    assert any(value != samples[0] for value in samples[1:])
    assert [row.samples for row in result.collect()] == samples
