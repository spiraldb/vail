import pytest

import vail as hl


def _partitioned_table():
    return hl.Table.parallelize(
        [{"k": k, "value": value} for k, value in [(1, "a"), (1, "b"), (2, "c"),
                                                    (3, "d"), (3, "e"), (4, "f")]],
        schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k", n_partitions=3,
        globals={"label": "kept"},
    )


def test_repartition_preserves_rows_types_keys_and_globals():
    table = _partitioned_table()
    expected = table.collect()

    shuffled = table.repartition(5)
    assert shuffled.count() == len(expected)
    assert sorted((row.k, row.value) for row in shuffled.collect()) == sorted(
        (row.k, row.value) for row in expected
    )
    assert shuffled.row.dtype == table.row.dtype
    assert shuffled.key.dtype == table.key.dtype
    assert shuffled.globals.dtype == table.globals.dtype
    assert shuffled.aggregate(shuffled.label) == "kept"

    coalesced = table.repartition(2, shuffle=False)
    assert coalesced.count() == len(expected)
    assert sorted((row.k, row.value) for row in coalesced.collect()) == sorted(
        (row.k, row.value) for row in expected
    )
    assert coalesced.key.dtype == table.key.dtype


def test_naive_coalesce_handles_empty_tables():
    table = hl.Table.parallelize(
        [{"x": 0}, {"x": 1}], schema=hl.tstruct(x=hl.tint32), n_partitions=5,
    )
    empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32), n_partitions=5)
    assert empty.naive_coalesce(2).collect() == []

    reduced = table.naive_coalesce(2)
    assert reduced.count() == 2
    assert sorted(row.x for row in reduced.collect()) == [0, 1]
    assert reduced.row.dtype == table.row.dtype


def test_range_repartition_and_naive_coalesce_preserve_values():
    table = hl.range_table(10, n_partitions=4)
    expanded = table.naive_coalesce(9)
    assert expanded.idx.collect() == list(range(10))
    assert table.repartition(5, shuffle=False).idx.collect() == list(range(10))


def test_calculate_new_partitions_without_keys_returns_empty_list():
    table = hl.Table.parallelize([{"x": 1}], n_partitions=2)
    assert table._calculate_new_partitions(4) == []


def test_calculate_new_partitions_zero_returns_empty_list():
    table = hl.range_table(10, n_partitions=4)
    assert table._calculate_new_partitions(0) == []
    from vail.arrow import read_arrow
    assert read_arrow(table._native.calculate_new_partitions(0))[1].num_rows == 0


def test_calculated_key_ranges_cover_input_keys():
    tables = [hl.range_table(10, n_partitions=4), hl.Table.parallelize(
        [{"idx": 1, "v": i} for i in range(5)] + [{"idx": 2, "v": 5}],
        key="idx", n_partitions=2)]
    for table in tables:
        bounds = table._calculate_new_partitions(4)
        assert [bound.start.idx for bound in bounds] == sorted(bound.start.idx for bound in bounds)
        for row in table.collect():
            assert any(
                (row.idx > bound.start.idx or (bound.includes_start and row.idx == bound.start.idx))
                and (row.idx < bound.end.idx or (bound.includes_end and row.idx == bound.end.idx))
                for bound in bounds)


def test_parallelized_naive_coalesce_preserves_values():
    table = hl.Table.parallelize(
        [{"x": x} for x in range(10)], schema=hl.tstruct(x=hl.tint32), n_partitions=4)
    for count in (2, 5, 9):
        assert table.naive_coalesce(count).collect() == table.collect()


def test_group_within_partitions_keeps_key_and_groups_duplicates():
    table = _partitioned_table()
    grouped = table._group_within_partitions("members", 2)
    assert grouped.key.dtype == table.key.dtype
    assert grouped.globals.dtype == table.globals.dtype
    assert grouped.row.dtype == hl.tstruct(k=hl.tint32, members=hl.tarray(table.row.dtype))
    members = [member for row in grouped.collect() for member in row.members]
    assert sorted((member.k, member.value) for member in members) == sorted(
        (row.k, row.value) for row in table.collect()
    )
    assert all(len(row.members) <= 2 for row in grouped.collect())
    replaced = table._group_within_partitions("value", 2)
    assert replaced.row.dtype == hl.tstruct(k=hl.tint32, value=hl.tarray(table.row.dtype))
    same_name_as_global = table.annotate_globals(members="global")._group_within_partitions("members", 2)
    assert same_name_as_global.globals.dtype == hl.tstruct(label=hl.tstr, members=hl.tstr)
    assert same_name_as_global.aggregate(same_name_as_global.globals.members) == "global"


def test_group_within_partitions_preserves_bounded_groups_and_row_order():
    table = hl.range_table(10, n_partitions=4)
    grouped = table._group_within_partitions("g", 3)
    rows = grouped.collect()
    assert [item.idx for row in rows for item in row.g] == list(range(10))
    assert all(0 < len(row.g) <= 3 for row in rows)


def test_partition_operation_argument_validation():
    table = _partitioned_table()
    with pytest.raises(TypeError, match="shuffle must be a bool"):
        table.repartition(2, shuffle=1)
    with pytest.raises(ValueError, match="positive"):
        table.repartition(0)
    with pytest.raises(hl.ExpressionException, match="Cannot select and assign"):
        table._group_within_partitions("k", 2)


def test_grouped_partition_hint_applies_to_result():
    table = _partitioned_table()
    grouped = table.group_by("k")
    assert grouped.partition_hint(2) is grouped
    result = grouped.aggregate(n=hl.agg.count())
    assert sorted((row.k, row.n) for row in result.collect()) == [(1, 2), (2, 1), (3, 2), (4, 1)]


def test_read_preserves_rows_keys_types_and_globals(tmp_path):
    path = tmp_path / "source"
    table = hl.Table.parallelize(
        [{"k": k, "value": str(k)} for k in range(12)],
        schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k", n_partitions=3,
        globals={"source": 42},
    )
    table.write(path)
    loaded = hl.read_table(path)
    assert sorted(row.k for row in loaded.collect()) == list(range(12))
    assert loaded.row.dtype == table.row.dtype
    assert loaded.key.dtype == table.key.dtype
    assert loaded.globals.dtype == table.globals.dtype
    assert loaded.aggregate(loaded.source) == 42
