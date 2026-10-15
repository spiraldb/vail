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
    # Four distinct keys cap the keyed shuffle at four output ranges.
    assert 0 < shuffled.n_partitions() <= 4
    assert shuffled.row.dtype == table.row.dtype
    assert shuffled.key.dtype == table.key.dtype
    assert shuffled.globals.dtype == table.globals.dtype
    assert shuffled.aggregate(shuffled.label) == "kept"

    coalesced = table.repartition(2, shuffle=False)
    assert coalesced.count() == len(expected)
    assert coalesced.n_partitions() <= 2
    assert sorted((row.k, row.value) for row in coalesced.collect()) == sorted(
        (row.k, row.value) for row in expected
    )
    assert coalesced.key.dtype == table.key.dtype


def test_naive_coalesce_and_filter_partitions_handle_empty_partitions():
    table = hl.Table.parallelize(
        [{"x": 0}, {"x": 1}], schema=hl.tstruct(x=hl.tint32), n_partitions=5,
    )
    assert table.n_partitions() == 5
    kept = table._filter_partitions([0, 4]).collect()
    discarded = table._filter_partitions([0, 4], keep=False).collect()
    assert sorted(row.x for row in kept + discarded) == [0, 1]
    assert len(kept) + len(discarded) == 2
    empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32), n_partitions=5)
    assert empty._filter_partitions([1, 3]).collect() == []
    assert empty._filter_partitions([]).count() == 0
    assert empty._filter_partitions(range(3)).n_partitions() == 3

    reduced = table.naive_coalesce(2)
    assert reduced.count() == 2
    assert reduced.n_partitions() <= 2
    assert sorted(row.x for row in reduced.collect()) == [0, 1]
    assert reduced.row.dtype == table.row.dtype


def test_range_repartition_and_naive_coalesce_follow_hail_oracle():
    table = hl.range_table(10, n_partitions=4)
    expanded = table.naive_coalesce(9)
    assert expanded.n_partitions() == 9
    assert expanded.idx.collect() == list(range(10))
    assert table.repartition(5, shuffle=False).n_partitions() == 5


def test_calculate_new_partitions_without_keys_returns_empty_list():
    table = hl.Table.parallelize([{"x": 1}], n_partitions=2)
    assert table._calculate_new_partitions(4) == []


def test_calculate_new_partitions_zero_returns_empty_list():
    table = hl.range_table(10, n_partitions=4)
    assert table._calculate_new_partitions(0) == []
    from vail.arrow import read_arrow
    assert read_arrow(table._native.calculate_new_partitions(0))[1].num_rows == 0


def test_calculate_new_partitions_matches_captured_hail_ranges():
    ranged = hl.range_table(10, n_partitions=4)
    bounds = ranged._calculate_new_partitions(2)
    assert [
        (bound.start.idx, bound.end.idx, bound.includes_start, bound.includes_end)
        for bound in bounds
    ] == [(0, 5, True, False), (5, 9, True, True)]

    duplicates = hl.Table.parallelize(
        [{"k": 1, "v": i} for i in range(5)] + [{"k": 2, "v": 5}],
        schema=hl.tstruct(k=hl.tint32, v=hl.tint32), key="k", n_partitions=2,
    )
    duplicate_bounds = duplicates._calculate_new_partitions(10)
    assert [
        (bound.start.k, bound.end.k, bound.includes_start, bound.includes_end)
        for bound in duplicate_bounds
    ] == [(1, 2, True, True), (2, 2, True, True)]


def test_parallelized_naive_coalesce_caps_and_combines_adjacent_partitions():
    table = hl.Table.parallelize(
        [{"x": x} for x in range(10)], schema=hl.tstruct(x=hl.tint32), n_partitions=4,
    )
    original = [table._filter_partitions([i]).collect() for i in range(4)]
    for n in (5, 9):
        result = table.naive_coalesce(n)
        assert result.n_partitions() == 4
        assert result.collect() == table.collect()

    reduced = table.naive_coalesce(2)
    assert reduced.n_partitions() == 2
    assert reduced._filter_partitions([0]).collect() == original[0] + original[1]
    assert reduced._filter_partitions([1]).collect() == original[2] + original[3]


def test_filter_partitions_accepts_range_and_duplicate_ids_are_set_like():
    table = hl.range_table(10, n_partitions=4)
    selected = table._filter_partitions(range(3))
    assert selected.n_partitions() == 3
    assert selected.idx.collect() == list(range(8))
    once = table._filter_partitions([1])
    duplicated = table._filter_partitions([1, 1])
    assert duplicated.n_partitions() == once.n_partitions() == 1
    assert duplicated.idx.collect() == once.idx.collect()


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


def test_group_within_partitions_keeps_partition_boundaries():
    table = hl.range_table(10, n_partitions=4)
    grouped = table._group_within_partitions("g", 3)
    assert [(row.idx, [item.idx for item in row.g]) for row in grouped.collect()] == [
        (0, [0, 1, 2]), (3, [3, 4, 5]), (6, [6, 7]), (8, [8, 9]),
    ]


def test_partition_operation_argument_validation():
    table = _partitioned_table()
    with pytest.raises(TypeError, match="shuffle must be a bool"):
        table.repartition(2, shuffle=1)
    with pytest.raises(ValueError, match="positive"):
        table.repartition(0)
    with pytest.raises(TypeError, match="integers"):
        table._filter_partitions([True])
    with pytest.raises(TypeError, match="keep must be a bool"):
        table._filter_partitions([], keep=0)
    with pytest.raises(hl.ExpressionException, match="Cannot select and assign"):
        table._group_within_partitions("k", 2)


def test_grouped_partition_hint_applies_to_result():
    table = _partitioned_table()
    grouped = table.group_by("k")
    assert grouped.partition_hint(2) is grouped
    result = grouped.aggregate(n=hl.agg.count())
    assert result.n_partitions() == 2
    assert sorted((row.k, row.n) for row in result.collect()) == [(1, 2), (2, 1), (3, 2), (4, 1)]


def test_read_partition_count_calculates_ranges_and_roundtrips(tmp_path):
    path = tmp_path / "source"
    table = hl.Table.parallelize(
        [{"k": k, "value": str(k)} for k in range(12)],
        schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k", n_partitions=3,
        globals={"source": 42},
    )
    table.write(path)
    loaded = hl.read_table(path, _n_partitions=4)
    assert 0 < loaded.n_partitions() <= 4
    assert sorted(row.k for row in loaded.collect()) == list(range(12))
    assert loaded.row.dtype == table.row.dtype
    assert loaded.key.dtype == table.key.dtype
    assert loaded.globals.dtype == table.globals.dtype
    assert loaded.aggregate(loaded.source) == 42
    with pytest.raises(ValueError, match="both"):
        hl.read_table(path, _intervals=[], _n_partitions=2)
