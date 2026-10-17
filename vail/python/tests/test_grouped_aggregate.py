import json

import pytest
import vail as hl


@pytest.mark.parametrize("dtype", [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64])
def test_native_grouped_sum_widening_and_all_missing_identity(dtype):
    table = hl.Table.parallelize(
        [{"g": 0, "x": 2}, {"g": 0, "x": 3},
         {"g": 1, "x": None}, {"g": 1, "x": None}],
        schema=hl.tstruct(g=hl.tint32, x=dtype), n_partitions=3,
    )
    grouped = table.group_by(table.g).aggregate(
        total=hl.agg.sum(table.x), n=hl.agg.count(),
    )
    expected_type = hl.tint64 if dtype in (hl.tint32, hl.tint64) else hl.tfloat64
    assert grouped.total.dtype == expected_type
    assert [(row.g, row.total, row.n) for row in grouped.collect()] == [
        (0, 5, 2), (1, 0, 2),
    ]
    empty = hl.Table.parallelize([], schema=table.row.dtype)
    assert empty.aggregate(hl.agg.sum(empty.x)) == 0


def test_native_grouped_integer_sum_wraps_at_int64_boundary():
    table = hl.Table.parallelize(
        [{"g": 0, "x": 2**63 - 1}, {"g": 0, "x": 1}],
        schema=hl.tstruct(g=hl.tint32, x=hl.tint64), n_partitions=2,
    )
    assert table.group_by(table.g).aggregate(total=hl.agg.sum(table.x)).collect()[0].total == -(2**63)


def test_grouped_aggregate_counts_sums_computed_and_multiple_keys():
    table = hl.Table.parallelize(
        [{"group": "a", "sub": 1, "x": 2},
         {"group": "a", "sub": 1, "x": 3},
         {"group": "a", "sub": 2, "x": None},
         {"group": "b", "sub": 1, "x": 8}],
        schema=hl.tstruct(group=hl.tstr, sub=hl.tint32, x=hl.tint32),
        globals={"offset": 10}, n_partitions=2,
    )
    result = table.group_by("group", sub=table.sub + 1).aggregate(
        n=hl.agg.count(), total=hl.agg.sum(table.x), adjusted=hl.agg.sum(table.x + table.offset)
    )
    assert result._keys == ("group", "sub")
    assert { (r.group, r.sub): (r.n, r.total, r.adjusted) for r in result.collect() } == {
        ("a", 2): (2, 5, 25), ("a", 3): (1, 0, 0), ("b", 2): (1, 8, 18)
    }
    assert result._global_schema == table._global_schema


def test_grouped_aggregate_null_keys_empty_table_and_lazy_plan():
    table = hl.Table.parallelize(
        [{"g": None, "x": 2}, {"g": None, "x": 4}, {"g": "a", "x": 5}],
        schema=hl.tstruct(g=hl.tstr, x=hl.tint32),
    )
    lazy = table.group_by(table.g).aggregate(n=hl.agg.count(), total=hl.agg.sum(table.x))
    assert lazy._native.ir()["node"] == "TableGroupedAggregate"
    assert {r.g: (r.n, r.total) for r in lazy.collect()} == {None: (2, 6), "a": (1, 5)}
    empty = hl.Table.parallelize([], schema=hl.tstruct(g=hl.tstr, x=hl.tint32))
    empty_result = empty.group_by("g").aggregate(n=hl.agg.count())
    assert empty_result.collect() == []
    assert empty_result._keys == ("g",)


def test_grouped_aggregate_keeps_groups_with_no_contributing_rows():
    table = hl.Table.parallelize(
        [{"g": "filtered", "x": 1, "xs": []},
         {"g": "exploded", "x": 2, "xs": []},
         {"g": "both", "x": 3, "xs": [4]}],
        schema=hl.tstruct(g=hl.tstr, x=hl.tint32, xs=hl.tarray(hl.tint32)),
    )
    result = table.group_by("g").aggregate(
        filtered=hl.agg.filter(table.x < 0, hl.agg.sum(table.x)),
        exploded=hl.agg.explode(lambda element: hl.agg.sum(element), table.xs),
    )
    assert {row.g: (row.filtered, row.exploded) for row in result.collect()} == {
        "filtered": (0, 0), "exploded": (0, 0), "both": (0, 4)
    }


def test_grouped_aggregate_no_keys_globals_and_key_shadowing_are_lazy():
    table = hl.Table.parallelize(
        [{"g": 1, "x": 2}, {"g": 1, "x": 3}, {"g": 2, "x": 5}],
        schema=hl.tstruct(g=hl.tint32, x=hl.tint32), globals={"offset": 7},
    )
    global_group = table.group_by().aggregate(n=hl.agg.count(), total=hl.agg.sum(table.x))
    assert global_group._keys == ()
    assert [(row.n, row.total) for row in global_group.collect()] == [(3, 10)]
    assert global_group._global_schema == table._global_schema

    shadowed = table.group_by(g=table.g + 10).aggregate(
        n=hl.agg.count(), shifted_total=hl.agg.sum(table.x + table.offset)
    )
    assert shadowed._keys == ("g",)
    assert shadowed._native.ir()["node"] == "TableGroupedAggregate"
    assert {row.g: (row.n, row.shifted_total) for row in shadowed.collect()} == {
        11: (2, 19), 12: (1, 12)
    }


def test_grouped_aggregate_scope_source_and_name_validation():
    table = hl.Table.parallelize([{"g": 1, "x": 2}], schema=hl.tstruct(g=hl.tint32, x=hl.tint32))
    other = hl.range_table(1)
    with pytest.raises(ValueError, match="source mismatch"):
        table.group_by(g=other.idx)
    with pytest.raises(ValueError, match="aggregations"):
        table.group_by(g=hl.agg.sum(table.x))
    grouped = table.group_by("g")
    with pytest.raises(ValueError, match="inside an aggregate"):
        grouped.aggregate(bad=table.x)
    with pytest.raises(ValueError, match="collides"):
        grouped.aggregate(g=hl.agg.count())
    with pytest.raises(ValueError, match="duplicate grouping"):
        table.group_by("g", g=table.g)
    with pytest.raises(ValueError, match="nested aggregations"):
        grouped.aggregate(bad=hl.agg.sum(hl.agg.count()))
