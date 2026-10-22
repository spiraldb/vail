import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3])
@pytest.mark.parametrize('interleaved', [False, True])
def test_grouped_collect_nested_values_and_nulls(partitions, interleaved):
    rows = [
        {'key': 0, 'x': [hl.Struct(a=1), None, hl.Struct(a=None)]},
        {'key': 0, 'x': None},
        {'key': 1, 'x': []},
        {'key': 1, 'x': [hl.Struct(a=4)]},
        {'key': 2, 'x': [None]},
    ]
    if interleaved:
        rows = [rows[i] for i in [0, 2, 4, 1, 3]]
    table = hl.Table.parallelize(
        rows,
        schema=hl.tstruct(key=hl.tint32, x=hl.tarray(hl.tstruct(a=hl.tint32))),
        n_partitions=partitions,
    )
    result = table.group_by(key=table["key"]).aggregate(
        values=hl.agg.collect(table.x),
        filtered=hl.agg.filter(table["key"] != 1, hl.agg.collect(table.x)),
    ).collect()
    expected = {
        0: [[hl.Struct(a=1), None, hl.Struct(a=None)], None],
        1: [[], [hl.Struct(a=4)]],
        2: [[None]],
    }
    for row in result:
        assert sorted(map(repr, row["values"])) == sorted(map(repr, expected[row.key]))
        assert sorted(map(repr, row.filtered)) == (
            [] if row.key == 1 else sorted(map(repr, expected[row.key]))
        )


@pytest.mark.parametrize('partitions', [1, 3])
def test_grouped_collect_empty_structs(partitions):
    table = hl.range_table(9, n_partitions=partitions)
    table = table.annotate(empty=hl.or_missing(table.idx % 2 == 0, hl.struct()))
    result = table.group_by(key=table.idx // 3).aggregate(
        values=hl.agg.collect(table.empty),
    ).collect()
    assert [len(row["values"]) for row in result] == [3, 3, 3]
    assert [sum(value is None for value in row["values"]) for row in result] == [1, 2, 1]
