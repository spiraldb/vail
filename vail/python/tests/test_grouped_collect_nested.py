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


@pytest.mark.parametrize('partitions', [1, 3])
def test_grouped_collect_localized_matrix_preserves_values_and_globals(tmp_path, partitions):
    path = str(tmp_path / 'localized.mt')
    matrix = hl.range_matrix_table(9, 4, n_partitions=partitions)
    matrix = matrix.annotate_entries(x=matrix.row_idx * 10 + matrix.col_idx)
    matrix.write(path)
    table = hl.read_matrix_table(path).localize_entries('entries', 'columns')
    grouped = table.group_by(*table.key).aggregate(values=hl.agg.collect(table.row_value))
    result = grouped.collect()
    assert [row.row_idx for row in result] == list(range(9))
    assert [[entry.x for entry in row['values'][0].entries] for row in result] == [
        [i * 10 + j for j in range(4)] for i in range(9)
    ]
    assert hl.eval(grouped.index_globals().columns) == [hl.Struct(col_idx=i) for i in range(4)]


def test_grouped_collect_referenced_globals_in_keys_inputs_and_fold():
    table = hl.range_table(6, n_partitions=3).annotate_globals(modulus=2, offset=10)
    grouped = table.group_by(key=table.idx % table.modulus).aggregate(
        values=hl.agg.collect(table.idx + table.offset),
        folded=hl.agg.fold(0, lambda state: state + table.idx + table.offset,
                           lambda left, right: left + right),
    )
    result = grouped.collect()
    assert [(row.key, sorted(row['values']), row.folded) for row in result] == [
        (0, [10, 12, 14], 36), (1, [11, 13, 15], 39),
    ]


def test_grouped_collect_many_partitions_force_count_and_values():
    table = hl.range_table(128, n_partitions=32)
    table = table.annotate(payload=hl.range(table.idx % 7).map(lambda j: hl.struct(x=j)))
    grouped = table.group_by(key=table.idx % 64).aggregate(values=hl.agg.collect(table.payload))
    assert grouped._force_count() == 64
    result = grouped.collect()
    assert [row.key for row in result] == list(range(64))
    for row in result:
        assert sorted(map(repr, row['values'])) == sorted(map(repr, [
            [hl.Struct(x=j) for j in range(row.key % 7)],
            [hl.Struct(x=j) for j in range((row.key + 64) % 7)],
        ]))
