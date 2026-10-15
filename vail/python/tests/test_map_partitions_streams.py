import pytest

import vail as hl


def _table():
    return hl.Table.parallelize(
        [{"k": k, "x": x} for k, x in [(1, 0), (1, 3), (2, 2), (3, 1), (3, 4)]],
        schema=hl.tstruct(k=hl.tint32, x=hl.tint32), key="k", n_partitions=3,
    )


def test_map_partitions_maps_and_filters_rows_lazily():
    table = _table()
    calls = []

    def transform(rows):
        calls.append(rows)
        return rows.map(lambda row: row.annotate(y=row.x + 1)).filter(lambda row: row.x % 2 == 0)

    result = table._map_partitions(transform)
    assert len(calls) == 1
    assert result.collect() == [
        hl.Struct(k=1, x=0, y=1), hl.Struct(k=2, x=2, y=3), hl.Struct(k=3, x=4, y=5)
    ]
    assert result.key.dtype == table.key.dtype
    assert result.row.dtype == hl.tstruct(k=hl.tint32, x=hl.tint32, y=hl.tint32)


def test_map_partitions_flatmap_range_and_group_flatmap():
    table = _table()
    expanded = table._map_partitions(
        lambda rows: rows.flatmap(
            lambda row: hl._stream_range(row.x).map(lambda i: row.annotate(i=i))
        )
    )
    assert expanded.collect() == [
        hl.Struct(k=1, x=3, i=0), hl.Struct(k=1, x=3, i=1), hl.Struct(k=1, x=3, i=2),
        hl.Struct(k=2, x=2, i=0), hl.Struct(k=2, x=2, i=1),
        hl.Struct(k=3, x=1, i=0),
        hl.Struct(k=3, x=4, i=0), hl.Struct(k=3, x=4, i=1), hl.Struct(k=3, x=4, i=2),
        hl.Struct(k=3, x=4, i=3),
    ]

    regrouped = table._map_partitions(
        lambda rows: rows.grouped(2).flatmap(lambda group: group._to_stream())
    )
    assert regrouped.collect() == table.collect()
    assert regrouped.key.dtype == table.key.dtype


def test_map_partitions_handles_empty_partitions_and_preserves_globals():
    table = hl.Table.parallelize(
        [], schema=hl.tstruct(k=hl.tint32, x=hl.tint32), key="k", n_partitions=4,
        globals={"tag": "empty"},
    )
    result = table._map_partitions(lambda rows: rows.map(lambda row: row.annotate(y=1)))
    assert result.collect() == []
    assert result.n_partitions() == table.n_partitions()
    assert result.globals.dtype == table.globals.dtype
    assert result.aggregate(result.tag) == "empty"


def test_total_aggregate_fanout_runs_after_map_partitions():
    from vail.streams import StreamExpression

    table = hl.range_table(37, n_partitions=4)

    def annotate_partition_size(rows):
        values = rows.to_array()
        return StreamExpression(
            hl.range(hl.len(values)).map(
                lambda i: hl.struct(
                    idx=values[i].idx,
                    partition_size=hl.len(values),
                )
            )
        )

    mapped = table._map_partitions(annotate_partition_size)
    assert mapped.n_partitions() == table.n_partitions() == 4
    partition_rows = [
        mapped._filter_partitions([partition]).collect() for partition in range(4)
    ]
    sizes = [len(rows) for rows in partition_rows]
    assert all(
        row.partition_size == len(rows)
        for rows in partition_rows
        for row in rows
    )
    assert mapped.aggregate(hl.agg.sum(mapped.partition_size)) == sum(size * size for size in sizes)
    assert mapped.n_partitions() == 4


def test_map_partitions_requires_stream_of_structs_and_preserved_keys():
    table = _table()
    with pytest.raises(TypeError, match="stream expression"):
        table._map_partitions(lambda rows: rows.to_array())
    with pytest.raises(ValueError, match="preserve key"):
        table._map_partitions(lambda rows: rows.map(lambda row: row.drop("k")))
    with pytest.raises(TypeError, match="stream of structs"):
        table._map_partitions(lambda rows: rows.map(lambda row: row.x))
    with pytest.raises(TypeError, match="callable"):
        table._map_partitions(None)


def test_struct_annotate_and_drop_rebuild_lazy_fields():
    table = hl.range_table(4)
    result = table._map_partitions(
        lambda rows: rows.map(lambda row: row.annotate(extra=row.idx + 2).drop("extra"))
    )
    assert result.collect() == table.collect()


@pytest.mark.parametrize("n,partitions", [(0, 1), (1, 1), (1, 3), (5, 3)])
def test_map_partitions_rebuild_preserves_empty_structs(n, partitions):
    from vail.streams import StreamExpression

    table = hl.range_table(n, partitions).annotate(empty=hl.struct()).key_by()

    def rebuild(rows):
        values = rows.to_array()
        return StreamExpression(
            hl.range(hl.len(values)).map(
                lambda i: hl.struct(idx=values[i].idx, empty=values[i].empty)
            )
        )

    result = table._map_partitions(rebuild)
    expected = [hl.Struct(idx=i, empty=hl.Struct()) for i in range(n)]
    assert result.collect() == expected
    assert result.collect() == expected
    assert result.n_partitions() == table.n_partitions()


def test_map_partitions_rebuild_preserves_missing_empty_structs():
    from vail.streams import StreamExpression

    schema = hl.tstruct(idx=hl.tint32, empty=hl.tstruct())
    values = [hl.Struct(idx=0, empty=hl.Struct()), hl.Struct(idx=1, empty=None)]
    table = hl.Table.parallelize(values, schema=schema)

    def rebuild(rows):
        values = rows.to_array()
        return StreamExpression(
            hl.range(hl.len(values)).map(
                lambda i: hl.struct(idx=values[i].idx, empty=values[i].empty)
            )
        )

    assert table._map_partitions(rebuild).collect() == values
