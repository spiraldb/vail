import pytest

import vail as hl


def _table():
    return hl.Table.parallelize(
        [{"k": k, "x": x} for k, x in [(1, 0), (1, 3), (2, 2), (3, 1), (3, 4)]],
        schema=hl.tstruct(k=hl.tint32, x=hl.tint32), key="k", n_partitions=3,
    )


def test_map_partitions_maps_and_filters_rows_lazily():
    table = _table()

    def transform(rows):
        return rows.map(lambda row: row.annotate(y=row.x + 1)).filter(lambda row: row.x % 2 == 0)

    result = table.map_partitions(transform)
    assert result.collect() == [
        hl.Struct(k=1, x=0, y=1), hl.Struct(k=2, x=2, y=3), hl.Struct(k=3, x=4, y=5)
    ]
    assert result.key.dtype == table.key.dtype
    assert result.row.dtype == hl.tstruct(k=hl.tint32, x=hl.tint32, y=hl.tint32)


def test_map_partitions_flatmap_range_and_group_flatmap():
    table = _table()
    expanded = table.map_partitions(
        lambda rows: rows.flatmap(
            lambda row: hl.stream_range(row.x).map(lambda i: row.annotate(i=i))
        )
    )
    assert expanded.collect() == [
        hl.Struct(k=1, x=3, i=0), hl.Struct(k=1, x=3, i=1), hl.Struct(k=1, x=3, i=2),
        hl.Struct(k=2, x=2, i=0), hl.Struct(k=2, x=2, i=1),
        hl.Struct(k=3, x=1, i=0),
        hl.Struct(k=3, x=4, i=0), hl.Struct(k=3, x=4, i=1), hl.Struct(k=3, x=4, i=2),
        hl.Struct(k=3, x=4, i=3),
    ]

    regrouped = table.map_partitions(
        lambda rows: rows.grouped(2).flatmap(lambda group: hl.StreamExpression(group))
    )
    assert regrouped.collect() == table.collect()
    assert regrouped.key.dtype == hl.tstruct()


def test_map_partitions_handles_empty_partitions_and_preserves_globals():
    table = hl.Table.parallelize(
        [], schema=hl.tstruct(k=hl.tint32, x=hl.tint32), key="k", n_partitions=4,
        globals={"tag": "empty"},
    )
    result = table.map_partitions(lambda rows: rows.map(lambda row: row.annotate(y=1)))
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

    mapped = table.map_partitions(annotate_partition_size)
    assert mapped.n_partitions() == table.n_partitions() == 4
    rows = mapped.collect()
    expected_sizes = [9] * 9 + [9] * 9 + [9] * 9 + [10] * 10
    assert [row.partition_size for row in rows] == expected_sizes
    assert mapped.aggregate(hl.agg.sum(mapped.partition_size)) == sum(expected_sizes)
    assert mapped.n_partitions() == 4


def test_map_partitions_requires_stream_of_structs_and_preserved_keys():
    table = _table()
    with pytest.raises(TypeError, match="stream expression"):
        table.map_partitions(lambda rows: rows.to_array())
    dropped = table.map_partitions(lambda rows: rows.map(lambda row: row.drop("k")))
    assert dropped.key.dtype == hl.tstruct()
    assert sorted(row.x for row in dropped.collect()) == list(range(5))
    with pytest.raises(TypeError, match="stream of structs"):
        table.map_partitions(lambda rows: rows.map(lambda row: row.x))
    with pytest.raises(TypeError, match="callable"):
        table.map_partitions(None)


def test_struct_annotate_and_drop_rebuild_lazy_fields():
    table = hl.range_table(4)
    result = table.map_partitions(
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

    result = table.map_partitions(rebuild)
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

    assert table.map_partitions(rebuild).collect() == values


def test_map_partitions_reestablishes_changed_and_reversed_keys():
    table = hl.range_table(9, n_partitions=3)
    changed = table.map_partitions(lambda rows: rows.map(lambda row: row.annotate(idx=8-row.idx)))
    assert changed.key.dtype == hl.tstruct()
    assert changed.collect() == [hl.Struct(idx=i) for i in reversed(range(9))]
    assert changed.key_by('idx').collect() == table.collect()
    reversed_rows = table.map_partitions(lambda rows: hl.StreamExpression(rows.to_array()[::-1]))
    assert reversed_rows.key.dtype == hl.tstruct()
    assert sorted(row.idx for row in reversed_rows.collect()) == list(range(9))
    assert reversed_rows.key_by('idx').collect() == table.collect()


def test_map_partitions_lazy_errors_and_streaming_head(tmp_path):
    table = hl.Table.parallelize([{'s': 'invalid'}], schema=hl.tstruct(s=hl.tstr))
    result = table.map_partitions(lambda rows: rows.map(lambda row: hl.struct(n=hl.int32(row.s))))
    assert result.row.dtype == hl.tstruct(n=hl.tint32)
    with pytest.raises(Exception, match='invalid|parse|cast'):
        result.collect()
    path = tmp_path / 'rows.txt'
    path.write_bytes(b'ok\n' * 131072 + b'\xff\n')
    source = hl.import_lines(str(path))
    mapped = source.map_partitions(lambda rows: rows.map(lambda row: row.annotate(n=hl.len(row.text))))
    assert mapped.head(1).collect()[0].n == 2


def test_stream_aggregate_scan_filter_scope_and_exclusive_results():
    stream = hl.stream_range(1, 5)
    result = stream.aggregate_scan(lambda x: hl.struct(
        current=x, total=hl.scan.sum(x), count=hl.scan.count(),
        filtered=hl.scan.filter(x > 1, hl.scan.sum(x)),
        nested=hl.scan.filter(x > 1, hl.scan.filter(x < 4, hl.scan.sum(x))),
        ignored=hl.scan.filter(hl.missing(hl.tbool), hl.scan.sum(x)),
    ))
    assert hl.eval(result.to_array()) == [
        hl.Struct(current=1, total=0, count=0, filtered=0, nested=0, ignored=0),
        hl.Struct(current=2, total=1, count=1, filtered=0, nested=0, ignored=0),
        hl.Struct(current=3, total=3, count=2, filtered=2, nested=2, ignored=0),
        hl.Struct(current=4, total=6, count=3, filtered=5, nested=5, ignored=0),
    ]
    assert hl.eval(hl.stream_range(0).aggregate_scan(lambda x: hl.scan.sum(x)).to_array()) == []


def test_stream_aggregate_scan_filtered_errors_are_lazy():
    values = hl.StreamExpression(hl.array(['bad', '2', '3']))
    result = values.aggregate_scan(lambda x: hl.scan.filter(x != 'bad', hl.scan.sum(hl.int32(x))))
    assert hl.eval(result.to_array()) == [0, 0, 2]


@pytest.mark.parametrize('function,expected', [
    ('sum', [0, 2, 2, 5]), ('product', [1, 2, 2, 6]),
    ('min', [None, 2, 2, 2]), ('max', [None, 2, 2, 3]),
    ('collect', [[], [2], [2, None], [2, None, 3]]),
    ('collect_as_set', [set(), {2}, {2, None}, {2, None, 3}]),
])
def test_stream_aggregate_scan_reducers_and_missingness(function, expected):
    stream = hl.StreamExpression(hl.literal([2, None, 3, 1], hl.tarray(hl.tint32)))
    assert hl.eval(stream.aggregate_scan(lambda x: getattr(hl.scan, function)(x)).to_array()) == expected


@pytest.mark.parametrize('function', ['all', 'any'])
def test_stream_aggregate_scan_boolean_missingness_matches_table_aggregation(function):
    # Hail aggregators.py defines all/any via count_where: missing records are
    # ignored. Scalar boolean operators have different missingness semantics.
    values = [True, None, False, True]
    stream = hl.StreamExpression(hl.literal(values, hl.tarray(hl.tbool)))
    actual = hl.eval(stream.aggregate_scan(lambda x: getattr(hl.scan, function)(x)).to_array())
    expected = []
    for end in range(len(values)):
        prefix = hl.Table.parallelize([{'value': value} for value in values[:end]],
                                      schema=hl.tstruct(value=hl.tbool))
        expected.append(prefix.aggregate(getattr(hl.agg, function)(prefix.value)))
    assert actual == expected
    assert expected == ([True, True, True, False] if function == 'all'
                        else [False, True, True, True])


def test_stream_aggregate_scan_nested_randomness_matches_input_and_output_positions():
    def random_value():
        return hl.rand_int32(hl.rand_int32(1, 100, seed=21), seed=12)

    values = hl.eval(hl.range(12).map(lambda _: random_value()))
    result = hl.stream_range(12).aggregate_scan(lambda _: hl.struct(
        current=random_value(), total=hl.scan.sum(random_value()),
        filtered=hl.scan.filter(random_value() % 2 == 0, hl.scan.sum(random_value())),
    ))
    expected = [hl.Struct(current=value, total=sum(values[:i]),
                          filtered=sum(v for v in values[:i] if v % 2 == 0))
                for i, value in enumerate(values)]
    assert hl.eval(result.to_array()) == expected
    assert hl.eval(result.to_array()) == expected


def test_stream_aggregate_scan_partition_rows_and_globals():
    table = hl.range_table(5, n_partitions=1).annotate_globals(offset=7)
    result = table.map_partitions(lambda rows: rows.aggregate_scan(lambda row: hl.struct(
        idx=row.idx, prior=hl.scan.sum(row.idx) + table.offset)))
    assert result.collect() == [hl.Struct(idx=i, prior=i*(i-1)//2+7) for i in range(5)]
    assert hl.eval(hl.StreamExpression(hl.missing(hl.tarray(hl.tint32))).aggregate_scan(
        lambda x: hl.scan.sum(x)).to_array()) is None


def test_partition_head_crosses_empty_partitions_and_skips_unneeded_errors():
    table = hl.range_table(12, n_partitions=4).map_partitions(
        lambda rows: rows.filter(lambda row: row.idx >= 3))
    assert table.head(5).collect() == [hl.Struct(idx=i) for i in range(3, 8)]
    assert table.head(0).collect() == []
    bad = hl.Table.parallelize([{'s':'bad'}], schema=hl.tstruct(s=hl.tstr)).map_partitions(
        lambda rows: rows.map(lambda row: hl.struct(x=hl.int32(row.s))))
    assert bad.head(0).collect() == []


def test_exclusive_stream_scan_does_not_evaluate_last_reducer_input():
    values = hl.StreamExpression(hl.array(['1', 'bad']))
    assert hl.eval(values.aggregate_scan(lambda x: hl.scan.sum(hl.int32(x))).to_array()) == [0, 1]
    single = hl.StreamExpression(hl.array(['bad']))
    assert hl.eval(single.aggregate_scan(lambda x: hl.scan.sum(hl.int32(x))).to_array()) == [0]
