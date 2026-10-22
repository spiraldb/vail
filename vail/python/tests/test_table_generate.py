"""Behavioral coverage for context-driven partition generation."""
import pytest
import vail as hl


def test_generate_variable_length_partitions_and_shared_globals():
    calls = []

    def rows(context, globals):
        calls.append(True)
        return hl.range(context).map(lambda i: hl.struct(value=globals.offset + i, context=context))

    table = hl.Table._generate(hl.array([2, 0, 3]), 3, rows, hl.struct(offset=10))
    assert calls == [True]
    assert table.n_partitions() == 3
    assert list(table.key.dtype) == []
    assert table.row.dtype == hl.tstruct(value=hl.tint32, context=hl.tint32)
    assert hl.eval(table.index_globals()) == hl.Struct(offset=10)
    expected = [hl.Struct(value=10, context=2), hl.Struct(value=11, context=2),
                hl.Struct(value=10, context=3), hl.Struct(value=11, context=3),
                hl.Struct(value=12, context=3)]
    assert table.collect() == expected
    assert table._filter_partitions([0]).collect() == expected[:2]
    assert table._filter_partitions([1]).collect() == []
    assert table._filter_partitions([2]).collect() == expected[2:]
    assert table.collect() == expected
    assert calls == [True]


def test_generate_random_contexts_rows_and_globals_have_distinct_stable_identities():
    table = hl.Table._generate(
        contexts=hl.repeat(hl.rand_int64, 2),
        globals=hl.struct(k=hl.rand_int64()), partitions=2,
        rowfn=lambda c, g: hl.repeat(hl.struct(a=c * g.k * hl.rand_int64()), 2),
    )
    table = table.annotate(r=hl.rand_int64())
    rows = table.collect()
    assert len(rows) == 4
    assert len({row.a for row in rows}) == 4
    assert len({row.r for row in rows}) == 4
    assert table.collect() == rows
    assert len(table._filter_partitions([0]).collect()) == 2
    assert len(table._filter_partitions([1]).collect()) == 2


def test_generate_unindexed_expression_dependencies_remain_lazy(tmp_path):
    path = tmp_path / 'contexts.ht'
    hl.range_table(3).write(path)
    source = hl.read_table(path)
    contexts = source.idx.collect(_localize=False)
    table = hl.Table._generate(contexts, 3, lambda c, g: hl.array([hl.struct(x=c + g.offset)]),
                               hl.struct(offset=source.aggregate(hl.agg.sum(source.idx), _localize=False)))
    assert table.x.collect() == [3, 4, 5]
    invalid = hl.Table._generate(hl.array([hl.int32('bad-context')]), 1,
                                 lambda c, g: hl.array([hl.struct(x=c)]))
    with pytest.raises(RuntimeError, match='bad-context|parse|cast'):
        invalid.collect()


@pytest.mark.parametrize('contexts', [hl.array([1]), hl.array([1, 2, 3]),
                                     hl.missing(hl.tarray(hl.tint32))])
def test_generate_context_count_checked_at_action_time(contexts):
    table = hl.Table._generate(contexts, 2, lambda c, g: hl.array([hl.struct(x=c)]))
    for action in (table.collect, table.count, lambda: table.aggregate(hl.agg.count())):
        with pytest.raises(RuntimeError, match='TableGen:.*contexts'):
            action()


def test_generate_missing_context_empty_output_and_unkeyed_intervals():
    bounds = [hl.Interval(hl.Struct(), hl.Struct(), True, True) for _ in range(2)]
    table = hl.Table._generate(hl.array([None, 2]), bounds,
                               lambda c, g: hl.range(hl.or_else(c, 1)).map(lambda _: hl.struct()))
    assert table.row.dtype == hl.tstruct()
    assert table.collect() == [hl.Struct(), hl.Struct(), hl.Struct()]
    assert table.n_partitions() == 2
    empty = hl.Table._generate(hl.array([0, 0]), 2,
                               lambda c, g: hl.range(c).map(lambda _: hl.struct(x=1)))
    assert empty.collect() == []
    assert empty.n_partitions() == 2


@pytest.mark.parametrize('partitions', [0, -1, True, 1.5, [], ['invalid']])
def test_generate_rejects_invalid_partition_specifications(partitions):
    with pytest.raises((TypeError, ValueError)):
        hl.Table._generate(hl.array([1]), partitions, lambda c, g: hl.array([hl.struct(x=c)]))


def test_generate_validates_expression_scopes_and_callback_types():
    source = hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='unindexed'):
        hl.Table._generate(hl.array([source.idx]), 1, lambda c, g: hl.array([hl.struct(x=c)]))
    with pytest.raises(hl.ExpressionException, match='unindexed'):
        hl.Table._generate(hl.array([1]), 1, lambda c, g: hl.array([hl.struct(x=c)]), hl.struct(x=source.idx))
    with pytest.raises(TypeError, match='contexts.*array'):
        hl.Table._generate(1, 1, lambda c, g: hl.array([hl.struct(x=c)]))
    with pytest.raises(TypeError, match='globals.*struct'):
        hl.Table._generate(hl.array([1]), 1, lambda c, g: hl.array([hl.struct(x=c)]), 1)
    with pytest.raises(TypeError, match='callable'):
        hl.Table._generate(hl.array([1]), 1, None)
    with pytest.raises(TypeError, match='array of structs'):
        hl.Table._generate(hl.array([1]), 1, lambda c, g: hl.array([c]))
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.Table._generate(hl.array([1]), 1, lambda c, g: hl.array([hl.struct(x=source.idx)]))
    with pytest.raises(NotImplementedError, match='keyed interval'):
        hl.Table._generate(hl.array([1]), [hl.Interval(hl.Struct(k=0), hl.Struct(k=10))],
                           lambda c, g: hl.array([hl.struct(k=c)]))
