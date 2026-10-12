"""Collection returns reusable lazy scalar array expressions before localization."""
import pytest
import vail as hl


def test_table_and_expression_collection_preserve_key_order_and_missing_values():
    t = hl.Table.parallelize(
        [{'k': 2, 'v': None}, {'k': 1, 'v': 8}, {'k': 1, 'v': 9}, {'k': None, 'v': 10}],
        schema=hl.tstruct(k=hl.tint32, v=hl.tint32), key='k', n_partitions=3)
    for n in (0, 1, 3, 20):
        assert hl.eval(t.take(n, _localize=False)) == t.take(n)
        assert hl.eval(t.v.take(n, _localize=False)) == t.v.collect()[:n]
    result = t.collect(_localize=False)
    assert result.source is None
    assert result.dtype == hl.tarray(t.row.dtype)
    assert hl.eval(result) == t.collect()
    assert hl.eval((t.v + 2).collect(_localize=False)) == [10, 11, None, 12]
    assert hl.eval(t.row.collect(_localize=False)) == t.collect()
    assert hl.eval(t.key.collect(_localize=False)) == [hl.Struct(k=x) for x in [1, 1, 2, None]]


def test_globals_literals_and_empty_row_structs_have_distinct_collection_scopes():
    t = hl.range_table(4).annotate_globals(g=7).select()
    for expr, value in [(t.g + 1, 8), (hl.literal(11), 11), (t.globals, hl.Struct(g=7))]:
        assert expr.collect() == [value]
        assert expr.collect(_localize=False).source is None
        assert hl.eval(expr.take(0, _localize=False)) == []
        assert expr.take(20) == [value]
    assert t.row.collect() == [hl.Struct(idx=i) for i in range(4)]
    empty_rows = t.key_by().select()
    for expr in (empty_rows.row, empty_rows.key, empty_rows.row_value,
                 empty_rows.row.annotate(), empty_rows.row.annotate(x=1).drop('x')):
        assert expr.collect() == [hl.Struct()] * 4
        assert expr.take(2) == [hl.Struct()] * 2
    assert empty_rows.row.annotate(x=1).collect() == [hl.Struct(x=1)] * 4
    assert t.row.drop('idx').collect() == [hl.Struct()] * 4
    empty = empty_rows.head(0)
    assert hl.eval(empty.collect(_localize=False)) == []
    assert empty.row.collect() == []
    assert empty.g.collect() == [7]
    assert hl.literal(None, hl.tint32).collect() == [None]


def test_lazy_collection_can_be_mapped_and_broadcast_into_another_table():
    source = hl.range_table(5, n_partitions=3)
    values = (source.idx * 2).collect(_localize=False)
    target = hl.range_table(3)
    annotated = target.annotate(x=values[target.idx], n=hl.len(values))
    assert [(r.x, r.n) for r in annotated.collect()] == [(0, 5), (2, 5), (4, 5)]
    assert hl.eval(values.map(lambda x: x + 1)) == [1, 3, 5, 7, 9]
    aggregate = source.aggregate(hl.agg.sum(source.idx), _localize=False)
    assert aggregate.collect() == [10]


def test_constructing_collection_ir_does_not_open_source_data(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(6, n_partitions=2).write(path)
    source = hl.read_table(path)
    for file in path.rglob('*.vortex'):
        file.unlink()
    # These constructs may inspect types/metadata, but cannot evaluate source rows.
    expressions = [source.collect(_localize=False), source.take(2, _localize=False),
                   source.idx.collect(_localize=False), source.idx.take(2, _localize=False),
                   source.aggregate(hl.agg.count(), _localize=False)]
    for expression in expressions:
        assert isinstance(expression, hl.Expression)
        with pytest.raises(Exception):
            hl.eval(expression)
    assert hl.eval(source.idx.take(0, _localize=False)) == []


def test_invalid_localize_counts_and_unexecuted_aggregations_are_rejected():
    t = hl.range_table(2)
    for expr in (t.idx, hl.literal(3)):
        with pytest.raises(TypeError, match='_localize'):
            expr.collect(_localize=0)
        with pytest.raises(TypeError, match='integer'):
            expr.take(True)
        with pytest.raises(ValueError, match='non-negative'):
            expr.take(-1)
    with pytest.raises(TypeError, match='_localize'):
        t.collect(_localize=0)
    with pytest.raises(NotImplementedError, match='aggregated expression'):
        hl.agg.sum(t.idx).collect(_localize=False)
