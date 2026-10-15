from vail.expressions import _native_expression, _native_projection
"""Execution boundaries and relational plans for whole-table aggregation."""
import json

import pytest
import vail as hl
from vail.expressions import _global_bindings


def plan(table, expression):
    return table._native.aggregate_physical_plan(
        expression.to_ir(), _global_bindings([expression]))


def test_common_reducers_share_one_datafusion_aggregate():
    n = 200_003
    t = hl.range_table(n, n_partitions=7)
    query = hl.struct(n=hl.agg.count(), total=hl.agg.sum(t.idx),
                      smallest=hl.agg.min(t.idx), largest=hl.agg.max(t.idx))
    physical = plan(t, query)
    assert physical.count('AggregateExec') == 2
    assert physical.count('mode=Partial') == 1
    assert physical.count('mode=Final') == 1
    assert 'RangeExec' in physical and 'ProjectionExec' in physical
    assert t.aggregate(query) == hl.Struct(n=n, total=n*(n-1)//2, smallest=0, largest=n-1)


def test_collect_uses_ordered_stream_across_files(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(23, n_partitions=4).write(path)
    assert len(list(path.rglob('*.vortex'))) >= 2
    t = hl.read_table(path)
    assert t.aggregate(hl.agg.collect(t.idx)) == list(range(23))
    reverse = t.order_by(hl.desc(t.idx))
    assert reverse.aggregate(hl.agg.collect(reverse.idx)) == list(reversed(range(23)))
    assert 'SortExec' not in plan(t, hl.agg.sum(t.idx))


def test_nonlocalized_aggregate_is_lazy_reusable_and_reads_on_each_action(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(6, n_partitions=2).write(path)
    source = hl.read_table(path)
    file = next(path.rglob('*.vortex'))
    contents = file.read_bytes()
    file.unlink()
    total = source.aggregate(hl.agg.sum(source.idx), _localize=False)
    assert total.dtype == hl.tint64
    target = hl.range_table(2)
    annotated = target.annotate(total=total + target.idx)
    assert annotated.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        hl.eval(total)
    file.write_bytes(contents)
    assert annotated.total.collect() == [15, 16]
    file.unlink()
    with pytest.raises(FileNotFoundError):
        hl.eval(total)


def test_filter_and_explode_compile_to_relational_operators():
    t = hl.Table.parallelize(
        [{'xs': [1, None, 2], 'keep': True}, {'xs': None, 'keep': True},
         {'xs': [], 'keep': True}, {'xs': [100], 'keep': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), keep=hl.tbool), n_partitions=3)
    query = hl.agg.filter(t.keep, hl.agg.explode(
        lambda x: hl.struct(n=hl.agg.count(), total=hl.agg.sum(x), items=hl.agg.collect(x)), t.xs))
    physical = plan(t, query)
    assert 'FilterExec' in physical and 'UnnestExec' in physical
    assert physical.count('AggregateExec') == 1
    assert t.aggregate(query) == hl.Struct(n=3, total=3, items=[1, None, 2])


def test_nested_explode_can_capture_outer_element_and_row():
    t = hl.range_table(2)
    query = hl.agg.explode(lambda x: hl.agg.explode(
        lambda y: hl.agg.collect(x + y + t.idx), [10, 20]), [1, 2])
    assert t.aggregate(query) == [11, 21, 12, 22, 12, 22, 13, 23]


def test_filtered_out_rows_do_not_evaluate_aggregate_body():
    t = hl.range_table(3)
    query = hl.agg.filter(False, hl.agg.sum(hl.array([1])[t.idx + 10]))
    assert t.aggregate(query) == 0


def test_collect_nested_values_and_sets_preserves_arrow_types():
    t = hl.range_table(3).annotate_globals(offset=7)
    result = t.aggregate(hl.agg.collect(hl.struct(i=t.idx, xs=[t.idx, None],
                                                  s=hl.literal({1, 2}), g=t.offset)))
    assert result == [hl.Struct(i=i, xs=[i, None], s={1, 2}, g=7) for i in range(3)]


def test_native_scope_validation_rejects_forged_aggregate_ir():
    t = hl.range_table(2)
    invalid = [
        {'op': 'column', 'name': 'idx'},
        {'op': 'aggregate', 'function': 'sum', 'value': {'op': 'aggregate', 'function': 'count', 'value': None}},
        {'op': 'aggregate', 'function': 'count', 'value': hl.literal(1).to_ir()},
        {'op': 'aggregate', 'function': 'unknown', 'value': hl.literal(1).to_ir()},
        {'op': 'agg_filter', 'predicate': hl.literal(1).to_ir(), 'body': hl.agg.count().to_ir()},
        {'op': 'agg_explode', 'parameter': 'p', 'value': hl.literal(1).to_ir(), 'body': hl.agg.count().to_ir()},
    ]
    for expression in invalid:
        with pytest.raises(ValueError):
            t._native.aggregate_globals(_native_expression(expression))
