import pytest
import vail as hl


def test_fold_sequence_and_combine_values():
    table = hl.range_table(9, n_partitions=3)
    expression = hl.agg.fold(0, lambda state: state + 1, lambda left, right: left + right)
    assert expression.dtype == hl.tint32
    assert table.aggregate(expression) == 9
    empty = hl.range_table(0, n_partitions=3)
    assert empty.aggregate(hl.agg.fold(7, lambda state: state + 1, lambda left, right: left + right)) == 7


def test_fold_rejects_row_initial_and_mismatched_result_types():
    table = hl.range_table(2)
    with pytest.raises(TypeError, match='cannot depend on rows'):
        hl.agg.fold(table.idx, lambda state: state, lambda left, right: left)
    with pytest.raises(TypeError, match='incompatible types'):
        hl.agg.fold(0, lambda state: 'wrong', lambda left, right: left)


def test_fold_requires_callable_operations():
    with pytest.raises(TypeError, match='must be callable'):
        hl.agg.fold(0, None, None)


def test_fold_promotes_numeric_state():
    table = hl.range_table(3)
    expression = hl.agg.fold(0, lambda state: state + hl.int64(table.idx), lambda left, right: left + right)
    assert expression.dtype == hl.tint64
    assert table.aggregate(expression) == 3


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_fold_nested_tagged_identity_state_survives_sequence_and_merge(partitions):
    table = hl.range_table(11, n_partitions=partitions)
    initial = hl.struct(s=hl.literal({1, 2}), d=hl.literal({'a': {3, 4}}), xs=[1, None, 3])
    expression = hl.agg.fold(initial, lambda state: state, lambda left, right: left)
    assert table.aggregate(expression) == hl.Struct(s={1, 2}, d={'a': {3, 4}}, xs=[1, None, 3])


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_fold_captures_nonnullable_range_columns(partitions):
    table = hl.range_table(17, n_partitions=partitions)
    expression = hl.agg.fold(hl.int64(0), lambda state: state + hl.int64(table.idx), lambda left, right: left + right)
    assert table.aggregate(expression) == 136


def test_scan_fold_nested_tagged_state_and_nonnullable_capture():
    table = hl.range_table(5, n_partitions=3)
    initial = hl.struct(s=hl.literal({1, 2}), xs=[1, 2, 3])
    result = table.annotate(state=hl.scan.fold(initial, lambda state: state, lambda left, right: left),
                            total=hl.scan.fold(hl.int64(0), lambda state: state + hl.int64(table.idx), lambda left, right: left + right))
    rows = result.collect()
    assert [r.state for r in rows] == [hl.Struct(s={1, 2}, xs=[1, 2, 3])] * 5
    assert [r.total for r in rows] == [0, 0, 1, 3, 6]
