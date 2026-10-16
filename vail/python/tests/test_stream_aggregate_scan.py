"""Stateful local scans: reducer semantics and exclusive evaluation scopes."""
import math
import pytest
import vail as hl


def scan(values, f, dtype=None):
    from vail.streams import StreamExpression
    array = hl.array(values) if dtype is None else hl.literal(values, hl.tarray(dtype))
    return StreamExpression(array).aggregate_scan(f).to_array()


def test_composed_filtered_reducers_preserve_positions_and_missing_values():
    expression = scan([None, 2, -1, 5], lambda x: hl.struct(
        count=hl.scan.count(),
        selected=hl.scan.filter(x > 0, hl.scan.count()),
        selected_sum=hl.scan.filter(x > 0, hl.scan.sum(x)),
        nested=hl.struct(product=hl.scan.product(x), minimum=hl.scan.min(x)),
        collected=hl.scan.collect(x),
    ), hl.tint32)
    rows = hl.eval(expression)
    assert [row.count for row in rows] == [0, 1, 2, 3]
    assert [row.selected for row in rows] == [0, 0, 1, 1]
    assert [row.selected_sum for row in rows] == [0, 0, 2, 2]
    assert [row.nested.product for row in rows] == [1, 1, 2, -2]
    assert [row.nested.minimum for row in rows] == [None, None, 2, -1]
    assert [row.collected for row in rows] == [[], [None], [None, 2], [None, 2, -1]]


def test_nested_filters_and_scalar_output_use_current_element():
    result = scan([1, 2, 3, 4, 5], lambda x:
                  x + hl.scan.filter(x > 1, hl.scan.filter(x % 2 == 0, hl.scan.sum(x))))
    assert hl.eval(result) == [1, 2, 5, 6, 11]


def test_mean_nan_empty_and_missing_arrays():
    result = hl.eval(scan([None, 2, 4], lambda x: hl.scan.mean(x), hl.tint32))
    assert math.isnan(result[0]) and math.isnan(result[1])
    assert result[2] == 2.0
    assert hl.eval(scan([], lambda x: hl.scan.sum(x), hl.tint32)) == []
    assert hl.eval(scan(None, lambda x: hl.scan.sum(x), hl.tint32)) is None


def test_array_valued_callback_keeps_independent_reducer_states():
    result = scan([2, 3, 7], lambda x: hl.array([hl.scan.sum(x), hl.scan.count()]))
    assert hl.eval(result) == [[0, 0], [2, 1], [5, 2]]


def test_ordinary_aggregation_is_rejected_in_local_scan():
    with pytest.raises(hl.ExpressionException, match="local stream scan cannot aggregate"):
        scan([1, 2], lambda x: hl.agg.sum(x))


def test_long_scan_exclusive_prefix_values():
    expression = hl.stream_range(20000).aggregate_scan(lambda x: hl.scan.sum(x)).to_array()
    result = hl.eval(expression)
    assert len(result) == 20000
    assert result[-1] == 19999 * 19998 // 2


def test_random_output_and_reducer_inputs_have_separate_position_contexts():
    expected_inputs = hl.eval(hl.range(10).map(lambda _: hl.rand_int32(100, seed=71)))
    expected_output = hl.eval(hl.range(10).map(lambda _: hl.rand_int64(seed=72)))
    expression = hl.stream_range(10).aggregate_scan(lambda _: hl.struct(
        state=hl.scan.sum(hl.rand_int32(100, seed=71)),
        output=hl.rand_int64(seed=72),
    )).to_array()
    rows = hl.eval(expression)
    assert [row.state for row in rows] == [sum(expected_inputs[:i]) for i in range(10)]
    assert [row.output for row in rows] == expected_output
    assert hl.eval(expression) == rows


def test_scan_over_table_owned_arrays_and_global_references():
    from vail.streams import StreamExpression
    table = hl.range_table(3).annotate_globals(offset=5)
    table = table.annotate(values=hl.range(table.idx + 1))
    table = table.annotate(prefix=StreamExpression(table.values).aggregate_scan(
        lambda x: hl.scan.sum(x + table.index_globals().offset)).to_array())
    assert [row.prefix for row in table.collect()] == [[0], [0, 5], [0, 5, 11]]


def test_scan_reducer_can_capture_outer_typed_bindings():
    expression = hl.bind(lambda offset: hl.stream_range(4).aggregate_scan(
        lambda x: hl.scan.sum(x + offset)).to_array(), 4)
    assert hl.eval(expression) == [0, 4, 9, 15]
    expression = hl.bind(lambda offset: hl.stream_range(4).aggregate_scan(
        lambda x: hl.scan.filter(x < offset, hl.scan.sum(x + offset))).to_array(), 2)
    assert hl.eval(expression) == [0, 2, 5, 5]


def test_scan_requires_a_callable():
    with pytest.raises(TypeError, match="requires a callable"):
        hl.stream_range(3).aggregate_scan(3)


def test_nested_typed_callbacks_preserve_element_types_and_values():
    expression = hl.bind(
        lambda offset: hl.stream_range(3).aggregate_scan(
            lambda item: hl.struct(
                current=hl.float64(item),
                prefix=hl.scan.sum(hl.float64(item) + offset),
                nested=hl.array([item]).map(lambda inner: hl.float64(inner) + offset),
            )
        ).to_array(),
        0.5,
    )
    assert expression.dtype == hl.tarray(hl.tstruct(
        current=hl.tfloat64, prefix=hl.tfloat64, nested=hl.tarray(hl.tfloat64)))
    rows = hl.eval(expression)
    assert [row.current for row in rows] == [0.0, 1.0, 2.0]
    assert [row.prefix for row in rows] == [0.0, 0.5, 2.0]
    assert [row.nested for row in rows] == [[0.5], [1.5], [2.5]]


def test_scan_inside_matrix_rows_can_capture_matrix_fields():
    from vail.streams import StreamExpression
    matrix = hl.utils.range_matrix_table(3, 2)
    matrix = matrix.annotate_rows(prefix=StreamExpression(hl.range(matrix.row_idx + 1)).aggregate_scan(
        lambda x: hl.scan.sum(x + matrix.row_idx)).to_array())
    assert [row.prefix for row in matrix.rows().collect()] == [[0], [0, 1], [0, 2, 5]]


def test_filtered_scan_does_not_evaluate_excluded_argument_errors():
    failure = hl.int32("excluded argument")
    expression = scan([0, 2, 0, 3], lambda x: hl.scan.filter(
        x != 0, hl.scan.sum(hl.if_else(x == 0, failure, x))))
    assert hl.eval(expression) == [0, 0, 2, 2]


def test_scan_reducers_can_capture_table_lookup_fields():
    from vail.streams import StreamExpression
    target = hl.range_table(3).annotate(offset=hl.int32(7)).key_by('idx')
    table = hl.range_table(3).annotate(values=hl.range(3))
    offset = target[table.idx].offset
    table = table.annotate(prefix=StreamExpression(table.values).aggregate_scan(
        lambda x: hl.scan.sum(x + offset)).to_array())
    assert [row.prefix for row in table.collect()] == [[0, 7, 15]] * 3


def test_matrix_field_cast_retains_declared_type_and_values():
    matrix = hl.utils.range_matrix_table(2, 1)
    matrix = matrix.annotate_rows(value=hl.float64(matrix.row_idx))
    assert matrix.value.dtype == hl.tfloat64
    assert [row.value for row in matrix.rows().collect()] == [0.0, 1.0]


def test_lookup_field_cast_retains_declared_type_and_values():
    target = hl.range_table(2).annotate(value=hl.int32(7)).key_by('idx')
    source = hl.range_table(2)
    source = source.annotate(value=hl.float64(target[source.idx].value))
    assert source.value.dtype == hl.tfloat64
    assert [row.value for row in source.collect()] == [7.0, 7.0]


def test_nested_random_parameters_receive_output_context_once():
    def draw(_):
        return hl.rand_norm(mean=hl.rand_norm(seed=111), seed=112)
    expected = hl.eval(hl.range(8).map(draw))
    expression = hl.stream_range(8).aggregate_scan(draw).to_array()
    assert hl.eval(expression) == expected


def test_missing_outer_filter_excludes_erroring_inner_predicate():
    def inner(x):
        return hl.int32(x) > 0
    expression = scan(["invalid", "2", "3"], lambda x: hl.scan.filter(
        hl.if_else(x == "invalid", hl.missing(hl.tbool), True),
        hl.scan.filter(inner(x), hl.scan.count())))
    assert hl.eval(expression) == [0, 0, 1]
    selected = scan(["invalid", "2"], lambda x: hl.scan.filter(
        True, hl.scan.filter(inner(x), hl.scan.count())))
    with pytest.raises((ValueError, RuntimeError), match="[Cc]ast|[Pp]ars|invalid"):
        hl.eval(selected)
