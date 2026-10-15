import pytest

from vail.agg_collections import array_sum, collect_as_set, counter, take
from vail.types import tarray, tint32, tint64, tstr


def test_collect_as_set_keeps_element_type_and_source():
    from vail.expressions import literal
    value = literal(1, tint32)
    result = collect_as_set(value)
    assert result.dtype.element_type == tint32
    assert result.to_ir()["function"] == "collect_as_set"


def test_take_validates_n_and_carries_ordering():
    from vail.expressions import literal
    value = literal(1, tint32)
    ordering = literal(2, tint64)
    result = take(value, 3, ordering)
    assert result.dtype == tarray(tint32)
    options = result.to_ir()["options"]
    assert options["n"] == 3
    assert options["ordered"] is True
    assert options["__vail_result_type"] == result.dtype._native_type
    assert result.to_ir()["value"]["fields"][1][1] == ordering.to_ir()
    with pytest.raises(ValueError):
        take(value, -1)


def test_array_sum_uses_coordinate_array_aggregation():
    from vail.expressions import literal
    value = literal([1, 2], tarray(tint32))
    result = array_sum(value)
    assert result.dtype == tarray(tint64)
    assert result.to_ir()["op"] == "agg_array"
    invalid = literal("x", tstr)
    with pytest.raises(TypeError):
        array_sum(invalid)
