import pytest

import vail as hl
from vail.types import type_from_wire, type_to_wire


def test_interval_types_wire_and_parser_support_nested_points():
    point_type = hl.tstruct(left=hl.tint32, right=hl.tarray(hl.tstr))
    interval_type = hl.tinterval(point_type)
    assert type_from_wire(type_to_wire(interval_type)) == interval_type
    assert hl.dtype("interval<struct{left: int32, right: array<str>}>") == interval_type


def test_interval_values_roundtrip_and_validate_bounds():
    point_type = hl.tstruct(left=hl.tint64, right=hl.tarray(hl.tstr))
    start = {"left": 2**40, "right": ["a"]}
    end = {"left": 2**40 + 1, "right": ["b"]}
    interval = hl.Interval(start, end, False, True, point_type=point_type)
    assert interval.point_type == point_type
    decoded = hl.eval(hl.literal(interval, hl.tinterval(point_type)))
    assert decoded == interval
    with pytest.raises(TypeError):
        hl.Interval(1, 2, includes_start=1)
    with pytest.raises(ValueError):
        hl.Interval(None, 2)


def test_typed_interval_can_have_missing_endpoints():
    interval = hl.Interval(None, 3, point_type=hl.tint32)
    assert interval.start is None
    assert interval.end == 3
    assert hl.eval(hl.literal(interval, hl.tinterval(hl.tint32))) == interval


def test_interval_expression_fields_and_operations_are_typed():
    expr = hl.interval(hl.int32(1), hl.int32(8), False, True)
    assert expr.dtype == hl.tinterval(hl.tint32)
    assert expr.start.dtype == hl.tint32
    assert expr.includes_end.dtype == hl.tbool
    assert expr.contains(3).dtype == hl.tbool
    assert expr.overlaps(hl.interval(4, 10)).dtype == hl.tbool
    missing_start = hl.interval(None, 3)
    assert missing_start.dtype == hl.tinterval(hl.tint32)
    with pytest.raises(TypeError):
        hl.interval(hl.int32(1), hl.int64(2))
