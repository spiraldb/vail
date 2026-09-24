import pytest

import vail as hl


def test_parallelize_rejects_schema_and_partial_type_together():
    data = [{"a": 1, "b": "a"}, {"a": 2, "b": "c"}]
    schema = "array<struct{a: int, b: str}>"
    partial_type = {"a": hl.tint32}

    with pytest.raises(ValueError, match="define either schema or partial type"):
        hl.Table.parallelize(data, schema=schema, partial_type=partial_type)


def test_parallelize_partial_type_alone_remains_unsupported():
    with pytest.raises(NotImplementedError, match="partial_type and globals"):
        hl.Table.parallelize([{"a": 1}], partial_type={"a": hl.tint32})


def test_parallelize_malformed_schema_alone_keeps_schema_validation():
    with pytest.raises(TypeError, match="schema must be a vail.tstruct"):
        hl.Table.parallelize([{"a": 1}], schema="array<struct{a: int}>")


def test_parallelize_globals_remain_unsupported():
    with pytest.raises(NotImplementedError, match="partial_type and globals"):
        hl.Table.parallelize([{"a": 1}], schema=hl.tstruct(a=hl.tint32), globals={})


def test_parallelize_valid_primitive_schema_still_works():
    table = hl.Table.parallelize(
        [{"a": 1}, {"a": 2}],
        schema=hl.tstruct(a=hl.tint32),
    )

    assert [row.a for row in table.collect()] == [1, 2]
