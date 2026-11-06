"""Lazy coalesce preserves numeric values and the selected row domain."""
import math
import pytest
import vail as hl


@pytest.fixture(params=[False, True])
def mode(request, monkeypatch):
    if request.param:
        monkeypatch.setenv('VAIL_EXPERIMENT_COALESCE_ONCE', '1')
    else:
        monkeypatch.delenv('VAIL_EXPERIMENT_COALESCE_ONCE', raising=False)


def test_all_valid_missing_and_mixed_frames(mode):
    for values in [[1, 2, 3], [None, None, None], [1, None, 3], []]:
        table = hl.Table.parallelize([{'id': i, 'x': x} for i, x in enumerate(values)],
                                    schema=hl.tstruct(id=hl.tint32, x=hl.tint32))
        query = table.select(value=hl.coalesce(table.x, table.id + 10),
                              scalar=hl.coalesce(hl.missing(hl.tint32), 99),
                              null=hl.coalesce(table.x, hl.missing(hl.tint32)))
        assert [(r.value, r.scalar, r.null) for r in query.collect()] == [
            (x if x is not None else i + 10, 99, x) for i, x in enumerate(values)]
        assert query.to_arrow().equals(query.to_arrow(), check_metadata=True)


def test_fallback_error_is_lazy_and_selected(mode):
    table = hl.Table.parallelize([{'x': 8}, {'x': 9}], schema=hl.tstruct(x=hl.tint32))
    assert table.select(value=hl.coalesce(table.x, 1 // hl.int32(0))).value.collect() == [8, 9]
    missing = hl.Table.parallelize([{'x': 8}, {'x': None}], schema=hl.tstruct(x=hl.tint32))
    with pytest.raises(Exception, match='(?i)zero'):
        missing.select(value=hl.coalesce(missing.x, 1 // hl.int32(0))).collect()


def test_nan_signed_zero_and_common_type(mode):
    table = hl.Table.parallelize([{'x': -0.0}, {'x': 0.0}, {'x': float('nan')}, {'x': None}],
                                schema=hl.tstruct(x=hl.tfloat64))
    values = table.select(value=hl.coalesce(table.x, 17.0)).value.collect()
    assert math.copysign(1, values[0]) == -1 and math.copysign(1, values[1]) == 1
    assert math.isnan(values[2]) and values[3] == 17.0
    assert hl.eval(hl.coalesce(hl.missing(hl.tint32), hl.int64(2**40))) == 2**40
    assert hl.eval(hl.coalesce(hl.int32(3), hl.float64(4.5))) == 3.0


def test_parent_nulls_and_reordered_callbacks(mode):
    table = hl.Table.parallelize([{'id': 2, 's': {'x': 4}, 'xs': [None, 8]},
                                 {'id': 0, 's': None, 'xs': [3, None]},
                                 {'id': 1, 's': {'x': None}, 'xs': []}],
        schema=hl.tstruct(id=hl.tint32, s=hl.tstruct(x=hl.tint32), xs=hl.tarray(hl.tint32)))
    table = table.order_by(table.id)
    query = table.select(value=hl.coalesce(table.s.x, table.id + 10),
                          mapped=table.xs.map(lambda x: hl.coalesce(x, table.id + 20)))
    assert [(r.value, r.mapped) for r in query.collect()] == [(10, [3, 20]), (11, []), (4, [22, 8])]


def test_nested_metadata_uses_the_ordinary_contract(mode):
    schema = hl.tstruct(value=hl.tcall)
    table = hl.Table.parallelize([{'value': None}, {'value': hl.Call((1, 0), True)}], schema=schema)
    result = table.select(value=hl.coalesce(table.value, hl.call(0, 0)))
    expected = hl.Table.parallelize([{'value': hl.Call((0, 0), False)},
                                     {'value': hl.Call((1, 0), True)}], schema=schema)
    assert result.to_arrow().equals(expected.to_arrow(), check_metadata=True)


def test_seeded_fallback_and_globals_keep_nested_context(mode):
    table = hl.Table.parallelize([{'xs': [1.0, None, 2.0, None]}, {'xs': [None, 3.0]}],
                                schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)))
    table = table.annotate_globals(offset=hl.float64(10))
    random = hl.rand_unif(seed=517)
    query = table.select(values=table.xs.map(lambda x: hl.struct(
        input=x, value=hl.coalesce(x, random + table.offset), reference=random)))
    rows = query.collect()
    for row in rows:
        for value in row['values']:
            assert 0 <= value.reference < 1
            assert value.value == (value.input if value.input is not None else value.reference + 10)
    assert query.to_arrow().equals(query.to_arrow(), check_metadata=True)
