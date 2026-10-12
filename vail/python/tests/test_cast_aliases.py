import pytest
import vail as hl


@pytest.mark.parametrize('alias,canonical,value,dtype,expected', [
    (hl.int, hl.int32, '42', hl.tint32, 42),
    (hl.int, hl.int32, True, hl.tint32, 1),
    (hl.float, hl.float64, '2.5', hl.tfloat64, 2.5),
    (hl.bool, hl._bool_cast, 0, hl.tbool, False),
    (hl.bool, hl._bool_cast, 2, hl.tbool, True),
    (hl.str, hl.str, 42, hl.tstr, '42'),
])
def test_public_cast_aliases(alias, canonical, value, dtype, expected):
    assert alias is canonical
    expression = alias(value)
    assert expression.dtype == dtype
    assert hl.eval(expression) == expected
    assert hl.eval(alias(hl.missing(hl.tint32))) is None


def test_cast_aliases_on_rows_and_partition_validation():
    t = hl.Table.parallelize([{'x': '3'}, {'x': None}], schema=hl.tstruct(x=hl.tstr), n_partitions=2)
    assert t.select(i=hl.int(t.x), f=hl.float(t.x), b=hl.bool(hl.int(t.x))).collect() == [
        hl.Struct(i=3, f=3., b=True), hl.Struct(i=None, f=None, b=None)]
    with pytest.raises(ValueError, match='non-negative'):
        t.take(-1)
    with pytest.raises(ValueError, match='positive'):
        hl.range_table(1, n_partitions=0)
