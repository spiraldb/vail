"""Public oracles for native float abs and delimited missing-value rendering."""
import pytest
import vail as hl


@pytest.mark.parametrize('x,y,expected', [
    (-2., -2.0000001, True), (-2., 2., False), (-0., 0., True),
    (float('inf'), float('inf'), True),
    (float('-inf'), float('inf'), False),
    (float('nan'), float('nan'), True), (None, 1., None),
])
def test_approx_equal_native_abs(x, y, expected):
    assert hl.eval(hl.approx_equal(hl.literal(x, hl.tfloat64),
                                 hl.literal(y, hl.tfloat64), nan_same=True)) == expected


@pytest.mark.parametrize('value,dtype,expected', [
    (None, hl.tstr, 'MISSING'), ('null', hl.tstr, 'null'),
    (None, hl.tint32, 'MISSING'),
    ({}, hl.tstruct(), '{}'),
    (None, hl.tstruct(x=hl.tint32), 'MISSING'),
    ({'x': None}, hl.tstruct(x=hl.tint32), '{"x":null}'),
])
def test_export_native_null_replacement(tmp_path, value, dtype, expected):
    matrix = hl.range_matrix_table(1, 1)
    matrix = matrix.annotate_entries(x=hl.literal(value, dtype))
    path = tmp_path / 'values.tsv'
    matrix.x.export(str(path), missing='MISSING', header=False)
    assert path.read_text().splitlines() == ['0\t' + expected]
