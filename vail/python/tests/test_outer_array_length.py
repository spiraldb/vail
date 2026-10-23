"""Hail length output type and outer-list semantics through public expression paths."""
import vail as hl


def test_lengths_scalar_projection_lambda_and_nested_outer_dimension():
    assert hl.len(hl.array([1, 2])).dtype == hl.tint32
    assert hl.eval(hl.len(hl.array([1, None, 3]))) == 3
    assert hl.eval(hl.len(hl.empty_array(hl.tint32))) == 0
    assert hl.eval(hl.len(hl.missing(hl.tarray(hl.tint32)))) is None
    table = hl.Table.parallelize([
        {'xs': [[1, 2], [], None]}, {'xs': []}, {'xs': None},
    ], schema=hl.tstruct(xs=hl.tarray(hl.tarray(hl.tint32))))
    table = table.annotate(n=hl.len(table.xs), inner=table.xs.map(lambda x: hl.len(x)))
    expected = [hl.Struct(xs=[[1, 2], [], None], n=3, inner=[2, 0, None]),
                hl.Struct(xs=[], n=0, inner=[]), hl.Struct(xs=None, n=None, inner=None)]
    assert table.n.dtype == hl.tint32
    assert table.collect() == expected
    assert table.collect() == expected
