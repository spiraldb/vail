import pytest
import vail as hl


def test_show_handler_types_and_prefix():
    table = hl.range_table(3)
    outputs = []
    assert table.show(n=1, width=80, handler=outputs.append) is None
    assert outputs == ['+-------+\n|   idx |\n+-------+\n| int32 |\n+-------+\n|     0 |\n+-------+\nshowing top 1 row\n']
    outputs.clear()
    table.show(n_rows=0, width=80, types=False, handler=outputs.append)
    assert 'showing top 0 rows' in outputs[0]
    assert 'int32' not in outputs[0]


def test_show_truthful_nested_missing_and_special_float_values():
    table = hl.Table.parallelize([
        {'s': 'a\nb', 'x': None, 'a': [None, 1], 'nested': {'f': float('inf')}},
    ], schema=hl.tstruct(s=hl.tstr, x=hl.tint32, a=hl.tarray(hl.tint32), nested=hl.tstruct(f=hl.tfloat64)))
    outputs = []
    table.show(n=-1, width=120, handler=outputs.append)
    text = outputs[0]
    assert '"a\\nb"' in text
    assert 'NA' in text and '[NA,1]' in text
    assert 'nested.f' in text and 'Infinity' in text
    assert 'showing top' not in text


def test_show_empty_truncation_wrapping_and_default_print(capsys):
    hl.range_table(0).show()
    assert 'idx' in capsys.readouterr().out
    table = hl.range_table(1).annotate(long_name='abcdefghijk', other='another')
    outputs = []
    table.show(width=12, truncate=5, handler=outputs.append)
    assert '"a...' in outputs[0]
    assert '\n\n' in outputs[0]


def test_show_rejects_invalid_arguments():
    table = hl.range_table(1)
    with pytest.raises(ValueError):
        table.show(n=1, n_rows=1)
    with pytest.raises(TypeError):
        table.show(n=True)
    with pytest.raises(TypeError):
        table.show(handler=123)
