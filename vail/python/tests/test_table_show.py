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


def _show(table, **kwargs):
    output = []
    table.show(handler=output.append, **kwargs)
    return output[0]


@pytest.mark.parametrize('text', ['a\\b"c\n\r\tα🙂', '🙂' * 30, '\\' * 30, 'α' * 30])
@pytest.mark.parametrize('truncate', [4, 5, 8, 16, 64])
def test_show_string_truncation_matches_visible_escaped_prefix(text, truncate):
    escaped = '"' + text.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t') + '"'
    expected = escaped[:truncate-3] + '...' if len(escaped) > truncate else escaped
    table = hl.Table.parallelize([{'s': text}])
    output = _show(table, n=1, width=100, truncate=truncate, types=False)
    assert f'| {expected}' in output


def test_show_nested_typed_cells_and_parent_missingness():
    schema = hl.tstruct(
        a=hl.tarray(hl.tstruct(x=hl.tint32, s=hl.tstr)),
        t=hl.ttuple(hl.tint32, hl.tstr), d=hl.tdict(hl.tstr, hl.tint32),
        s=hl.tset(hl.tint32), call=hl.tcall, locus=hl.tlocus('GRCh37'),
        nested=hl.tstruct(value=hl.tint32), empty=hl.tarray(hl.tstruct()),
    )
    table = hl.Table.parallelize([
        {'a':[hl.Struct(x=1, s='a\n'), None], 't':(2,'q'), 'd':{'a':3}, 's':{1,2},
         'call':hl.Call((0,1), True), 'locus':hl.Locus('1',1,'GRCh37'), 'nested':None,
         'empty':[hl.Struct(),None]},
    ], schema=schema)
    output = _show(table, n=1, width=400, truncate=64, types=False)
    for expected in ['[(1,"a\\n"),NA]', '(2,"q")', '{"a":3}', '{1,2}', str(hl.Call((0,1), True)), '1:1', '[(),NA]']:
        assert expected in output
    assert 'nested.value' in output and 'NA' in output


@pytest.mark.parametrize('truncate', [4, 8, 12, 16, 64])
def test_show_nested_cells_truncate_after_rendering_exact_prefix(truncate):
    table = hl.Table.parallelize([{'x': [[1, None], [2, 3]], 'y': [('a\n', 1), ('b', 2)]}])
    output = _show(table, n=1, width=160, truncate=truncate, types=False)
    for rendered in ['[[1,NA],[2,3]]', '[("a\\n",1),("b",2)]']:
        expected = rendered[:truncate-3]+'...' if len(rendered)>truncate else rendered
        assert f'| {expected}' in output


def test_show_empty_struct_rows_and_zero_row_sentinel():
    output = _show(hl.range_table(2).key_by().select(), n=1, width=80, types=False)
    assert output == '++\n||\n++\n||\n++\nshowing top 1 row\n'
    table = hl.Table.parallelize([{'values':[1,2,3]}])
    output = _show(table, n=0, width=80, types=False)
    assert 'showing top 0 rows' in output and '[1,2,3]' not in output


def test_show_numeric_alignment_special_logical_values():
    import numpy as np
    table = hl.Table.parallelize([{'i':-2, 'f':float('nan'), 'interval':hl.Interval(1,3),
                                  'nd':np.array([[1,2],[3,4]],dtype=np.int32)}])
    output = _show(table, n=1, width=200, truncate=64)
    assert 'NaN' in output and '[1-3)' in output and '[[1 2]' in output


def test_show_masks_hidden_children_of_missing_arrow_parent():
    import pyarrow as pa
    nested = pa.StructArray.from_arrays([pa.array([123, 7], type=pa.int32())],
                                        names=['value'], mask=pa.array([True, False]))
    table = hl.Table.parallelize(pa.table({'nested':nested}))
    output = _show(table, n=-1, width=80, types=False)
    assert 'nested.value' in output and '123' not in output
    assert [line.split('|')[1].strip() for line in output.splitlines() if line.startswith('|')][1:] == ['NA','7']
