from pathlib import Path

import pytest
import vail as hl


def test_regex_csv_public_grammar_and_record_width(tmp_path):
    assert hl.eval(hl.split_line('1  "two words" NA ', r'\s+', quote='"', regex=True)) == ['1', 'two words', None, '']
    for line in ['1 "unterminated', '1 "a"tail']:
        with pytest.raises(RuntimeError):
            hl.eval(hl.split_line(line, r' +', quote='"', regex=True))
    path = tmp_path / 'wrong-width.tsv'
    path.write_text('a b\n1 2 3\n')
    with pytest.raises(RuntimeError, match='Expected 2 fields'):
        hl.import_table(path, delimiter=r' +').collect()


def test_import_typed_integer_arrays_and_type_aliases(tmp_path):
    path = tmp_path / 'arrays.tsv'
    path.write_text('idx\txs\n0\t[1, null, -2]\n1\t[]\n2\tNA\n3\t [ 2147483647 ] \n')
    table = hl.import_table(path, types={'idx': 'int', 'xs': 'array<int>'})
    assert table.row.dtype == hl.tstruct(idx=hl.tint32, xs=hl.tarray(hl.tint32))
    assert [(row.idx, row.xs) for row in table.collect()] == [
        (0, [1, None, -2]), (1, []), (2, None), (3, [2147483647]),
    ]


@pytest.mark.parametrize('value', ['[01]', '[+1]', '[1,]', '[1.0]', '1,2', '[2147483648]'])
def test_integer_array_import_rejects_malformed_json_or_overflow(tmp_path, value):
    path = tmp_path / 'invalid.tsv'
    path.write_text('xs\n' + value + '\n')
    with pytest.raises((RuntimeError, ValueError)):
        hl.import_table(path, types={'xs': hl.tarray(hl.tint32)}).collect()


def test_import_lines_preserves_physical_lines_and_files(tmp_path):
    one, two = tmp_path / 'one.txt', tmp_path / 'two.txt'
    one.write_bytes('a,"b"\r\n\r\n\t😀\nlast'.encode())
    two.write_text('x\n\n', encoding='utf8')
    table = hl.import_lines([one, two], file_per_partition=True)
    assert table.n_partitions() == 2
    assert [(row.file, row.text) for row in table.collect()] == [
        ('file:' + str(one), 'a,"b"'), ('file:' + str(one), ''), ('file:' + str(one), '\t😀'), ('file:' + str(one), 'last'),
        ('file:' + str(two), 'x'), ('file:' + str(two), ''),
    ]
    assert [row.text for row in table._filter_partitions([1]).collect()] == ['x', '']


def test_lines_are_lazy_and_empty_file_is_a_partition(tmp_path):
    path = tmp_path / 'lazy.txt'
    path.write_text('old\n')
    table = hl.import_lines(path)
    path.write_text('new\n')
    assert table.collect()[0].text == 'new'
    path.write_text('')
    assert table.collect() == []
    assert table.n_partitions() == 1


@pytest.mark.parametrize('contents', [b'a\rb\r', b'a\r\nb\r\n', b'a\nb\n'])
def test_lines_accept_hail_line_endings(tmp_path, contents):
    path = tmp_path / 'endings.txt'
    path.write_bytes(contents)
    assert [row.text for row in hl.import_lines(path).collect()] == ['a', 'b']


def test_lines_preserve_nul(tmp_path):
    path = tmp_path / 'nul.txt'
    path.write_bytes(b'a\x00b\n')
    assert hl.import_lines(path).collect()[0].text == 'a\x00b'
    assert hl.import_lines(path.as_uri()).collect()[0].text == 'a\x00b'


def test_lines_validate_utf8_and_missing_file_at_action(tmp_path):
    path = tmp_path / 'bad.txt'
    path.write_bytes(b'\xff\n')
    with pytest.raises(Exception, match='UTF|utf|encoding|stream'):
        hl.import_lines(path).collect()
    with pytest.raises(Exception):
        hl.import_lines(tmp_path / 'absent').collect()


def test_lines_globs_and_partition_minimum(tmp_path):
    (tmp_path / 'b.txt').write_text('b\n')
    (tmp_path / 'a.txt').write_text('a\n')
    assert [r.text for r in hl.import_lines(str(tmp_path / '*.txt')).collect()] == ['a', 'b']
    table = hl.import_lines(str(tmp_path / '*.txt'), min_partitions=4)
    assert table.n_partitions() == 4
    assert [r.text for r in table.collect()] == ['a', 'b']
    with pytest.raises(ValueError):
        hl.import_lines(str(tmp_path / '*.txt'), min_partitions=3, file_per_partition=True)


def test_import_table_uses_lines_and_handles_headers_missing_and_quotes(tmp_path):
    path = tmp_path / 'input.tsv'
    path.write_text('id\tvalue\tnote\n2\tNA\t"literal"\n1\t3\t\n')
    table = hl.import_table(path, types={'id': hl.tint32, 'value': hl.tint64},
                            key='id', source_file_field='source')
    rows = table.collect()
    assert [(r.id, r.value, r.note, r.source) for r in rows] == [
        (1, 3, '', 'file:' + str(path)), (2, None, '"literal"', 'file:' + str(path)),
    ]
    assert table.row.dtype == hl.tstruct(id=hl.tint32, value=hl.tint64, note=hl.tstr, source=hl.tstr)


def test_import_table_quoted_records_and_missing_markers(tmp_path):
    path = tmp_path / 'quoted.csv'
    path.write_text('id,note\n1,"a,b"\n2,"a b"\n3,\n4,NA\n')
    rows = hl.import_table(path, delimiter=',', quote='"', missing=['', 'NA']).collect()
    assert [(r.id, r.note) for r in rows] == [('1', 'a,b'), ('2', 'a b'), ('3', None), ('4', None)]


@pytest.mark.parametrize('bad', ['1,"a""b"', '1,"unterminated', '1,a"b'])
def test_import_table_rejects_non_hail_quote_syntax(tmp_path, bad):
    path = tmp_path / 'invalid-quotes.csv'
    path.write_text('id,note\n' + bad + '\n')
    with pytest.raises(Exception, match='quot|CSV'):
        hl.import_table(path, delimiter=',', quote='"').collect()


def test_split_line_expression():
    assert hl.eval(hl.split_line('a,NA,', ',', quote=None)) == ['a', None, '']
    assert hl.eval(hl.split_line('a,"b,c"', ',', quote='"', missing=[])) == ['a', 'b,c']
    assert hl.eval(hl.split_line('', ',', missing=[])) == ['']


@pytest.mark.parametrize('line,expected', [
    ('', ['']), ('\n', ['']), ('\r\n', ['']),
    (',,', ['', '', '']), ('a,NA,', ['a', None, '']),
    ('\r\na,b\n\r\n', ['a', 'b']),
    ('a,"b,c"', ['a', '"b', 'c"']),
    ('雪,😀,\x00', ['雪', '😀', '\x00']),
])
def test_unquoted_split_preserves_record_and_field_semantics(line, expected):
    assert hl.eval(hl.split_line(line, ',')) == expected


@pytest.mark.parametrize('line,message', [
    ('\r', 'empty CSV line'), ('\n\n', 'empty CSV line'),
    ('a\nb', 'expected exactly one CSV record'),
    ('a\r\nb', 'expected exactly one CSV record'),
    ('a\n ', 'expected exactly one CSV record'),
])
def test_unquoted_split_rejects_empty_or_multiple_records(line, message):
    with pytest.raises(RuntimeError, match=message):
        hl.eval(hl.split_line(line, ','))


@pytest.mark.parametrize('quote', [None, '"'])
def test_split_batch_keeps_rows_independent_with_long_null_and_multiline_values(quote):
    long = '雪' * 10000
    cases = [(None, None), ('', ['']), ('a,NA,', ['a', None, '']),
             (long + ',end', [long, 'end']), ('small', ['small'])]
    if quote is not None:
        cases += [('"left\nright",NA', ['left\nright', None]),
                  ('"a,b",tail', ['a,b', 'tail'])]
    cases *= 1500
    table = hl.Table.parallelize([{'line': line} for line, _ in cases],
                                schema=hl.tstruct(line=hl.tstr), n_partitions=3)
    actual = table.select(fields=hl.split_line(table.line, ',', quote=quote)).collect()
    assert [row.fields for row in actual] == [expected for _, expected in cases]


def test_text_source_transformations_and_globals(tmp_path):
    path = tmp_path / 'query.tsv'
    path.write_text('id\ttext\n1\ta,b\n2\tc,d\n')
    table = hl.import_table(path, types={'id': hl.tint32}).annotate_globals(suffix='!')
    table = table.annotate(parts=table.text.split(','), label=table.text + table.suffix)
    table = table.filter(table.id > 1)
    output = tmp_path / 'query.json'
    table.export_json(output)
    schema = hl.tstruct(id=hl.tint32, text=hl.tstr, parts=hl.tarray(hl.tstr), label=hl.tstr)
    assert hl.read_json(output, schema).collect() == [hl.Struct(id=2, text='c,d', parts=['c', 'd'], label='c,d!')]


def test_import_table_repeated_headers_comments_and_duplicate_names(tmp_path):
    paths = [tmp_path / 'one.tsv', tmp_path / 'two.tsv']
    paths[0].write_text('# comment\na\ta\ta_1\n1\t2\t3\n')
    paths[1].write_text('a\ta\ta_1\n4\t5\t6\n\n')
    table = hl.import_table(paths, comment='#', skip_blank_lines=True)
    assert list(table.row.dtype) == ['a', 'a_1', 'a_1_1']
    assert [tuple(r.values()) for r in table.collect()] == [('1', '2', '3'), ('4', '5', '6')]


def test_import_table_no_header_and_find_replace(tmp_path):
    path = tmp_path / 'no-header.txt'
    path.write_text('a|b\nc|d\n')
    rows = hl.import_table(path, no_header=True, delimiter='|', find_replace=('a', 'A')).collect()
    assert [(r.f0, r.f1) for r in rows] == [('A', 'b'), ('c', 'd')]


@pytest.mark.parametrize('bad', ['1\n', '1\t2\t3\n', '\n'])
def test_import_table_errors_on_malformed_records(tmp_path, bad):
    path = tmp_path / 'bad.tsv'
    path.write_text('a\tb\n' + bad)
    table = hl.import_table(path)
    with pytest.raises(Exception, match='field|blank|Blank|empty|Empty'):
        table.collect()


def test_import_table_bad_number_errors(tmp_path):
    path = tmp_path / 'bad-number.tsv'
    path.write_text('a\nwrong\n')
    with pytest.raises(Exception):
        hl.import_table(path, types={'a': hl.tint32}).collect()


@pytest.mark.parametrize('min_partitions', [None, 50])
def test_header_discovery_does_not_scan_the_entire_file(tmp_path, min_partitions):
    path = tmp_path / 'bounded-header.tsv'
    # The invalid UTF-8 lies beyond the first source batch. Constructing an
    # import must discover the header without counting every source partition.
    path.write_bytes(b'a\n' + b'x\n' * 8191 + b'\xff\n')
    table = hl.import_table(path, min_partitions=min_partitions)
    assert table.row.dtype == hl.tstruct(a=hl.tstr)
    with pytest.raises(Exception):
        table.count()


def test_text_export_and_import_roundtrip_multiple_partitions(tmp_path):
    table = hl.Table.parallelize([hl.Struct(id=2, name='two'), hl.Struct(id=1, name=None),
                                  hl.Struct(id=3, name='')],
                                 schema=hl.tstruct(id=hl.tint32, name=hl.tstr), n_partitions=2)
    output = tmp_path / 'out.tsv'
    table.export(output)
    assert output.read_text() == 'id\tname\n2\ttwo\n1\tNA\n3\t\n'
    restored = hl.import_table(output, types={'id': hl.tint32})
    assert restored.collect() == table.collect()


def test_export_without_header_and_hail_nested_format(tmp_path):
    table = hl.utils.range_table(3, n_partitions=2)
    table.export(tmp_path / 'no-header.tsv', header=False)
    assert (tmp_path / 'no-header.tsv').read_text() == '0\n1\n2\n'
    table.annotate(values=hl.array([1])).export(tmp_path / 'nested.tsv')
    assert (tmp_path / 'nested.tsv').read_text() == 'idx\tvalues\n0\t[1]\n1\t[1]\n2\t[1]\n'
    table.annotate(value=hl.float64(table.idx)).export(tmp_path / 'float.tsv')
    assert (tmp_path / 'float.tsv').read_text() == 'idx\tvalue\n0\t0.0000e+00\n1\t1.0000e+00\n2\t2.0000e+00\n'


def test_empty_and_multiple_batch_exports(tmp_path):
    empty = tmp_path / 'empty.tsv'
    hl.utils.range_table(0).export(empty)
    assert empty.read_text() == 'idx\n'
    output = tmp_path / 'many.tsv'
    hl.utils.range_table(20_000, n_partitions=3).export(output)
    lines = output.read_text().splitlines()
    assert lines[0] == 'idx'
    assert lines[1:] == [str(i) for i in range(20_000)]
    assert hl.import_table(output, types={'idx': hl.tint32}).count() == 20_000


def test_dataframe_csv_read_write_quoted_and_multiline(tmp_path):
    schema = hl.tstruct(id=hl.tint32, text=hl.tstr)
    table = hl.Table.parallelize([hl.Struct(id=1, text='a,b'), hl.Struct(id=2, text='a\nb'),
                                  hl.Struct(id=3, text='"quoted"'), hl.Struct(id=4, text=None)], schema=schema, n_partitions=2)
    output = tmp_path / 'out.csv'
    table.export_csv(output)
    assert hl.read_csv(output, schema).collect() == table.collect()
    assert hl.read_csv(output).count() == 4


def test_json_read_write_nested_and_lazy(tmp_path):
    path = tmp_path / 'input.json'
    path.write_text('{"id":1,"xs":[1,2],"nested":{"label":"hi"}}\n'
                    '{"id":2,"xs":null,"nested":null}\n')
    schema = hl.tstruct(id=hl.tint64, xs=hl.tarray(hl.tint64), nested=hl.tstruct(label=hl.tstr))
    table = hl.read_json(path, schema)
    output = tmp_path / 'out.json'
    table.export_json(output)
    assert hl.read_json(output, schema).collect() == table.collect()
    assert hl.read_json(path).count() == 2
    path.write_text('{"id":3,"xs":[],"nested":{"label":"changed"}}\n')
    assert table.collect()[0].id == 3


def test_text_option_rejections(tmp_path):
    path = tmp_path / 'input.tsv'
    path.write_text('a\n1\n')
    assert hl.import_table(path, impute=True).row.dtype == hl.tstruct(a=hl.tint32)
    assert hl.import_table(path, delimiter=r'\s+').collect() == [hl.Struct(a='1')]
    with pytest.raises(Exception):
        hl.import_lines(path, force_bgz=True).collect()
    with pytest.raises(NotImplementedError):
        hl.utils.range_table(1).export(tmp_path / 'out', parallel='header_per_shard')
    for action in ('export_csv', 'export_json'):
        with pytest.raises(NotImplementedError, match='compressed'):
            getattr(hl.utils.range_table(1), action)(tmp_path / 'out.gz')
        with pytest.raises(NotImplementedError, match='local'):
            getattr(hl.utils.range_table(1), action)('s3://bucket/out.txt')
    assert not (tmp_path / 'out.gz').exists()


def test_find_replace_removes_raw_repeated_headers_before_transforming_data(tmp_path):
    one,two=tmp_path/'one.tsv',tmp_path/'two.tsv'
    one.write_text('a\tb\na1\tx\na\tb\n')
    two.write_text('a\tb\na2\ty\n')
    table=hl.import_table([one,two],find_replace=('a','A'))
    assert list(table.row)==['a','b']
    assert table.collect()==[hl.Struct(a='A1',b='x'),hl.Struct(a='A2',b='y')]


def test_duplicate_header_suffixes_preserve_existing_name_collisions(tmp_path):
    path=tmp_path/'duplicates.tsv'
    names=['a','a','a_1','a','a_2','a']+['a']*128
    expected=['a','a_1','a_1_1','a_2','a_2_1','a_3']+[f'a_{i}' for i in range(4,132)]
    path.write_text('\t'.join(names)+'\n'+'\t'.join(map(str,range(len(names))))+'\n')
    table=hl.import_table(path)
    assert list(table.row)==expected
    assert list(table.collect()[0].values())==list(map(str,range(len(names))))


def test_public_split_line_missingness_validation_and_lazy_errors():
    table=hl.Table.parallelize([{'line':'a,,NA','keep':True},{'line':None,'keep':True}],
                               schema=hl.tstruct(line=hl.tstr,keep=hl.tbool))
    assert hl.split_line(table.line,',',missing=['','NA']).collect()==[['a',None,None],None]
    bad=hl.split_line('a,"bad',',',quote='"')
    with pytest.raises(RuntimeError,match='quot'):
        hl.eval(bad)
    assert hl.eval(hl.if_else(False,bad,hl.missing(hl.tarray(hl.tstr)))) is None
    assert hl.eval(hl.if_else(hl.missing(hl.tbool),bad,hl.missing(hl.tarray(hl.tstr)))) is None
    assert table.select(value=bad).filter(False).collect()==[]
    for delimiter in ['', 'é', '\n', '\x00']:
        with pytest.raises(ValueError): hl.split_line('a',delimiter)
    with pytest.raises(TypeError,match='string'): hl.split_line(1,',')
    with pytest.raises(ValueError): hl.split_line('a',',',quote=',')


def test_import_table_imputation_boundaries_and_missing(tmp_path):
    path = tmp_path / 'types.tsv'
    path.write_text('b\ti32\ti64\tf\tstr\tempty\n'
                    'TRUE\t-2147483648\t9223372036854775807\t2.5\tcat\tNA\n'
                    'false\t+00002147483647\t-9223372036854775808\t1e3\t3\tNA\n')
    table = hl.import_table(path, impute=True)
    assert table.row.dtype == hl.tstruct(b=hl.tbool, i32=hl.tint32, i64=hl.tint64,
                                        f=hl.tfloat64, str=hl.tstr, empty=hl.tstr)
    assert table.collect()[1].i32 == 2147483647
    assert hl.import_table(path, impute=True, types={'i64': hl.tstr}).i64.dtype == hl.tstr


def test_import_table_regex_quotes_and_empty_fields(tmp_path):
    path = tmp_path / 'regex.txt'
    path.write_text('a  b  c\n1  "two  words"  NA\n2  end  \n')
    rows = hl.import_table(path, delimiter=r' +', quote='"').collect()
    assert rows == [hl.Struct(a='1', b='two  words', c=None),
                    hl.Struct(a='2', b='end', c='')]
    assert hl.eval(hl.split_line('1  "two  words"  NA', r' +', quote='"', regex=True)) == ['1', 'two  words', None]


def test_typed_json_import_and_roundtrip(tmp_path):
    schema = hl.tstruct(nested=hl.tstruct(name=hl.tstr, scores=hl.tarray(hl.tfloat64)),
                        tags=hl.tset(hl.tstr), pair=hl.ttuple(hl.tint32, hl.tstr),
                        mapping=hl.tdict(hl.tstr, hl.tint32))
    rows = [hl.Struct(nested=hl.Struct(name='hello', scores=[1.25, None]),
                      tags={'a', 'b'}, pair=(3, 'x'), mapping={'key': 4}),
            hl.Struct(nested=None, tags=set(), pair=(None, 'z'), mapping={})]
    table = hl.Table.parallelize(rows, schema=schema)
    path = tmp_path / 'nested.tsv'
    table.export(path)
    restored = hl.import_table(path, types=dict(schema))
    assert restored.row.dtype == table.row.dtype
    assert restored.key.dtype == table.key.dtype
    assert restored.collect() == table.collect()
    assert hl.eval(hl.parse_json('{"a":5,"unknown":1}', 'struct{a:int32,b:str}')) == hl.Struct(a=5, b=None)
    assert hl.eval(hl.parse_json('[1,true,null]', 'array<int32>')) == [1, None, None]


def test_export_primitive_float_annotation_format(tmp_path):
    table = hl.utils.range_table(1).annotate(x=1.234567, y=hl.float32(1.25), z=-0.0)
    path = tmp_path / 'floats.tsv'
    table.export(path)
    assert path.read_text() == 'idx\tx\ty\tz\n0\t1.2346e+00\t1.25\t-0.0000e+00\n'


def test_public_json_scalar_roundtrip_preserves_logical_types():
    values = [hl.missing('int32'), hl.missing('str'),
              hl.missing('struct{a:int32,b:str}'), hl.locus('1', 10000),
              hl.set({'x', 'y'}), hl.array([1, 2, hl.missing('int32')]),
              hl.call(0, 2, phased=True), hl.locus_interval('1', 10000, 10005),
              hl.struct(foo='bar'), hl.tuple([1, 2, 'str']), hl.tuple([]), hl.struct()]
    for value in values:
        restored = hl.parse_json(hl.json(value), value.dtype)
        assert restored.dtype == value.dtype
        assert hl.eval(restored) == hl.eval(value)
    assert hl.eval(hl.json('abc')) == '"abc"'
    assert hl.eval(hl.json(hl.missing(hl.tint32))) == 'null'


def test_typed_import_roundtrip_genomic_and_ndarray_fields(tmp_path):
    table = hl.utils.range_table(2).annotate(
        locus=hl.locus('chr22', 33878978, 'GRCh38'),
        interval=hl.interval(hl.locus('1', 999), hl.locus('1', 1001)),
        call=hl.call(0, 1), missing_call=hl.missing(hl.tcall),
        pair=hl.tuple([hl.call(1, 2, phased=True), 'foo', hl.missing(hl.tstr)]),
        matrix=hl.nd.array([[0, 1], [2, 3]]),
    )
    path = tmp_path / 'genomic.tsv'
    table.export(path)
    restored = hl.import_table(path, types=dict(table.row.dtype)).key_by('idx')
    assert restored.row.dtype == table.row.dtype
    assert restored.key.dtype == table.key.dtype
    import numpy as np
    for expected, actual in zip(table.collect(), restored.collect(), strict=True):
        np.testing.assert_array_equal(actual.matrix, expected.matrix)
        assert {k: v for k, v in actual.items() if k != 'matrix'} == {
            k: v for k, v in expected.items() if k != 'matrix'}


def test_json_collections_deduplicate_with_last_dictionary_value():
    import json
    values = hl.parse_json('[0.0,-0.0,2.0,2.0,null,null]', 'set<float64>')
    assert hl.eval(hl.len(values)) == 3
    assert hl.eval(values) == {0.0, 2.0, None}
    assert hl.eval(hl.len(hl.parse_json('[1,1.0]', 'set<float64>'))) == 1
    assert hl.eval(hl.len(hl.parse_json('[16777216,16777217]', 'set<float32>'))) == 1
    mapping = hl.parse_json(
        '[{"key":0.0,"value":"first"},{"key":-0.0,"value":"last"}]',
        'dict<float64,str>')
    assert hl.eval(hl.len(mapping)) == 1
    assert hl.eval(mapping) == {0.0: 'last'}
    nested = hl.parse_json(
        '[{"key":{"x":1},"value":4},{"key":{"x":2},"value":5},'
        '{"key":{"x":1},"value":9}]', 'dict<struct{x:int32},int32>')
    assert json.loads(hl.eval(hl.json(nested))) == [
        {'key': {'x': 1}, 'value': 9}, {'key': {'x': 2}, 'value': 5}]


def test_json_large_collections_preserve_all_values():
    import json
    values = list(range(20000))
    parsed = hl.parse_json(json.dumps(values + values), 'set<int32>')
    assert hl.eval(parsed) == set(values)
    pairs = [{'key': str(i), 'value': i} for i in range(10000)]
    pairs += [{'key': str(i), 'value': -i} for i in range(10000)]
    parsed = hl.parse_json(json.dumps(pairs), 'dict<str,int32>')
    assert hl.eval(parsed) == {str(i): -i for i in range(10000)}


def test_json_nested_unordered_collections_use_logical_key_equality():
    import json
    sets = hl.parse_json('[[2,1],[1,2],[2,1,1]]', 'set<set<int32>>')
    assert hl.eval(hl.len(sets)) == 1
    assert hl.eval(sets.contains(hl.set([1, 2])))
    mapping = hl.parse_json(
        '[{"key":[2,1],"value":"first"},{"key":[1,2],"value":"last"}]',
        'dict<set<int32>,str>')
    assert hl.eval(hl.len(mapping)) == 1
    assert hl.eval(mapping.get(hl.parse_json('[1,2]', 'set<int32>'))) == 'last'
    assert hl.eval(mapping.get(hl.parse_json('[2,1]', 'set<int32>'))) == 'last'
    first = [{'key': 'a', 'value': 1}, {'key': 'b', 'value': 2}]
    second = list(reversed(first))
    mapping = hl.parse_json(json.dumps([
        {'key': first, 'value': 4}, {'key': second, 'value': 9}]),
        'dict<dict<str,int32>,int32>')
    assert hl.eval(hl.len(mapping)) == 1
    for key in (first, second):
        assert hl.eval(mapping.get(hl.parse_json(json.dumps(key), 'dict<str,int32>'))) == 9
    sets = hl.parse_json(json.dumps([first, second]), 'set<dict<str,int32>>')
    assert hl.eval(hl.len(sets)) == 1
