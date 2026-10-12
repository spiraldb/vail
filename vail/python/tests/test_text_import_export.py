from pathlib import Path

import pytest
import vail as hl


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
    assert hl.eval(hl.literal('a,NA,')._split_line(',', quote=None)) == ['a', None, '']
    assert hl.eval(hl.literal('a,"b,c"')._split_line(',', quote='"', missing=[])) == ['a', 'b,c']
    assert hl.eval(hl.literal('')._split_line(',', missing=[])) == ['']


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


def test_export_without_header_and_nested_rejection(tmp_path):
    table = hl.utils.range_table(3, n_partitions=2)
    table.export(tmp_path / 'no-header.tsv', header=False)
    assert (tmp_path / 'no-header.tsv').read_text() == '0\n1\n2\n'
    with pytest.raises(NotImplementedError, match='nested'):
        table.annotate(values=hl.array([1])).export(tmp_path / 'nested.tsv')
    with pytest.raises(NotImplementedError, match='scientific'):
        table.annotate(value=hl.float64(table.idx)).export(tmp_path / 'float.tsv')


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
    with pytest.raises(NotImplementedError):
        hl.import_table(path, impute=True)
    with pytest.raises(ValueError):
        hl.import_table(path, delimiter='\\s+')
    with pytest.raises(NotImplementedError):
        hl.import_lines(path, force_bgz=True)
    with pytest.raises(NotImplementedError):
        hl.utils.range_table(1).export(tmp_path / 'out', parallel='header_per_shard')
    for action in ('export', 'export_csv', 'export_json'):
        with pytest.raises(NotImplementedError, match='compressed'):
            getattr(hl.utils.range_table(1), action)(tmp_path / 'out.gz')
        with pytest.raises(NotImplementedError, match='local'):
            getattr(hl.utils.range_table(1), action)('s3://bucket/out.txt')
    assert not (tmp_path / 'out.gz').exists()
