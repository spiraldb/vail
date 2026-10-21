"""Exercise the public CSV entry point against Hail's CSV wrapper contract."""
import gzip

import pytest
import vail as hl


def test_csv_default_strings_and_quoted_commas(tmp_path):
    path = tmp_path / 'records.csv'
    path.write_bytes(b'id,label,value\r\n2,"a,b",NA\r\n1,ab, 3 ')
    table = hl.import_csv(str(path), key='id')
    assert table.row.dtype == hl.tstruct(id=hl.tstr, label=hl.tstr, value=hl.tstr)
    assert table.collect() == [
        hl.Struct(id='1', label='ab', value=' 3 '),
        hl.Struct(id='2', label='a,b', value=None),
    ]


def test_csv_hail_rejects_embedded_quotes(tmp_path):
    path = tmp_path / 'records.csv'
    path.write_text('label\n"a""b"\n')
    with pytest.raises(hl.utils.FatalError, match='quot'):
        hl.import_csv(str(path)).collect()


def test_csv_imputation_types_and_missing(tmp_path):
    path = tmp_path / 'records.csv'
    path.write_text('id,value,flag\n2,1.5,true\n1,.,false\n')
    table = hl.import_csv(str(path), impute=True, types={'id': hl.tstr}, missing='.')
    assert table.row.dtype == hl.tstruct(id=hl.tstr, value=hl.tfloat64, flag=hl.tbool)
    assert table.collect() == [hl.Struct(id='2', value=1.5, flag=True),
                               hl.Struct(id='1', value=None, flag=False)]


def test_csv_no_header_filters_and_source(tmp_path):
    path = tmp_path / 'records.csv.gz'
    with gzip.open(path, 'wt') as out:
        out.write('# comment\n\nskip,record\na,x\n')
    table = hl.import_csv(str(path), no_header=True, comment='#',
                          skip_blank_lines=True, filter='skip',
                          find_replace=('x', 'y'), force=True,
                          source_file_field='source')
    rows = table.collect()
    assert len(rows) == 1
    assert rows[0].f0 == 'a' and rows[0].f1 == 'y'
    assert rows[0].source.endswith('/records.csv.gz')


def test_csv_custom_quote_and_multiple_files(tmp_path):
    paths = [tmp_path / f'{i}.csv' for i in range(2)]
    for i, path in enumerate(paths):
        path.write_text(f"id,label\n{i},'a,b'\n")
    rows = hl.import_csv([str(p) for p in paths], quote="'").collect()
    assert rows == [hl.Struct(id='0', label='a,b'), hl.Struct(id='1', label='a,b')]
