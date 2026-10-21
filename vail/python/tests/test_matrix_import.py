"""Public contracts for Hail's delimited matrix format."""
import pytest
import vail as hl


def test_export_labels_follow_permuted_column_slots(tmp_path):
    matrix = hl.utils.range_matrix_table(1, 2)
    matrix = matrix.key_cols_by(s='s' + hl.str(matrix.col_idx))
    matrix = matrix.annotate_entries(x=matrix.col_idx + 10).choose_cols([1, 0])
    path = tmp_path / 'permuted.tsv'
    matrix.x.export(path)
    assert path.read_text().splitlines() == ['row_idx\ts1\ts0', '0\t11\t10']
    restored = hl.import_matrix_table(path, row_fields={'row_idx': hl.tint32}, row_key='row_idx')
    assert [(row.col_id, row.x) for row in restored.entries().collect()] == [('s0', 10), ('s1', 11)]


def test_header_order_missing_values_and_native_laziness(tmp_path):
    path = tmp_path / 'matrix.tsv'
    path.write_text('name\tposition\ts1\ts2\na\t7\t1\tNA\n')
    mt = hl.import_matrix_table(str(path), row_fields={'position': hl.tint64, 'name': hl.tstr}, row_key='name')
    path.write_text('name\tposition\ts1\ts2\nb\t8\tNA\t3\n')
    assert mt.entries().collect() == [
        hl.Struct(name='b', position=8, col_id='s1', x=None),
        hl.Struct(name='b', position=8, col_id='s2', x=3),
    ]


def test_no_header_comments_and_literal_delimiter(tmp_path):
    path = tmp_path / 'matrix.txt'
    path.write_text('# comment\n\na.1.2\nb.NA.4\n')
    mt = hl.import_matrix_table(str(path), row_fields={'f0': hl.tstr}, no_header=True,
                                delimiter='.', comment='#')
    assert mt.row_id.collect() == [0, 1]
    assert mt.col_id.collect() == [0, 1]
    assert mt.x.collect() == [1, 2, None, 4]


def test_headers_validated_for_each_file_and_glob(tmp_path):
    first, second = tmp_path / 'a.tsv', tmp_path / 'b.tsv'
    first.write_text('s1\ts2\n1\t2\n')
    second.write_text('s1\ts3\n3\t4\n')
    with pytest.raises(ValueError, match='invalid header'):
        hl.import_matrix_table(str(tmp_path / '*.tsv'))
    second.write_text('s1\ts2\n3\t4\n')
    assert hl.import_matrix_table(str(tmp_path / '*.tsv')).x.collect() == [1, 2, 3, 4]


def test_zero_columns_and_bad_entry(tmp_path):
    path = tmp_path / 'matrix.tsv'
    path.write_text('id\nA\nB\n')
    mt = hl.import_matrix_table(str(path), row_fields={'id': hl.tstr}, row_key='id')
    assert mt.count() == (2, 0)
    assert mt.id.collect() == ['A', 'B']
    path.write_text('s1\ts2\n1\t2\n3\n')
    mt = hl.import_matrix_table(str(path))
    with pytest.raises(Exception, match='unexpected end of line while reading entries'):
        mt.entries().collect()


@pytest.mark.parametrize('header', [True, False])
@pytest.mark.parametrize('entry_type', [hl.tint32, hl.tfloat64, hl.tstr])
def test_expression_export_roundtrip(tmp_path, header, entry_type):
    path = str(tmp_path / 'matrix.tsv')
    mt = hl.utils.range_matrix_table(3, 2)
    mt = mt.annotate_entries(x=hl.cast(mt.row_idx + mt.col_idx, entry_type))
    mt.x.export(path, header=header)
    row_field = 'row_idx' if header else 'f0'
    actual = hl.import_matrix_table(path, row_fields={row_field: hl.tint32},
                                    no_header=not header, row_key=row_field, entry_type=entry_type)
    assert actual.x.collect() == mt.x.collect()
