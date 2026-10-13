import vail as hl


def test_matrix_query_dynamic_points_ranges_and_filtered_entries(tmp_path):
    matrix = hl.range_matrix_table(4, 3)
    matrix = matrix.annotate_entries(value=matrix.row_idx * 10 + matrix.col_idx)
    matrix = matrix.filter_entries(matrix.col_idx != 1)
    path = str(tmp_path / 'matrix')
    matrix.write(path)
    expected = lambda i: hl.Struct(
        row_idx=i, entries_array=[hl.Struct(value=i * 10), None, hl.Struct(value=i * 10 + 2)]
    )
    outer = hl.range_table(5)
    rows = outer.annotate(matches=hl.query_matrix_table_rows(path, outer.idx)).collect()
    assert [row.matches for row in rows] == [[expected(i)] for i in range(4)] + [[]]
    ranges = outer.annotate(matches=hl.query_matrix_table_rows(
        path, hl.interval(outer.idx, outer.idx + 2)
    )).collect()
    assert [row.matches for row in ranges] == [
        [expected(j) for j in range(i, min(i + 2, 4))] for i in range(5)
    ]
    assert hl.eval(hl.query_matrix_table_rows(path, 2)) == [expected(2)]


def test_matrix_query_duplicate_keys_and_no_columns(tmp_path):
    matrix = hl.range_matrix_table(3, 0)
    matrix = matrix.key_rows_by(k=matrix.row_idx % 2)
    path = str(tmp_path / 'matrix')
    matrix.write(path)
    assert hl.eval(hl.query_matrix_table_rows(path, 0, 'e')) == [
        hl.Struct(row_idx=0, k=0, e=[]), hl.Struct(row_idx=2, k=0, e=[])
    ]
