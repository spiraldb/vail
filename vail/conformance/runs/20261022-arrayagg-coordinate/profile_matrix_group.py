import vail as hl

rows, cols = 100_000, 128
matrix = hl.range_matrix_table(rows, cols, n_partitions=16)
matrix = matrix.annotate_entries(x=matrix.row_idx + matrix.col_idx)
grouped = matrix.group_rows_by(new_idx=matrix.row_idx % 3).aggregate(
    x=hl.agg.mean(matrix.x)
)
entries = grouped.entries().collect()
assert len(entries) == 3 * cols
expected_row_mean = {0: (rows - 1) / 2, 1: (rows - 1) / 2 - 0.5, 2: (rows - 1) / 2 + 0.5}
for entry in entries:
    expected = expected_row_mean[entry.new_idx] + entry.col_idx
    assert abs(entry.x - expected) < 1e-9
