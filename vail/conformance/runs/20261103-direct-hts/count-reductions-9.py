import vail as hl
mt=hl.read_matrix_table('/Users/danielking/.cache/vail-benchmarks/indexed-20261103/profile-first-200.mt')
assert hl.split_multi_hts(mt)._force_count_rows()==204
print('output_rows=204 input_cells=507000 emitted_cells=517140')
