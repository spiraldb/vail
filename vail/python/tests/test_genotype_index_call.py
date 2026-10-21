"""Diploid genotype-index inversion through the public scalar and matrix APIs."""
import math
import pytest
import vail as hl


def expected(index):
    b = (math.isqrt(8 * index + 1) - 1) // 2
    return hl.Call([index - b * (b + 1) // 2, b])


@pytest.mark.parametrize('index', [0, 1, 2, 3, 32767, 65535, 2147450879, 2147450880, 2147483647])
def test_diploid_index_scalar_boundaries(index):
    assert hl.eval(hl.unphased_diploid_gt_index_call(index)) == expected(index)


def test_diploid_index_missing_negative_and_relational():
    assert hl.eval(hl.unphased_diploid_gt_index_call(hl.missing(hl.tint32))) is None
    with pytest.raises(hl.utils.FatalError, match='nonnegative'):
        hl.eval(hl.unphased_diploid_gt_index_call(-1))
    with pytest.raises(TypeError, match='int32'):
        hl.unphased_diploid_gt_index_call(hl.int64(0))
    indices = [0, None, 1, 32767, 65535, 2147483647]
    table = hl.Table.parallelize([{'i': i} for i in indices], schema=hl.tstruct(i=hl.tint32))
    assert table.select(c=hl.unphased_diploid_gt_index_call(table.i)).collect() == [
        hl.Struct(c=None if i is None else expected(i)) for i in indices]
    invalid = hl.Table.parallelize([{'i': -1}], schema=hl.tstruct(i=hl.tint32))
    with pytest.raises(hl.utils.FatalError, match='nonnegative'):
        invalid.select(c=hl.unphased_diploid_gt_index_call(invalid.i)).collect()


def test_diploid_index_zero_column_matrix_vcf_export(tmp_path):
    matrix = hl.range_matrix_table(10, 10, n_partitions=4).filter_cols(False).key_cols_by(s='dummy')
    matrix = matrix.annotate_entries(GT=hl.unphased_diploid_gt_index_call(0))
    matrix = matrix.key_rows_by(locus=hl.locus('1', 100 - matrix.row_idx), alleles=['A', 'T'])
    assert [row.locus.position for row in matrix.rows().collect()] == list(range(91, 101))
    path = tmp_path / 'sorted.vcf'
    hl.export_vcf(matrix, path)
    assert hl.import_vcf(path).locus.position.collect() == list(range(91, 101))
