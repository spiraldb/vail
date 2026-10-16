"""VCF sink ownership, formatting, and publication regressions."""
import pytest
import vail as hl

HEADER = '''##fileformat=VCFv4.2
##INFO=<ID=FLAG,Number=0,Type=Flag,Description="flag">
##INFO=<ID=AF,Number=A,Type=Float,Description="frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="depth">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="depths">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ta\tb
'''


def source(tmp_path):
    path = tmp_path / 'in.vcf'
    path.write_text(HEADER +
        '1\t1\trs1\tA\tC\t12\tq20;q10\tFLAG;AF=0.25\tDP:GT:AD\t5:1|0:2,3\t.:.:.\n' +
        '1\t2\t.\tT\t.\t.\tPASS\t.\tGT\t1\t0/0\n')
    return hl.import_vcf(path)


def assert_public_matrix_equal(actual, expected):
    assert actual.row.dtype == expected.row.dtype
    assert actual.col.dtype == expected.col.dtype
    assert actual.entry.dtype == expected.entry.dtype
    assert actual.row_key.dtype == expected.row_key.dtype
    assert actual.col_key.dtype == expected.col_key.dtype
    assert actual.rows().collect() == expected.rows().collect()
    assert actual.cols().collect() == expected.cols().collect()
    assert actual.entries().collect() == expected.entries().collect()
    assert hl.eval(actual.globals) == hl.eval(expected.globals)


def test_export_preserves_format_and_column_slot_order(tmp_path):
    matrix = source(tmp_path).choose_cols([1, 0])
    output = tmp_path / 'out.vcf'
    hl.export_vcf(matrix, output)
    lines = output.read_text().splitlines()
    assert lines[-3].endswith('\tb\ta')
    assert lines[-2] == '1\t1\trs1\tA\tC\t12.0\tq10;q20\tFLAG;AF=0.25\tGT:DP:AD\t./.:.:.\t1|0:5:2,3'
    assert lines[-1] == '1\t2\t.\tT\t.\t.\tPASS\t.\tGT:DP:AD\t0/0:.:.\t1:.:.'
    assert_public_matrix_equal(hl.import_vcf(output), matrix)


def test_failed_export_preserves_destination_and_removes_temporary_file(tmp_path):
    matrix = source(tmp_path)
    matrix = matrix.annotate_rows(rsid=hl.if_else(matrix.locus.position == 2, 'bad\nidentifier', 'valid'))
    output = tmp_path / 'out.vcf'
    output.write_text('previous result\n')
    before = set(tmp_path.iterdir())
    with pytest.raises(ValueError, match='tabs or newlines'):
        hl.export_vcf(matrix, output)
    assert output.read_text() == 'previous result\n'
    assert set(tmp_path.iterdir()) == before


def test_export_no_samples_and_plain_locus_struct(tmp_path):
    matrix = source(tmp_path).filter_cols(False)
    output = tmp_path / 'no-samples.vcf'
    hl.export_vcf(matrix, output)
    assert all(len(line.split('\t')) == 8 for line in output.read_text().splitlines() if not line.startswith('##'))
    plain = hl.import_vcf(output, reference_genome=None)
    hl.export_vcf(plain, tmp_path / 'plain.vcf')
    assert (tmp_path / 'plain.vcf').read_text() == output.read_text()


def test_export_wide_format_fields_from_sliced_rows(tmp_path):
    matrix = source(tmp_path).tail(1).choose_cols([1, 0])
    matrix = matrix.annotate_entries(**{
        f'F{i}': matrix.locus.position * 100 + matrix.s.length() + i
        for i in range(12)
    })
    output = tmp_path / 'wide.vcf'
    hl.export_vcf(matrix, output)
    line = output.read_text().splitlines()[-1].split('\t')
    names = line[8].split(':')
    for sample in line[9:]:
        values = dict(zip(names, sample.split(':'), strict=True))
        assert [values[f'F{i}'] for i in range(12)] == [str(201 + i) for i in range(12)]
    assert_public_matrix_equal(hl.import_vcf(output), matrix)
