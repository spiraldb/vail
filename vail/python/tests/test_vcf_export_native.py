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
    assert next(line for line in lines if line.startswith('#CHROM')).endswith('\tb\ta')
    assert lines[-2] == '1\t1\trs1\tA\tC\t12.0\tq10;q20\tFLAG;AF=0.25\tGT:DP:AD\t./.\t1|0:5:2,3'
    assert lines[-1] == '1\t2\t.\tT\t.\t.\tPASS\t.\tGT:DP:AD\t0/0\t1'
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
    assert [line for line in (tmp_path / 'plain.vcf').read_text().splitlines() if not line.startswith('##contig=')] == [line for line in output.read_text().splitlines() if not line.startswith('##contig=')]


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


def test_sites_table_and_compressed_roundtrip(tmp_path):
    import gzip
    matrix = source(tmp_path)
    for suffix in ('.vcf.gz', '.vcf.bgz'):
        output = tmp_path / ('out' + suffix)
        hl.export_vcf(matrix, output)
        assert gzip.open(output, 'rt').read().startswith('##fileformat=VCFv4.2\n')
        assert_public_matrix_equal(hl.import_vcf(output, force=True), matrix)
    output = tmp_path / 'sites.vcf'
    hl.export_vcf(matrix.rows(), output)
    assert_public_matrix_equal(hl.import_vcf(output), matrix.select_entries().filter_cols(False))


def test_format_header_order_preserved_with_gt_record_first(tmp_path):
    matrix = source(tmp_path).select_entries('DP', 'GT', 'AD')
    output = tmp_path / 'order.vcf'
    hl.export_vcf(matrix, output)
    assert_public_matrix_equal(hl.import_vcf(output), matrix)
    assert output.read_text().splitlines()[-2].split('\t')[8] == 'GT:DP:AD'


def test_separate_header_export_manifest_roundtrip(tmp_path):
    matrix = source(tmp_path)
    directory = tmp_path / 'shards.vcf.bgz'
    hl.export_vcf(matrix, directory, parallel='separate_header')
    names = (directory / 'shard-manifest.txt').read_text().splitlines()
    assert names == ['header.vcf.bgz', 'part-00000.vcf.bgz']
    assert_public_matrix_equal(hl.import_vcf(directory / names[1], header_file=directory / names[0]), matrix)
    combined = tmp_path / 'combined.vcf.bgz'
    with combined.open('wb') as output:
        for name in names:
            output.write((directory / name).read_bytes())
    assert_public_matrix_equal(hl.import_vcf(combined), matrix)


@pytest.mark.parametrize('suffix', ['.vcf', '.vcf.gz', '.vcf.bgz'])
def test_parallel_export_orders_partitions_and_preserves_missing_entries(tmp_path, suffix):
    import gzip
    samples = 400
    header = HEADER[:HEADER.index('#CHROM')] + '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t' + '\t'.join(f's{i}' for i in range(samples)) + '\n'
    records = [f'1\t{row}\t.\tA\tC\t.\tPASS\t.\tGT:DP:AD\t' + '\t'.join(f'0/1:{row + col}:2,3' for col in range(samples)) + '\n' for row in range(1, 601)]
    input_path = tmp_path / 'partitioned.vcf'
    input_path.write_text(header + ''.join(records))
    matrix = hl.import_vcf(input_path, min_partitions=7).choose_cols(list(reversed(range(samples))))
    matrix = matrix.filter_entries((matrix.locus.position + hl.int32(matrix.s[1:])) % 7 != 0)
    output = tmp_path / ('out' + suffix)
    hl.export_vcf(matrix, output)
    content = output.read_text() if suffix == '.vcf' else gzip.open(output, 'rt').read()
    lines = content.splitlines()
    assert sum(line.startswith('#CHROM') for line in lines) == 1
    assert next(line for line in lines if line.startswith('#CHROM')).endswith('\ts0')
    data = [line.split('\t') for line in lines if not line.startswith('#')]
    assert [int(line[1]) for line in data] == list(range(1, 601))
    for row, line in enumerate(data, 1):
        for col, entry in zip(reversed(range(samples)), line[9:], strict=True):
            assert entry == ('./.' if (row + col) % 7 == 0 else f'0/1:{row + col}:2,3')
    actual = hl.import_vcf(output, force=True)
    assert actual.count() == (600, samples)
    assert actual.aggregate_entries(hl.agg.sum(actual.DP)) == sum(row + col for row in range(1, 601) for col in range(samples) if (row + col) % 7)


@pytest.mark.parametrize('suffix', ['.vcf', '.vcf.gz', '.vcf.bgz'])
def test_parallel_export_late_failure_is_atomic(tmp_path, suffix):
    input_path = tmp_path / 'late.vcf'
    input_path.write_text(HEADER + ''.join(f'1\t{row}\t.\tA\tC\t.\tPASS\t.\tGT\t0/1\t0/0\n' for row in range(1, 601)))
    matrix = hl.import_vcf(input_path, min_partitions=7)
    matrix = matrix.annotate_rows(rsid=hl.if_else(matrix.locus.position == 550, 'bad\nidentifier', 'valid'))
    output = tmp_path / ('out' + suffix)
    output.write_bytes(b'previous result\n')
    before = set(tmp_path.iterdir())
    with pytest.raises(ValueError, match='tabs or newlines'):
        hl.export_vcf(matrix, output)
    assert output.read_bytes() == b'previous result\n'
    assert set(tmp_path.iterdir()) == before


def test_parallel_bgzf_export_is_tabix_queryable(tmp_path):
    import shutil
    import subprocess
    if shutil.which('tabix') is None:
        pytest.skip('tabix executable required')
    samples = 10000
    path = tmp_path / 'wide.vcf'
    header = HEADER[:HEADER.index('#CHROM')] + '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t' + '\t'.join(f's{i}' for i in range(samples)) + '\n'
    records = [f'1\t{row}\t.\tA\tC\t.\tPASS\t.\tGT:DP\t' + '\t'.join(f'0/1:{row + col}' for col in range(samples)) + '\n' for row in range(1, 21)]
    path.write_text(header + ''.join(records))
    matrix = hl.import_vcf(path, min_partitions=4)
    output = tmp_path / 'wide.vcf.bgz'
    hl.export_vcf(matrix, output, tabix=True)
    selected = subprocess.run(['tabix', str(output), '1:7-12'], check=True, capture_output=True, text=True).stdout.splitlines()
    assert [int(line.split('\t')[1]) for line in selected] == list(range(7, 13))
    for row, line in zip(range(7, 13), selected, strict=True):
        assert line.split('\t')[9:] == [f'0/1:{row + col}' for col in range(samples)]


def test_parallel_export_empty_matrix_has_one_complete_header(tmp_path):
    import gzip
    matrix = source(tmp_path).filter_rows(False)
    output = tmp_path / 'empty.vcf.bgz'
    hl.export_vcf(matrix, output)
    text = gzip.open(output, 'rt').read()
    assert text.count('#CHROM') == 1
    assert all(line.startswith('#') for line in text.splitlines())
    assert hl.import_vcf(output).count() == (0, 2)
