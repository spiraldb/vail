"""Public interval-array import checks against the pinned Hail reader contract."""
import pytest
import vail as hl

HEADER = '''##fileformat=VCFv4.2
##INFO=<ID=END,Number=1,Type=Integer,Description="Block end">
##INFO=<ID=FLAG,Number=0,Type=Flag,Description="Flag">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Depths">
##FORMAT=<ID=AF,Number=A,Type=Float,Description="Frequency">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tz\ta
'''


def fixture(tmp_path):
    path = tmp_path / 'sample.g.vcf'
    path.write_text(HEADER +
        '1\t1\t.\tA\t<NON_REF>\t.\tPASS\tEND=9\tGT:AD:AF\t0/0:10,0:0.0\t0/0:8,0:0.0\n' +
        '1\t10\trs10\tG\tT,<NON_REF>\t30\tPASS\tFLAG\tGT:AD:AF\t0|1:5,3,0:0.4,0.0\t.:.:.\n' +
        '1\t11\t.\tA\t<NON_REF>\t.\tPASS\tEND=20\tGT:AD:AF\t0/0:10,0:0.0\t0/0:8,0:0.0\n')
    return path


def test_gvcf_interval_boundaries_symbolic_alleles_and_sample_order(tmp_path):
    path = fixture(tmp_path)
    header = hl.eval(hl.get_vcf_header_info(path))
    assert header.sampleIDs == ['z', 'a']
    assert header.infoFields == [('END', 'Int32'), ('FLAG', 'Boolean')]
    assert header.formatFields == [('GT', 'String'), ('AD', 'Array[Int32]'), ('AF', 'Array[Float64]')]
    rows = hl.eval(hl.import_gvcf_interval(path, 3, '1', 1, 10, header))
    assert [r.locus.position for r in rows] == [1, 10]
    assert rows[0].info.END == 9
    assert rows[0].alleles == ['A', '<NON_REF>']
    assert rows[1].alleles == ['G', 'T', '<NON_REF>']
    assert rows[1].__entries[0].GT == hl.Call((0, 1), True)
    assert rows[1].__entries[0].AD == [5, 3, 0]
    assert rows[1].__entries[1].GT is None
    assert rows[1].info.FLAG is True
    # A reference block starting before the interval does not overlap-import.
    assert hl.eval(hl.import_gvcf_interval(path, 0, '1', 2, 9, header)) == []
    assert hl.eval(hl.import_gvcf_interval(path, 0, '2', 1, 100, header)) == []


def test_gvcf_interval_scalar_expressions_header_override_and_float_type(tmp_path):
    path = fixture(tmp_path)
    header = hl.eval(hl.get_vcf_header_info(path))
    header = dict(header)
    header['sampleIDs'] = ['renamed1', 'renamed2']
    records = hl.import_gvcf_interval(hl.literal(str(path)), 0, hl.literal('1'),
                                    hl.int32(10), hl.int32(10), header,
                                    reference_genome=None, entry_float_type=hl.tfloat32)
    assert records.dtype.element_type['__entries'].element_type['AF'] == hl.tarray(hl.tfloat32)
    row = hl.eval(records)[0]
    assert row.locus == hl.Struct(contig='1', position=10)
    assert row.__entries[0].AF == pytest.approx([0.4, 0.0])


def test_gvcf_header_does_not_decode_data_and_errors_are_explicit(tmp_path):
    path = tmp_path / 'bad.vcf'
    path.write_text(HEADER + 'bad data\n')
    header = hl.get_vcf_header_info(path)
    assert hl.eval(header).sampleIDs == ['z', 'a']
    records = hl.import_gvcf_interval(path, 0, '1', 1, 10, header)
    with pytest.raises(Exception, match='VCF|vcf|field|column'):
        hl.eval(records)
    table = hl.utils.range_table(1).annotate(path=str(path))
    with pytest.raises(NotImplementedError, match='row-dependent paths'):
        hl.import_gvcf_interval(table.path, 0, '1', 1, 10, header)
    with pytest.raises(TypeError, match='start must have type'):
        hl.import_gvcf_interval(path, 0, '1', 1.0, 10, header)


def test_gvcf_interval_row_dependent_bounds(tmp_path):
    path = fixture(tmp_path)
    header = hl.get_vcf_header_info(path)
    bounds = hl.utils.range_table(2)
    bounds = bounds.annotate(records=hl.import_gvcf_interval(
        path, 0, '1', bounds.idx + 10, bounds.idx + 10, header))
    assert [[record.locus.position for record in row.records] for row in bounds.collect()] == [[10], [11]]


def _indexed_fixture(tmp_path, body, *, name='indexed.g.vcf.bgz'):
    import shutil
    import subprocess
    bgzip = shutil.which('bgzip')
    tabix = shutil.which('tabix')
    if bgzip is None or tabix is None:
        pytest.skip('indexed gVCF tests require htslib bgzip and tabix')
    source = tmp_path / 'input.g.vcf'
    source.write_text(HEADER + body)
    output = tmp_path / name
    with output.open('wb') as stream:
        subprocess.run([bgzip, '-c', str(source)], stdout=stream, check=True)
    subprocess.run([tabix, '-f', '-p', 'vcf', str(output)], check=True)
    return output


def test_indexed_gvcf_queries_do_not_decode_unselected_records(tmp_path):
    # Invalid AD outside the selected interval and inside a reference block
    # overlapping the interval both fail full VCF decoding. The indexed reader
    # excludes them before decoding, according to Hail's POS-only semantics.
    path = _indexed_fixture(tmp_path,
        '1\t1\t.\tA\t<NON_REF>\t.\tPASS\tEND=100\tGT:AD\t0/0:bad\t0/0:bad\n' +
        '1\t10\t.\tG\tT,<NON_REF>\t.\tPASS\t.\tGT:AD\t0/1:5,3,0\t0/0:8,0,0\n' +
        '2\t20\t.\tA\tC\t.\tPASS\t.\tGT:AD\t0/1:bad\t0/0:bad\n')
    header = hl.get_vcf_header_info(path)
    records = hl.import_gvcf_interval(path, 0, '1', 10, 10, header)
    rows = hl.eval(records)
    assert [row.locus.position for row in rows] == [10]
    assert rows[0].__entries[0].AD == [5, 3, 0]
    with pytest.raises(Exception, match='invalid integer|invalid digit|VCF|vcf'):
        hl.import_vcf(path).entries().collect()


def test_indexed_gvcf_remains_lazy_and_supports_recoding(tmp_path):
    body = '1\t10\t.\tG\tT,<NON_REF>\t.\tPASS\t.\tGT:AD\t0/1:5,3,0\t0/0:8,0,0\n'
    path = _indexed_fixture(tmp_path, body)
    header = hl.get_vcf_header_info(path)
    records = hl.import_gvcf_interval(path, 0, 'chr1', 10, 10, header,
                                    contig_recoding={'1': 'chr1'}, reference_genome='GRCh38')
    # Replacing and reindexing the data after construction must change the
    # result, proving no eager interval materialization occurred.
    _indexed_fixture(tmp_path, body.replace('5,3,0', '6,4,0'))
    row = hl.eval(records)[0]
    assert row.locus == hl.Locus('chr1', 10, reference_genome='GRCh38')
    assert row.__entries[0].AD == [6, 4, 0]
    assert hl.eval(hl.import_gvcf_interval(path, 0, 'chr2', 1, 100, header,
                                       contig_recoding={'1': 'chr1'}, reference_genome='GRCh38')) == []


def test_indexed_gvcf_reports_index_failure_at_execution(tmp_path):
    path = _indexed_fixture(tmp_path,
        '1\t10\t.\tA\tC\t.\tPASS\t.\tGT:AD\t0/1:5,3\t0/0:8,0\n')
    header = hl.get_vcf_header_info(path)
    records = hl.import_gvcf_interval(path, 0, '1', 10, 10, header)
    (tmp_path / (path.name + '.tbi')).write_bytes(b'corrupted')
    with pytest.raises(Exception, match='tabix query failed'):
        hl.eval(records)
