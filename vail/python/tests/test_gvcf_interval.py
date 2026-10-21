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
