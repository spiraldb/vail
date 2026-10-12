"""Native VCF parser regressions beyond the upstream fixture inventory."""
import gzip
import pytest
import vail as hl

HEADER = '''##fileformat=VCFv4.2
##INFO=<ID=FLAG,Number=0,Type=Flag,Description="a flag">
##INFO=<ID=AF,Number=A,Type=Float,Description="frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="depth">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="depths">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ta\tb
'''


def test_changing_format_trailing_missing_and_missing_calls(tmp_path):
    path=tmp_path/'test.vcf'
    path.write_text(HEADER +
        '1\t2\t.\tA\tC\t.\tPASS\tFLAG;AF=0.2\tGT:DP:AD\t1|0:5:2,3\t.\n' +
        '1\t1\trs1\tT\t.\t20\tq10;q10\t.\tAD:GT\t4:1\t.:./1\n')
    m=hl.import_vcf(path)
    assert m.s.collect()==['a','b']
    assert m.alleles.collect()==[['T'],['A','C']]
    assert m.filters.collect()==[{'q10'},set()]
    assert m.info.FLAG.collect()==[False,True]
    assert m.GT.collect()==[hl.Call((1,),False),None,hl.Call((1,0),True),None]
    assert m.DP.collect()==[None,None,5,None]
    assert m.AD.collect()==[[4],None,[2,3],None]


def test_bgzf_members_header_override_and_drop_samples(tmp_path):
    rows='1\t1\t.\tA\tC\t.\tPASS\t.\tGT:DP\t0/1:7\t0/0:9\n'
    path=tmp_path/'test.vcf.bgz'
    path.write_bytes(gzip.compress(HEADER.encode())+gzip.compress(rows.encode()))
    m=hl.import_vcf(path)
    assert m.DP.collect()==[7,9]
    assert hl.import_vcf(path,drop_samples=True).count()==(1,0)
    header=tmp_path/'header.vcf';header.write_text(HEADER)
    body=tmp_path/'body.vcf';body.write_text(rows)
    assert hl.import_vcf(body,header_file=header)._same(m)


def test_column_count_and_partial_call_validation(tmp_path):
    path=tmp_path/'bad.vcf'
    path.write_text(HEADER+'1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t./bad\t0/0\n')
    m=hl.import_vcf(path)
    with pytest.raises(RuntimeError,match='invalid character'):
        m.entries().collect()
    path.write_text(HEADER+'1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t0/0\n')
    with pytest.raises(RuntimeError,match='expected 2 samples'):
        hl.import_vcf(path).entries().collect()


def test_header_mismatch_and_float_option(tmp_path):
    first=tmp_path/'a.vcf';second=tmp_path/'b.vcf'
    first.write_text(HEADER)
    second.write_text(HEADER.replace('\ta\tb\n','\tx\tb\n'))
    with pytest.raises(ValueError,match='invalid sample IDs'):
        hl.import_vcf([first,second])
    with pytest.raises(TypeError,match='entry_float_type'):
        hl.import_vcf(first,entry_float_type=hl.tint32)


def test_float_expression_width_and_similarity_policy():
    from vail.expr.expressions.typed_expressions import Float32Expression, Float64Expression
    assert type(hl.float32(1)) is Float32Expression
    assert type(hl.float64(1)) is Float64Expression
    for left,right,options,expected in [
        (1.0,1.0000001,{},True), (1.0,1.01,{},False),
        (float('nan'),float('nan'),{},False),
        (float('nan'),float('nan'),dict(nan_same=True),True),
        (float('inf'),float('inf'),{},True),
        (float('inf'),1.0,{},False),
        (100.0,100.01,dict(tolerance=.001,absolute=True),False),
    ]:
        assert hl.eval(hl.approx_equal(left,right,**options)) is expected
    assert hl.eval(hl.coalesce(hl.missing(hl.tint32),2,3))==2
    assert hl.eval(hl.approx_equal(hl.missing(hl.tfloat64),1.0)) is None


def test_cross_matrix_entry_lookup_with_reordered_string_sample_keys():
    m=hl.range_matrix_table(3,100)
    m=m.annotate_cols(sample=hl.str(m.col_idx)).key_cols_by('sample')
    m=m.annotate_entries(dosage=hl.float64(m.row_idx)+hl.float64(m.col_idx)/100)
    target=m.annotate_entries(dosage=hl.float32(m.dosage)).choose_cols(list(reversed(range(100))))
    target=target.filter_rows(target.row_idx!=1)
    result=m.annotate_entries(copy=target[m.row_key,m.col_key].dosage)
    values=result.copy.collect()
    assert values[100:200]==[None]*100
    assert values[:100]==pytest.approx([i/100 for i in range(100)],rel=1e-6)
    assert values[200:]==pytest.approx([2+i/100 for i in range(100)],rel=1e-6)
