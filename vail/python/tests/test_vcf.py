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


def test_import_vcf_builds_nullable_entry_slots(tmp_path):
    path = tmp_path / 'entries.vcf'
    path.write_text(HEADER +
        '1\t1\t.\tA\tC\t.\tPASS\t.\tGT:DP\t.:.\t0/1:7\n' +
        '1\t2\t.\tG\tT\t.\tPASS\t.\tGT:DP\t1/1:9\t.:.\n')

    mt = hl.import_vcf(path)

    assert mt.s.collect() == ['a', 'b']
    assert mt.count() == (2, 2)
    entries = mt.entries().collect()
    assert [(entry.locus.position, entry.s) for entry in entries] == [
        (1, 'a'), (1, 'b'), (2, 'a'), (2, 'b'),
    ]
    assert [(entry.GT, entry.DP, entry.AD) for entry in entries] == [
        (None, None, None),
        (hl.Call([0, 1]), 7, None),
        (hl.Call([1, 1]), 9, None),
        (None, None, None),
    ]


def test_bgzf_members_header_override_and_drop_samples(tmp_path):
    rows='1\t1\t.\tA\tC\t.\tPASS\t.\tGT:DP\t0/1:7\t0/0:9\n'
    path=tmp_path/'test.vcf.bgz'
    path.write_bytes(gzip.compress(HEADER.encode())+gzip.compress(rows.encode()))
    m=hl.import_vcf(path)
    assert m.DP.collect()==[7,9]
    assert hl.import_vcf(path,drop_samples=True).count()==(1,0)
    header=tmp_path/'header.vcf';header.write_text(HEADER)
    body=tmp_path/'body.vcf';body.write_text(rows)
    overridden = hl.import_vcf(body, header_file=header)
    assert overridden.DP.collect() == [7, 9]
    assert overridden.GT.collect() == m.GT.collect()
    assert overridden.s.collect() == ['a', 'b']


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
    with pytest.raises(hl.utils.FatalError,match='invalid sample IDs'):
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


def test_export_keeps_physical_samples_paired_with_entries(tmp_path):
    mt = hl.range_matrix_table(2, 3)
    sample = hl.literal(['z', 'a', 'm'])
    calls = hl.literal([hl.Call([0, 0]), hl.Call([0, 1]), hl.Call([1, 1])], hl.tarray(hl.tcall))
    mt = mt.annotate_cols(sample=sample[mt.col_idx]).key_cols_by('sample')
    mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 10), alleles=['A', 'C'])
    mt = mt.key_rows_by('locus', 'alleles')
    mt = mt.annotate_entries(GT=calls[mt.col_idx])
    output = tmp_path / 'out.vcf'
    hl.export_vcf(mt, output)
    loaded = hl.import_vcf(output)
    actual = {(r.locus.position, r.s): r.GT for r in loaded.entries().collect()}
    assert actual == {(position, sample): call for position in [10, 11]
                      for sample, call in zip(['z', 'a', 'm'], [hl.Call([0, 0]), hl.Call([0, 1]), hl.Call([1, 1])])}


def test_export_info_format_missing_and_filtered_roundtrip(tmp_path):
    mt = hl.range_matrix_table(2, 2)
    mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 1), alleles=['A', 'T'],
                          info=hl.struct(flag=True, number=3, rates=[.2, .3], label='hello'),
                          filters=hl.set(['q10']), qual=17., rsid='rs1')
    mt = mt.key_rows_by('locus', 'alleles')
    mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by('s')
    mt = mt.annotate_entries(GT=hl.if_else(mt.col_idx == 0, hl.call(1, 0, phased=True), hl.missing(hl.tcall)),
                            DP=7, AD=[2, 5], score=hl.float32(.5))
    mt = mt.filter_entries((mt.row_idx != 1) | (mt.col_idx != 0))
    path = tmp_path / 'roundtrip.vcf'
    hl.export_vcf(mt, path, metadata={'info': {'label': {'Description': 'a, "quote" and \\slash'}}})
    result = hl.import_vcf(path, array_elements_required=False, entry_float_type=hl.tfloat32)
    assert [r.info for r in result.rows().collect()] == [hl.Struct(flag=True, number=3, rates=[.2, .3], label='hello')] * 2
    assert result.DP.collect() == [7, 7, None, 7]
    assert result.GT.collect() == [hl.Call([1, 0], True), None, None, None]
    assert result.AD.collect() == [[2, 5], [2, 5], None, [2, 5]]
    assert hl.get_vcf_metadata(path)['info']['label']['Description'] == 'a, "quote" and \\slash'


def test_export_failure_preserves_existing_destination(tmp_path):
    mt = hl.range_matrix_table(1, 1)
    mt = mt.annotate_rows(locus=hl.locus('1', 1), alleles=['A', 'C'])
    mt = mt.annotate_cols(s='a').key_cols_by('s').key_rows_by('locus', 'alleles')
    mt = mt.annotate_entries(label='bad:delimiter')
    path = tmp_path / 'existing.vcf'
    path.write_text('existing output')
    with pytest.raises((RuntimeError, ValueError), match='delimiter'):
        hl.export_vcf(mt, path)
    assert path.read_text() == 'existing output'


def test_projection_skips_unrequested_bad_fields_but_keeps_requested_errors(tmp_path):
    path = tmp_path / 'project.vcf'
    path.write_text(HEADER + '1\t1\t.\tA\tC\t.\tPASS\tAF=bad\tGT:DP\tbad:7\t0/1:9\n')
    mt = hl.import_vcf(path)
    assert mt.locus.collect()[0].position == 1
    assert mt.DP.collect() == [7, 9]
    rows = mt.rows()
    grouped = rows.group_by(contig=rows.locus.contig).aggregate(position=hl.agg.sum(rows.locus.position))
    assert grouped.collect() == [hl.Struct(contig='1', position=1)]
    assert rows.filter(rows.locus.position == 1).count() == 1
    preview = rows.key_by()
    preview = preview.select(position=preview.locus.position)
    assert preview.head(1).collect() == [hl.Struct(position=1)]
    with pytest.raises(RuntimeError, match='invalid character|genotype|Call'):
        mt.GT.collect()
    with pytest.raises(RuntimeError, match='parse|invalid'):
        mt.info.AF.collect()
    filtered = mt.filter_entries(mt.GT == hl.call(0, 1))
    with pytest.raises(RuntimeError, match='invalid character|genotype|Call'):
        filtered.DP.collect()


def test_nested_map_missingness_reads_computed_vcf_values(tmp_path):
    path = tmp_path / 'nested-missingness.vcf'
    path.write_text(HEADER + '1\t1\t.\tA\tC\t.\tPASS\t.\tGT:DP\t./.:7\t0/1:9\n')
    mt = hl.import_vcf(path)
    localized = mt.localize_entries('entries', 'columns').key_by()
    mapped_gt = localized.entries.map(lambda cell: cell.GT)
    missing = mapped_gt.map(lambda gt: hl.is_missing(gt))
    defined = mapped_gt.map(lambda gt: hl.is_defined(gt))
    row = localized.select(missing=missing, defined=defined).collect()[0]
    assert row.missing == [True, False]
    assert row.defined == [False, True]


def test_nested_map_missingness_preserves_gt_validity_only(tmp_path):
    header = HEADER.replace("\ta\tb\n", "\ta\tb\tc\td\te\n")
    path = tmp_path / 'gt-validity.vcf'
    path.write_text(header +
                    '1\t1\t.\tA\tC\t.\tPASS\t.\tDP:GT\t1:0\t1:1|0\t1:0/.\t1:./1\t1\n')
    mt = hl.import_vcf(path)
    localized = mt.localize_entries('entries', 'columns').key_by()
    mapped_gt = localized.entries.map(lambda cell: cell.GT)
    row = localized.select(
        missing=mapped_gt.map(lambda gt: hl.is_missing(gt)),
        defined=mapped_gt.map(lambda gt: hl.is_defined(gt)),
    ).collect()[0]
    assert row.missing == [False, False, True, True, True]
    assert row.defined == [True, True, False, False, False]


def test_nested_map_preserves_format_array_shape_without_values(tmp_path):
    path = tmp_path / 'ad-shape.vcf'
    path.write_text(HEADER +
                    '1\t1\t.\tA\tC\t.\tPASS\t.\tGT:AD\t0/1:2,.\t./.:.\n')
    mt = hl.import_vcf(path)
    localized = mt.localize_entries('entries', 'columns').key_by()
    row = localized.select(
        lengths=localized.entries.map(lambda cell: cell.AD.length()),
        constants=localized.entries.map(lambda cell: cell.AD.map(lambda _: 1)),
        child_defined=localized.entries.map(
            lambda cell: cell.AD.map(lambda value: hl.is_defined(value))
        ),
    ).collect()[0]
    assert row.lengths == [2, None]
    assert row.constants == [[1, 1], None]
    assert row.child_defined == [[True, False], None]


def test_many_format_layouts_decode_consistently(tmp_path):
    import itertools
    extra = ''.join(f'##FORMAT=<ID={name},Number=1,Type=Integer,Description="extra">\n' for name in ['X', 'Y', 'Z'])
    header = HEADER.replace('#CHROM', extra + '#CHROM')
    values = dict(GT='0/1', DP='7', AD='2,5', X='1', Y='2', Z='3')
    lines = []
    for position, names in enumerate(itertools.islice(itertools.permutations(values), 300), 1):
        sample = ':'.join(values[name] for name in names)
        lines.append(f'1\t{position}\t.\tA\tC\t.\tPASS\t.\t{":".join(names)}\t{sample}\t{sample}\n')
    path = tmp_path / 'layouts.vcf'
    path.write_text(header + ''.join(lines))
    result = hl.import_vcf(path)
    assert result.DP.collect() == [7] * 600
    assert result.GT.collect() == [hl.Call([0, 1])] * 600
    assert result.AD.collect() == [[2, 5]] * 600


def test_projection_retains_constant_callback_entry_shape(tmp_path):
    path = tmp_path / 'shape.vcf'
    path.write_text(HEADER + '1\t1\t.\tA\tC\t.\tPASS\t.\tGT:DP\t./.:7\t0/1:9\n')
    mt = hl.import_vcf(path)
    assert mt.select_entries(constant=1).constant.collect() == [1, 1]
    selected = mt.annotate_entries(constant=1).select_entries('constant')
    assert selected.constant.collect() == [1, 1]
    filtered = mt.filter_entries(mt.DP == 9).select_entries(constant=1)
    assert filtered.constant.collect() == [1]
    localized = mt.localize_entries('entries', 'columns').key_by()
    projected = localized.select(values=localized.entries.map(lambda _: 7),
                                 nested=localized.entries.map(lambda _: hl.struct(value=hl.struct(x=3))).map(lambda item: item.value))
    row = projected.collect()[0]
    assert row['values'] == [7, 7]
    assert row.nested == [hl.Struct(x=3), hl.Struct(x=3)]
    assert mt.filter_cols(False).select_entries(constant=1).entries().collect() == []


def test_projection_map_constant_index_then_field(tmp_path):
    path = tmp_path / 'index-shape.vcf'
    path.write_text(HEADER + '1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t.\t0/1\n')
    mt = hl.import_vcf(path)
    table = mt.localize_entries('entries', 'columns').key_by()
    table = table.select(mapped=table.entries.map(lambda _: hl.struct(constant=1)))
    table = table.select(value=table.mapped[0].constant)
    assert table.collect()[0].value == 1


@pytest.mark.parametrize('rows,samples', [(0, 0), (0, 2), (2, 0)])
def test_export_empty_axes(tmp_path, rows, samples):
    mt = hl.range_matrix_table(rows, samples)
    mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 1), alleles=['A', 'T'])
    mt = mt.key_rows_by('locus', 'alleles')
    mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by('s')
    path = tmp_path / 'empty.vcf'
    hl.export_vcf(mt, path)
    assert hl.import_vcf(path).count() == (rows, samples)


def test_export_haploid_unphased_and_missing_info(tmp_path):
    mt = hl.range_matrix_table(1, 2)
    mt = mt.annotate_rows(locus=hl.locus('1', 1), alleles=['A', 'T'],
                          info=hl.missing(hl.tstruct(flag=hl.tbool, label=hl.tstr)))
    mt = mt.key_rows_by('locus', 'alleles')
    mt = mt.annotate_cols(s=hl.str(mt.col_idx)).key_cols_by('s')
    calls = hl.literal([hl.Call([1]), hl.Call([1, 0])], hl.tarray(hl.tcall))
    mt = mt.annotate_entries(GT=calls[mt.col_idx])
    path = tmp_path / 'calls.vcf'
    hl.export_vcf(mt, path)
    loaded = hl.import_vcf(path)
    assert loaded.GT.collect() == [hl.Call([1]), hl.Call([0, 1])]
    assert loaded.info.flag.collect() == [False]
    assert loaded.info.label.collect() == [None]


def test_export_rejects_invalid_schema_and_sample_ids(tmp_path):
    mt = hl.range_matrix_table(1, 2)
    mt = mt.annotate_rows(locus=hl.locus('1', 1), alleles=['A', 'C'])
    mt = mt.annotate_cols(s='duplicate').key_cols_by('s')
    path = tmp_path / 'bad.vcf'
    with pytest.raises(ValueError, match='sample IDs'):
        hl.export_vcf(mt, path)
    mt = hl.range_matrix_table(1, 1)
    mt = mt.annotate_cols(s='sample').key_cols_by('s')
    bad = mt.annotate_rows(locus=hl.locus('1', 1), alleles='AC')
    with pytest.raises(ValueError, match='locus and alleles'):
        hl.export_vcf(bad, path)
    valid = mt.annotate_rows(locus=hl.locus('1', 1), alleles=['A', 'C'])
    with pytest.raises(hl.utils.FatalError, match='FORMAT field'):
        hl.export_vcf(valid.annotate_entries(flag=True), path)
    assert not path.exists()


def test_projection_shape_requests_cannot_alias_user_field_names(tmp_path):
    header = HEADER.replace('##INFO=<ID=FLAG', '##INFO=<ID=__vail_shape,Number=1,Type=Integer,Description="user name">\n##INFO=<ID=FLAG')
    header = header.replace('##FORMAT=<ID=GT', '##FORMAT=<ID=__vail_shape,Number=1,Type=Integer,Description="user name">\n##FORMAT=<ID=GT')
    path = tmp_path / 'user-name.vcf'
    path.write_text(header + '1\t1\t.\tA\tC\t.\tPASS\t__vail_shape=42\tGT:__vail_shape\t0/1:7\t0/0:9\n')
    mt = hl.import_vcf(path)
    assert mt.info['__vail_shape'].collect() == [42]
    assert mt.entry['__vail_shape'].collect() == [7, 9]
    selected = mt.select_entries(__vail_shape=mt.entry['__vail_shape'])
    assert selected.entry['__vail_shape'].collect() == [7, 9]


@pytest.mark.parametrize('gt',['65536','0/65536','./65536','70000|.','65536/.'])
def test_vcf_uint16_call_range_is_checked_even_when_partly_missing(tmp_path,gt):
    path=tmp_path/'overflow.vcf'
    path.write_text(HEADER+f'1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t{gt}\t0/0\n')
    with pytest.raises((ValueError,RuntimeError),match='65535|allele'):
        hl.import_vcf(path).entries().collect()


def test_vcf_uint16_call_boundary_roundtrips(tmp_path):
    path=tmp_path/'max.vcf'
    path.write_text(HEADER+'1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t65535|0\t65535/65535\n')
    assert hl.import_vcf(path).GT.collect()==[hl.Call([65535,0],True),hl.Call([65535,65535])]
