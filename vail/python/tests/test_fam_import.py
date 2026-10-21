"""Public FAM import behavior, checked against the pinned Hail parser contract."""
import pytest
import vail as hl


def test_fam_case_control_fields_and_missing(tmp_path):
    path = tmp_path / 'samples.fam'
    path.write_text('f b dad mom 2 2\n0 a 0 0 1 1\n0 c 0 0 0 nonnumeric\n0 d 0 0 -9 -9\n')
    table = hl.import_fam(path)
    assert table.row.dtype == hl.tstruct(id=hl.tstr, fam_id=hl.tstr, pat_id=hl.tstr,
                                      mat_id=hl.tstr, is_female=hl.tbool, is_case=hl.tbool)
    assert list(table.key) == ['id']
    assert table.collect() == [
        hl.Struct(id='a', fam_id=None, pat_id=None, mat_id=None, is_female=False, is_case=False),
        hl.Struct(id='b', fam_id='f', pat_id='dad', mat_id='mom', is_female=True, is_case=True),
        hl.Struct(id='c', fam_id=None, pat_id=None, mat_id=None, is_female=None, is_case=None),
        hl.Struct(id='d', fam_id=None, pat_id=None, mat_id=None, is_female=None, is_case=None),
    ]


def test_fam_quantitative_and_custom_delimiter(tmp_path):
    path = tmp_path / 'samples.fam'
    path.write_text('0,a,0,0,NA,-9\n0,b,0,0,2,1.25e-2\n0,c,0,0,1,NA\n')
    table = hl.import_fam(path, quant_pheno=True, delimiter=',')
    assert [(r.id, r.quant_pheno) for r in table.collect()] == [('a', -9.), ('b', .0125), ('c', None)]
    path.write_text('0\ta\t0\t0\tX\tX\n')
    row = hl.import_fam(path, delimiter=r'\\t', missing='X').collect()[0]
    assert row.is_female is None and row.is_case is None


@pytest.mark.parametrize('record,quant,message', [
    ('0 a 0 0 3 1', False, 'Invalid sex'),
    ('0 a 0 0 1 3', False, 'Invalid case-control phenotype'),
    ('0 a 0 0 1 nan', True, 'Invalid quantitative phenotype'),
    ('0 a 0 0 1 x', True, 'Invalid quantitative phenotype'),
    ('0 a 0 0 1', False, 'fields'),
    ('0 a 0 0 1 1 extra', False, 'fields'),
])
def test_fam_rejects_invalid_records(tmp_path, record, quant, message):
    path = tmp_path / 'invalid.fam'
    path.write_text(record + '\n')
    with pytest.raises((RuntimeError, ValueError), match=message):
        hl.import_fam(path, quant_pheno=quant).collect()


@pytest.mark.parametrize('quant_pheno', [False, True])
def test_fam_rejects_empty_file(tmp_path, quant_pheno):
    path = tmp_path / 'empty.fam'
    path.write_text('')
    with pytest.raises(hl.utils.FatalError, match='^Empty FAM file$'):
        hl.import_fam(path, quant_pheno=quant_pheno)
