"""Public BED import assertions from UCSC coordinates and Hail schema."""
import pytest
import vail as hl


def test_bed_headers_recoding_and_optional_fields(tmp_path):
    path = tmp_path / 'regions.bed'
    path.write_text('browser position chr1:1-10\ntrack name="regions"\n\nchr1 0 10 first 100 +\nchr1 10 11 second 0 -\n')
    t = hl.import_bed(path, reference_genome='GRCh37', contig_recoding={'chr1': '1'})
    assert list(t.key) == ['interval']
    assert t.row.dtype == hl.tstruct(interval=hl.tinterval(hl.tlocus('GRCh37')), target=hl.tstr)
    rows = t.collect()
    assert [r.target for r in rows] == ['first', 'second']
    assert [(r.interval.start.position, r.interval.end.position) for r in rows] == [(1, 11), (11, 12)]
    assert all(r.interval.includes_start and not r.interval.includes_end for r in rows)


def test_bed_no_reference_and_three_fields(tmp_path):
    path = tmp_path / 'regions.bed'
    path.write_text('scaffold 100 105\n')
    t = hl.import_bed(path, reference_genome=None)
    assert list(t.row.dtype) == ['interval']
    r = t.collect()[0].interval
    assert r.start == hl.Struct(contig='scaffold', position=101)
    assert r.end == hl.Struct(contig='scaffold', position=106)


def test_bed_invalid_and_reference_end(tmp_path):
    path = tmp_path / 'regions.bed'
    length = hl.get_reference('GRCh37').lengths['1']
    path.write_text(f'1 0 {length}\nunknown 0 2\n1 10 1\n1 -2 10\n')
    rows = hl.import_bed(path, reference_genome='GRCh37', skip_invalid_intervals=True).collect()
    assert len(rows) == 1
    assert rows[0].interval.end.position == length
    assert rows[0].interval.includes_end
    with pytest.raises((RuntimeError, ValueError), match='Invalid locus interval'):
        hl.import_bed(path, reference_genome='GRCh37').collect()


def test_bed_too_few_fields(tmp_path):
    path = tmp_path / 'bad.bed'
    path.write_text('1 2\n')
    with pytest.raises(hl.utils.FatalError, match='too few fields'):
        hl.import_bed(path)


def test_bed_zero_length_interval(tmp_path):
    path = tmp_path / 'empty.bed'
    path.write_text('20 5 5 insertion\n')
    row = hl.import_bed(path, reference_genome='GRCh37').collect()[0]
    assert row.interval.start.position == 5
    assert row.interval.end.position == 6
    assert not row.interval.includes_start and not row.interval.includes_end
