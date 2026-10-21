"""Public blackbox checks of Picard coordinates and Hail interval extensions."""
import pytest
import vail as hl
from vail.interval_import import import_locus_intervals, locus_interval, parse_locus_interval


def write(tmp_path, text):
    path = tmp_path / 'intervals.list'
    path.write_text(text)
    return str(path)


def test_picard_headers_targets_and_singletons(tmp_path):
    table = import_locus_intervals(write(tmp_path, '@HD\tVN:1.0\n@SQ\tSN:1\tLN:249250621\n1\t2\t2\t-\tone\n1\t8\t9\t+\ttwo\n'))
    rows = table.collect()
    assert list(table.key.dtype) == ['interval']
    assert [r.target for r in rows] == ['one', 'two']
    assert rows[0].interval.start.position == rows[0].interval.end.position == 2
    assert rows[0].interval.includes_start and rows[0].interval.includes_end


def test_reference_recoding_and_invalid_filter(tmp_path):
    path = write(tmp_path, '1\t1\t3\nmissing\t1\t3\n')
    table = import_locus_intervals(path, reference_genome='GRCh38', contig_recoding={'1': 'chr1'}, skip_invalid_intervals=True)
    assert table.count() == 1
    assert table.collect()[0].interval.start.contig == 'chr1'
    with pytest.raises(Exception, match='Invalid locus interval'):
        import_locus_intervals(path, reference_genome='GRCh38').collect()


def test_struct_points_and_one_column(tmp_path):
    path = write(tmp_path, 'arbitrary:12-15\n')
    row = import_locus_intervals(path, reference_genome=None).collect()[0]
    assert row.interval.start.contig == 'arbitrary'
    assert row.interval.start.position == 12
    assert row.interval.includes_end
    assert row.interval.point_type == hl.tstruct(contig=hl.tstr, position=hl.tint32)


@pytest.mark.parametrize('text,start,end,inc_start,inc_end', [
    ('1:1K-1.5K', 1000, 1500, True, False),
    ('(1:0-5]', 1, 5, True, True),
    ('1', 1, 249250621, True, True),
    ('[1:start-end]', 1, 249250621, True, True),
])
def test_interval_parser(text, start, end, inc_start, inc_end):
    value = hl.eval(parse_locus_interval(text))
    assert (value.start.position, value.end.position, value.includes_start, value.includes_end) == (start, end, inc_start, inc_end)


def test_cross_contig_and_sentinel_end():
    value = hl.eval(parse_locus_interval('1:5-2:10'))
    assert (value.start.contig, value.end.contig) == ('1', '2')
    value = hl.eval(locus_interval('1', 0, 249250622, False, False))
    assert (value.start.position, value.end.position, value.includes_start, value.includes_end) == (1, 249250621, True, True)
    assert hl.eval(locus_interval('not-a-contig', 1, 2, invalid_missing=True)) is None


def test_one_column_reference_export_bounds(tmp_path):
    table = import_locus_intervals(write(tmp_path, '[1:2-1:2]\n'))
    value = table.collect()[0].interval
    assert value.includes_end and value.end.position == 2


def test_interval_export_roundtrip_and_missing(tmp_path):
    path = tmp_path / 'exported.txt'
    table = hl.Table.parallelize([
        {'interval': hl.eval(locus_interval('1', 1, 5, True, True))},
        {'interval': None},
    ], schema=hl.tstruct(interval=hl.tinterval(hl.tlocus('GRCh37'))))
    table.export(str(path), header=False)
    assert path.read_text() == '[1:1-1:5]\nNA\n'
    path.write_text('[1:1-1:5]\n')
    assert import_locus_intervals(str(path)).collect()[0].interval == table.collect()[0].interval
