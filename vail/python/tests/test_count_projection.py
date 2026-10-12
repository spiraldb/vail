import pytest
import vail as hl


def test_count_skips_unused_malformed_csv_column(tmp_path):
    path = tmp_path / 'malformed.csv'
    path.write_text('good,bad\n1,invalid\n2,invalid\n')
    table = hl.read_csv(str(path), schema=hl.tstruct(good=hl.tint32, bad=hl.tint32))
    assert table.count() == 2
    assert table.select('good').count() == 2
    assert table.filter(table.good == 1).count() == 1
    assert [row.good for row in table.select('good').collect()] == [1, 2]
    with pytest.raises(RuntimeError, match='parsing'):
        table.collect()
    with pytest.raises(RuntimeError, match='parsing'):
        table.filter(table.bad == 1).count()
