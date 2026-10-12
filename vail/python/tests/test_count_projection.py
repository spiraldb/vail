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
        table._force_count()
    assert table.select('good')._force_count() == 2
    with pytest.raises(RuntimeError, match='parsing'):
        table.filter(table.bad == 1).count()


@pytest.mark.parametrize('rows,partitions', [(0, 1), (7, 3)])
def test_force_count_executes_full_output_even_with_known_count(rows, partitions):
    table = hl.range_table(rows, partitions)
    assert table._force_count() == rows
    assert table._force_count() == rows
    invalid = table.annotate(value=hl.int32(hl.str(table.idx) + 'invalid'))
    assert invalid.count() == rows
    if rows:
        with pytest.raises(RuntimeError):
            invalid._force_count()
    else:
        assert invalid._force_count() == 0


@pytest.mark.parametrize('wrapper', ['order_by', 'key_by', 'select_after_order'])
def test_count_skips_cardinality_preserving_ordering(tmp_path, wrapper):
    path = tmp_path / 'ordered-malformed.csv'
    path.write_text('good,bad\n2,invalid\n1,invalid\n')
    table = hl.read_csv(str(path), schema=hl.tstruct(good=hl.tint32, bad=hl.tint32))
    if wrapper == 'key_by':
        table = table.key_by('good')
    else:
        table = table.order_by(table.good)
        if wrapper == 'select_after_order':
            table = table.select('good', 'bad')
    assert table.count() == 2
    with pytest.raises(RuntimeError, match='parsing'):
        table._force_count()


def test_count_preserves_sort_before_order_sensitive_row_selection():
    table = hl.Table.parallelize([{'x': 3}, {'x': 1}, {'x': 2}])
    table = table.order_by(table.x).head(1)
    table = table.filter(table.x == 1)
    assert table.count() == 1
