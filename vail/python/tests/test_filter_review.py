"""Integration checks added during review of the native filter implementation."""
import vail as hl


def test_filter_count_follows_operation_order_across_logical_partitions():
    rows = [hl.Struct(i=i, p=None if i % 4 == 0 else i % 2 == 0) for i in range(19)]
    table = hl.Table.parallelize(rows, schema=hl.tstruct(i=hl.tint32, p=hl.tbool), n_partitions=7)
    for keep in (True, False):
        expected = [row.i for row in rows if row.p is keep]
        filtered = table.filter(table.p, keep=keep)
        assert filtered.i.collect() == expected
        assert filtered.count() == filtered._force_count() == len(expected)
        assert filtered.head(3).i.collect() == expected[:3]
        assert filtered.tail(3).i.collect() == expected[-3:]
        assert filtered.head(3).count() == min(3, len(expected))
        assert filtered.tail(3).count() == min(3, len(expected))
        prefix = table.head(3)
        assert prefix.filter(prefix.p, keep=keep).i.collect() == [row.i for row in rows[:3] if row.p is keep]


def test_filter_after_zero_column_projection_preserves_cardinality():
    table = hl.range_table(8, n_partitions=3).key_by().select()
    for keep in (True, False):
        for predicate in (True, False, hl.missing(hl.tbool)):
            filtered = table.filter(predicate, keep=keep)
            expected = 8 if predicate is keep else 0
            assert filtered.count() == filtered._force_count() == expected
            assert filtered.collect() == [hl.Struct()] * expected
            assert filtered.row.dtype == hl.tstruct()


def test_filter_accepts_hail_expr_keyword():
    table = hl.range_table(5)
    assert table.filter(expr=table.idx > 2, keep=True).idx.collect() == [3, 4]
