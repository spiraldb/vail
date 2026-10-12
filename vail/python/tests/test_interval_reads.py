import pytest
import vail as hl


def test_interval_read_multiple_files_and_endpoint_semantics(tmp_path):
    table = hl.range_table(12, n_partitions=3)
    path = tmp_path / "table"
    table.write(path)

    intervals = [hl.Interval(2, 5, True, False), hl.Interval(5, 8, False, True)]
    read = hl.read_table(path, _intervals=intervals)
    assert [row.idx for row in read.collect()] == [2, 3, 4, 6, 7, 8]
    assert read._keys == table._keys


def test_adjacent_intervals_keep_separate_output_partitions(tmp_path):
    table = hl.range_table(20, n_partitions=4)
    path = tmp_path / "table"
    table.write(path)

    read = hl.read_table(path, _intervals=[hl.Interval(2, 7, True, False),
                                           hl.Interval(7, 12, True, False)])
    assert read.n_partitions() == 2
    assert [row.idx for row in read.collect()] == list(range(2, 12))


def test_create_row_uids_option_is_accepted(tmp_path):
    table = hl.range_table(12, n_partitions=3)
    path = tmp_path / "table"
    table.write(path)

    read = hl.read_table(path, _create_row_uids=True)
    assert read.collect() == table.collect()


def test_indexed_read_intervals_and_filter_partition_selection(tmp_path):
    table = hl.range_table(2000, n_partitions=10)
    path = tmp_path / "table"
    table.write(path)
    intervals = [hl.Interval(150, 250, True, False),
                 hl.Interval(250, 500, True, False)]

    indexed = hl.read_table(path, _intervals=intervals)
    filtered = hl.read_table(path, _intervals=intervals, _filter_intervals=True)
    expected = list(range(150, 500))
    assert [row.idx for row in indexed.collect()] == expected
    assert indexed.n_partitions() == 2
    assert [row.idx for row in filtered.collect()] == expected
    assert filtered.n_partitions() == 3


@pytest.mark.parametrize("n, expected", [(10, 10), (0, 3)])
def test_read_table_partition_selection(tmp_path, n, expected):
    table = hl.range_table(100, n_partitions=3)
    path = tmp_path / "table"
    table.write(path)

    read = hl.read_table(path, _n_partitions=n)
    assert read.n_partitions() == expected
    assert [row.idx for row in read.collect()] == list(range(100))


def test_read_table_partition_selection_with_missing_key(tmp_path):
    table = hl.range_table(100, n_partitions=3).key_by(idx=hl.missing(hl.tint32))
    path = tmp_path / "table"
    table.write(path)

    read = hl.read_table(path, _n_partitions=10)
    assert read.n_partitions() == 1
    assert len(read.collect()) == 100


def test_interval_read_empty_and_overlapping_ranges(tmp_path):
    table = hl.range_table(10, n_partitions=3)
    path = tmp_path / "table"
    table.write(path)

    assert hl.read_table(path, _intervals=[]).collect() == []
    ranges = [hl.Interval(2, 6), hl.Interval(4, 8)]
    read = hl.read_table(path, _intervals=ranges)
    assert [row.idx for row in read.collect()] == list(range(2, 8))


def test_filter_intervals_selects_source_partitions(tmp_path):
    table = hl.range_table(100, n_partitions=10)
    path = tmp_path / "table"
    table.write(path)
    intervals = [hl.Interval(5, 10), hl.Interval(12, 13), hl.Interval(15, 17)]

    ordinary = hl.read_table(path, _intervals=intervals)
    filtered = hl.read_table(path, _intervals=intervals, _filter_intervals=True)
    assert filtered.collect() == ordinary.collect()
    # Hail's _filter_intervals retains source partitions overlapping the
    # requested ranges instead of creating a partition per requested interval.
    assert filtered.n_partitions() <= ordinary.n_partitions()


def test_interval_read_composite_key_prefix(tmp_path):
    table = hl.Table.parallelize(
        [{"a": a, "b": b, "value": f"{a}:{b}"} for a in range(3) for b in range(4)],
        schema=hl.tstruct(a=hl.tint32, b=hl.tint32, value=hl.tstr),
        key=("a", "b"), n_partitions=3,
    )
    path = tmp_path / "table"
    table.write(path)
    prefix_ranges = [hl.Interval(hl.Struct(a=1), hl.Struct(a=2))]
    read = hl.read_table(path, _intervals=prefix_ranges)
    assert [(row.a, row.b) for row in read.collect()] == [(1, b) for b in range(4)]


@pytest.mark.parametrize("kwargs, exception", [
    ({"_filter_intervals": True}, ValueError),
    ({"_intervals": [1]}, TypeError),
    ({"_create_row_uids": 1}, TypeError),
])
def test_interval_read_rejects_invalid_options(tmp_path, kwargs, exception):
    table = hl.range_table(2)
    path = tmp_path / "table"
    table.write(path)
    with pytest.raises(exception):
        hl.read_table(path, **kwargs)
