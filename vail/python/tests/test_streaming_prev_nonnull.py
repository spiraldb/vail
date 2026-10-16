"""Previous-defined scans stream nested Arrow values in observable Table order."""
import vail as hl
import pytest


@pytest.mark.parametrize('partitions', [1, 3, 12])
def test_previous_nested_values_and_missing_partitions(partitions):
    dtype = hl.tstruct(s=hl.tstr, a=hl.tarray(hl.tint32), empty=hl.tstruct())
    values = [None, None, dict(s='a', a=[1, None], empty={}), None,
              dict(s='b', a=[], empty={}), None, None,
              dict(s=None, a=None, empty={}), None]
    table = hl.Table.parallelize([dict(i=i, v=v) for i, v in enumerate(values)],
                                schema=hl.tstruct(i=hl.tint32, v=dtype), n_partitions=partitions)
    scanned = table.annotate(previous=hl.scan.prev_nonnull(table.v))
    previous = None
    expected = []
    for value in table.collect():
        expected.append(previous)
        if value.v is not None:
            previous = value.v
    assert [row.previous for row in scanned.collect()] == expected
    assert [row.previous for row in scanned.collect()] == expected
    assert scanned.previous.dtype == dtype
    assert scanned.n_partitions() == partitions
    assert [row.previous for row in scanned.filter(scanned.i >= 6).collect()] == expected[6:]


def test_previous_across_many_batches_and_selected_rows():
    table = hl.range_table(25003, n_partitions=3)
    table = table.annotate(v=hl.or_missing(table.idx % 9001 == 0,
                                         hl.struct(i=table.idx, a=hl.range(table.idx % 7))))
    rows = table.annotate(previous=hl.scan.prev_nonnull(table.v)).collect()
    previous = None
    for row in rows:
        assert row.previous == previous
        if row.v is not None:
            previous = row.v
    filtered = table.filter(table.idx % 2 == 0)
    rows = filtered.annotate(previous=hl.scan.prev_nonnull(filtered.row)).collect()
    assert rows[0].previous is None
    assert [row.previous.idx for row in rows[1:]] == [row.idx for row in rows[:-1]]


def test_previous_multiple_types_and_current_row_expressions():
    table = hl.range_table(17, n_partitions=5)
    table = table.annotate(a=hl.range(table.idx % 4))
    result = table.annotate(previous=hl.scan.prev_nonnull(table.row),
                            array=hl.scan.prev_nonnull(table.a),
                            text=hl.scan.prev_nonnull(hl.str(table.idx)),
                            shifted=hl.or_else(hl.scan.prev_nonnull(table.idx), -1) + table.idx)
    rows = result.collect()
    assert rows[0].previous is None
    assert rows[0].array is None
    assert rows[0].text is None
    for i, row in enumerate(rows):
        assert row.shifted == i + i - 1
        if i:
            assert row.previous.idx == i - 1
            assert row.array == list(range((i - 1) % 4))
            assert row.text == str(i - 1)


def test_previous_empty_all_missing_and_empty_struct():
    for n in [0, 11]:
        table = hl.range_table(n, n_partitions=5)
        rows = table.annotate(previous=hl.scan.prev_nonnull(hl.missing(hl.tarray(hl.tint32)))).collect()
        assert [row.previous for row in rows] == [None] * n
    table = hl.range_table(5, n_partitions=8)
    assert [row.previous for row in table.annotate(previous=hl.scan.prev_nonnull(hl.struct())).collect()] == [None] + [hl.Struct()] * 4


def test_previous_follows_key_order_and_general_fold_keeps_initializer():
    table = hl.Table.parallelize([dict(k=i) for i in [4, 1, 3, 2]], n_partitions=3).key_by('k')
    rows = table.annotate(previous=hl.scan.prev_nonnull(table.row)).collect()
    assert [None if row.previous is None else row.previous.k for row in rows] == [None, 1, 2, 3]
    # A non-null initializer changes the first prefix and must not be optimized
    # into the null-identity scan merely because the sequence looks similar.
    result = table.annotate(previous=hl.scan.fold(99,
        lambda state: hl.if_else(hl.is_defined(table.k), table.k, state),
        lambda left, right: hl.if_else(hl.is_defined(right), right, left)))
    assert [row.previous for row in result.collect()] == [99, 1, 2, 3]


def test_similar_state_dependent_fold_is_not_a_previous_value_scan():
    table = hl.range_table(6, n_partitions=1)
    def step(state):
        value = hl.or_else(state, 0) + table.idx
        return hl.if_else(hl.is_defined(value), value, state)
    scanned = table.annotate(value=hl.scan.fold(hl.missing(hl.tint32), step,
        lambda left, right: hl.if_else(hl.is_defined(right), right, left)))
    assert [row.value for row in scanned.collect()] == [None, 0, 1, 3, 6, 10]


def test_reading_first_partition_does_not_evaluate_future_failures():
    table = hl.range_table(12, n_partitions=3)
    table = table.annotate(v=hl.if_else(table.idx < 4, table.idx,
                                      hl.int32(hl.str(table.idx) + 'invalid')))
    scanned = table.annotate(previous=hl.scan.prev_nonnull(table.v))
    assert [(row.idx, row.previous) for row in scanned.head(3).collect()] == [(0, None), (1, 0), (2, 1)]


def test_second_partition_does_not_evaluate_future_partition():
    table = hl.range_table(12, n_partitions=3)
    table = table.annotate(v=hl.if_else(table.idx < 8, table.idx,
                                      hl.int32(hl.str(table.idx) + 'invalid')))
    scanned = table.annotate(previous=hl.scan.prev_nonnull(table.v))
    assert [row.previous for row in scanned.head(7).collect()] == [None, 0, 1, 2, 3, 4, 5]


def test_many_partition_dependencies_and_late_partition_demand():
    table = hl.range_table(1100, n_partitions=1100)
    scanned = table.annotate(previous=hl.scan.prev_nonnull(table.idx))
    assert [row.previous for row in scanned.collect()] == [None] + list(range(1099))


def test_previous_after_reasserting_an_existing_nonempty_key():
    table = hl.range_table(12, n_partitions=3).key_by('idx')
    rows = table.annotate(previous=hl.scan.prev_nonnull(table.idx)).collect()
    assert [row.previous for row in rows] == [None] + list(range(11))


def test_previous_binds_globals_once_per_action():
    table = hl.range_table(9, n_partitions=3).annotate_globals(offset=7)
    rows = table.annotate(previous=hl.scan.prev_nonnull(hl.struct(v=table.idx + table.offset))).collect()
    assert [None if row.previous is None else row.previous.v for row in rows] == [None] + list(range(7, 15))


def test_previous_over_native_read_partitions(tmp_path):
    table = hl.range_table(19, n_partitions=5)
    table = table.annotate(v=hl.or_missing(table.idx % 4 == 1,
                                         hl.struct(i=table.idx, a=hl.range(table.idx % 3))))
    path = str(tmp_path / 'previous.vt')
    table.write(path)
    restored = hl.read_table(path)
    previous = None
    for row in restored.annotate(previous=hl.scan.prev_nonnull(restored.v)).collect():
        assert row.previous == previous
        if row.v is not None:
            previous = row.v
