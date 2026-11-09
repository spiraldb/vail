import pytest

import vail as hl


@pytest.mark.parametrize('descending', [False, True])
def test_sort_keeps_equal_keys_in_encounter_order_across_partitions(descending):
    keys = [None, 2, 0, 2, 1, None, 0] * 19
    rows = [{'k': key, 'position': i, 'payload': [i, None, -i]}
            for i, key in enumerate(keys)]
    table = hl.Table.parallelize(
        rows, schema=hl.tstruct(k=hl.tint32, position=hl.tint64,
                               payload=hl.tarray(hl.tint64)), n_partitions=11,
    )
    expected = sorted(rows, key=lambda r: (
        r['k'] is None,
        0 if r['k'] is None else (-r['k'] if descending else r['k']),
    ))
    result = table.order_by(hl.desc(table.k) if descending else table.k)
    for actual in (result.collect(), result.collect()):
        assert [(r.k, r.position, r.payload) for r in actual] == [
            (r['k'], r['position'], r['payload']) for r in expected]
    assert [r.position for r in result.take(17)] == [r['position'] for r in expected[:17]]


def test_sort_stability_survives_union_filter_explode_and_resort():
    left = hl.range_table(31, n_partitions=7).select(values=hl.array([2, 1, 2]))
    right = hl.range_table(17, n_partitions=5).select(values=hl.array([1, 2]))
    source = left.union(right).explode('values')
    source = source.add_index('position')
    source = source.filter(source.values > 0)
    encounter = source.collect()
    sorted_table = source.key_by('values')
    expected = sorted(encounter, key=lambda row: row['values'])
    assert sorted_table.collect() == expected
    # A second sort on an equal key must preserve the first sort's sequence.
    tied = sorted_table.key_by().annotate(tie=0).key_by('tie')
    assert [(r['values'], r.position) for r in tied.collect()] == [
        (r['values'], r.position) for r in expected]


def test_sort_private_names_do_not_hide_user_fields():
    source = hl.Table.parallelize([
        {'__vail_partition': 4, '__vail_sort_ordinal': 7},
        {'__vail_partition': 1, '__vail_sort_ordinal': 8},
        {'__vail_partition': 4, '__vail_sort_ordinal': 9},
    ], n_partitions=5)
    rows = source.key_by('__vail_partition').collect()
    assert [(r['__vail_partition'], r['__vail_sort_ordinal']) for r in rows] == [
        (1, 8), (4, 7), (4, 9)]
