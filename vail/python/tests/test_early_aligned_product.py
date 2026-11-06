"""Entry updates around row expansion preserve values, masks and lexical captures."""
import math
import os
import pytest
import vail as hl


@pytest.fixture(params=['ordinary', 'default', 'early-product'])
def early_product(request, monkeypatch):
    for name in list(os.environ):
        if name.startswith('VAIL_EXPERIMENT_'):
            monkeypatch.delenv(name)
    for flag in ['VAIL_DISABLE_EARLY_ALIGNED_PRODUCT', 'VAIL_DISABLE_BOUNDED_PREFETCH',
                 'VAIL_DISABLE_COUNT_SUBQUERY_MERGE']:
        monkeypatch.delenv(flag, raising=False)
    if request.param == 'ordinary':
        monkeypatch.setenv('VAIL_DISABLE_EARLY_ALIGNED_PRODUCT', '1')
        monkeypatch.setenv('VAIL_DISABLE_BOUNDED_PREFETCH', '1')
        monkeypatch.setenv('VAIL_DISABLE_COUNT_SUBQUERY_MERGE', '1')
    for flag in ['VAIL_EXPERIMENT_EARLY_ALIGNED_PRODUCT',
                 'VAIL_EXPERIMENT_FUSED_PRODUCER',
                 'VAIL_EXPERIMENT_PROJECTED_PRODUCER']:
        if request.param == 'early-product':
            monkeypatch.setenv(flag, '1')


def source(n_cols=3):
    mt = hl.range_matrix_table(4, n_cols, n_partitions=2)
    choices = hl.literal([[2, 0, 2], [], None, [None, 4]],
                         hl.tarray(hl.tarray(hl.tint32)))
    mt = mt.annotate_rows(choices=choices[mt.row_idx], bias=hl.int32(10 * mt.row_idx))
    mt = mt.annotate_entries(
        base=hl.int32(10 * mt.row_idx + mt.col_idx),
        payload=hl.or_missing(mt.col_idx != 1, hl.array([hl.int32(mt.row_idx), 9])),
        call_value=hl.call(1, 0, phased=True))
    # The row capture makes this an aligned entry update, rather than an
    # ordinary entry-only list map.
    return mt.annotate_entries(prepared=mt.base + mt.bias)


def expanded(mt):
    return mt.key_rows_by().explode_rows('choices')


def test_duplicates_missing_choices_and_output_order(early_product):
    mt = expanded(source())
    mt = mt.select_entries(value=mt.prepared + mt.choices,
                           payload=mt.payload, call_value=mt.call_value)
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [(r.row_idx, r.choices) for r in rows] == [
        (0, 2), (0, 0), (0, 2), (3, None), (3, 4)]
    for row in rows:
        for column, entry in enumerate(row.entries):
            assert entry.value == (None if row.choices is None else
                                   20 * row.row_idx + column + row.choices)
            assert entry.payload == (None if column == 1 else [row.row_idx, 9])
            assert entry.call_value == hl.Call((1, 0), phased=True)
    table = mt.localize_entries('entries', 'columns')
    assert table.to_arrow().equals(table.to_arrow(), check_metadata=True)


def test_filtered_entries_and_metadata_only_branch(early_product):
    mt = source()
    mt = mt.filter_entries(mt.col_idx != 1)
    mt = expanded(mt)
    mt = mt.filter_rows((mt.row_idx + mt.choices) % 2 == 0)
    mt = mt.select_entries(value=mt.prepared + mt.choices)
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [(r.row_idx, r.choices) for r in rows] == [(0, 2), (0, 0), (0, 2)]
    for row in rows:
        assert row.entries[1] is None
        assert [row.entries[c].value for c in [0, 2]] == [row.choices, row.choices + 2]


def test_empty_entry_lists_retain_expanded_rows(early_product):
    mt = expanded(source(n_cols=0))
    mt = mt.select_entries(value=mt.prepared + mt.choices)
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [(r.row_idx, r.choices) for r in rows] == [
        (0, 2), (0, 0), (0, 2), (3, None), (3, 4)]
    assert [r.entries for r in rows] == [[], [], [], [], []]


def test_shared_producer_and_external_globals(early_product):
    mt = source().annotate_globals(offset=100)
    external = hl.range_table(0).annotate_globals(offset=7).index_globals()
    mt = mt.annotate_entries(prepared=mt.prepared + mt.index_globals().offset + external.offset)
    before = mt.localize_entries('entries', 'columns')
    out = expanded(mt)
    out = out.select_entries(value=out.prepared + out.choices)
    result = out.localize_entries('entries', 'columns')
    expected = before.to_arrow()
    rows = result.collect()
    for row in rows:
        assert [e.value for e in row.entries] == [
            None if row.choices is None else 20 * row.row_idx + c + 107 + row.choices
            for c in range(3)]
    assert before.to_arrow().equals(expected, check_metadata=True)
    assert result.to_arrow().equals(result.to_arrow(), check_metadata=True)


def test_explicit_error_stays_lazy_inside_entry_callback(early_product):
    mt = expanded(source())
    mt = mt.select_entries(value=hl.case().when(
        mt.choices >= 0, mt.prepared + mt.choices).or_error('negative choice was demanded'))
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [r.entries[0].value for r in rows] == [2, 0, 2, None, 64]
    bad = source()
    bad = bad.annotate_rows(choices=hl.array([-1]))
    bad = expanded(bad)
    bad = bad.select_entries(value=hl.case().when(
        bad.choices >= 0, bad.prepared + bad.choices).or_error('negative choice was demanded'))
    with pytest.raises(Exception, match='negative choice was demanded'):
        bad.localize_entries('entries', 'columns').collect()


def test_random_producer_repeats_original_values_after_expansion(early_product):
    mt = source().annotate_entries(random_value=hl.rand_unif(seed=731))
    before = mt.localize_entries('entries', 'columns').collect()
    out = expanded(mt)
    out = out.select_entries(random_value=out.random_value,
                              value=out.prepared + out.choices)
    for row in out.localize_entries('entries', 'columns').collect():
        assert [e.random_value for e in row.entries] == [
            e.random_value for e in before[row.row_idx].entries]


def test_list_consumers_across_row_expansion(early_product):
    mt = expanded(source())
    mt = mt.select_entries(value=mt.payload[0] + mt.choices,
                           mapped=mt.payload.map(lambda value: value + mt.choices))
    rows = mt.localize_entries('entries', 'columns').collect()
    assert [(r.row_idx, r.choices) for r in rows] == [
        (0, 2), (0, 0), (0, 2), (3, None), (3, 4)]
    for row in rows:
        for column, entry in enumerate(row.entries):
            missing = column == 1 or row.choices is None
            assert entry.value == (None if missing else row.row_idx + row.choices)
            assert entry.mapped == (None if column == 1 else [
                None if row.choices is None else value + row.choices
                for value in [row.row_idx, 9]])


def test_signed_zero_row_captures_and_call_metadata(early_product):
    import math

    mt = source()
    mt = mt.annotate_rows(zero=hl.if_else(mt.row_idx % 2 == 0, 0.0, -0.0))
    mt = expanded(mt)
    mt = mt.select_entries(
        scalar=mt.zero,
        mapped=mt.payload.map(lambda value: hl.float64(value) * mt.zero),
        call_value=mt.call_value,
    )
    table = mt.localize_entries('entries', 'columns')
    for row in table.collect():
        sign = 1.0 if row.row_idx % 2 == 0 else -1.0
        for column, entry in enumerate(row.entries):
            assert math.copysign(1.0, entry.scalar) == sign
            if column == 1:
                assert entry.mapped is None
            else:
                assert len(entry.mapped) == 2
                assert all(value == 0.0 and math.copysign(1.0, value) == sign
                           for value in entry.mapped)
            assert entry.call_value == hl.Call((1, 0), phased=True)
    # The public Arrow round trip must retain the logical Call type.
    restored = hl.Table.parallelize(table.to_arrow())
    assert restored.entries.dtype.element_type['call_value'] == hl.tcall
    assert [row.entries for row in restored.collect()] == [row.entries for row in table.collect()]


def test_error_in_producer_of_empty_choice_row_is_not_skipped(early_product):
    mt = source()
    # Row 1 has no choices and therefore contributes no exploded rows. An
    # explicit error in its original entry update remains an effect barrier.
    mt = mt.annotate_entries(checked=hl.case().when(
        mt.row_idx != 1, mt.prepared + mt.bias).or_error(
            'producer error before empty row expansion'))
    out = expanded(mt)
    out = out.select_entries(value=out.checked + out.choices)
    with pytest.raises(Exception, match='producer error before empty row expansion'):
        out.localize_entries('entries', 'columns').collect()


def test_empty_filtered_branch_keeps_shared_producer_and_null_globals(early_product):
    mt = source().annotate_globals(extra=7, absent=hl.missing(hl.tint32))
    mt = mt.annotate_entries(
        prepared=mt.prepared + mt.index_globals().extra,
        absent_value=mt.index_globals().absent)
    before = mt.localize_entries('entries', 'columns')
    expected_before = before.to_arrow()

    empty = expanded(mt)
    empty = empty.filter_rows(empty.choices < 0)
    empty = empty.select_entries(value=empty.prepared + empty.choices,
                                  absent_value=empty.absent_value + empty.choices)
    assert empty.localize_entries('entries', 'columns').collect() == []

    out = expanded(mt)
    out = out.select_entries(value=out.prepared + out.choices,
                              absent_value=out.absent_value + out.choices)
    rows = out.localize_entries('entries', 'columns').collect()
    assert [(row.row_idx, row.choices) for row in rows] == [
        (0, 2), (0, 0), (0, 2), (3, None), (3, 4)]
    for row in rows:
        assert [entry.value for entry in row.entries] == [
            None if row.choices is None else 20 * row.row_idx + c + 7 + row.choices
            for c in range(3)]
        assert [entry.absent_value for entry in row.entries] == [None, None, None]
    assert before.to_arrow().equals(expected_before, check_metadata=True)


def test_full_and_prefix_actions_share_source_without_changing_values(early_product):
    mt = source()
    original = mt.localize_entries('entries', 'columns')
    before = original.to_arrow()
    out = expanded(mt)
    out = out.select_entries(value=out.prepared + out.choices)
    table = out.localize_entries('entries', 'columns')

    expected = table.to_arrow()
    assert table.head(2).to_arrow().equals(expected.slice(0, 2), check_metadata=True)
    assert table._force_count() == 5
    assert table.head(1).collect()[0].entries[0].value == 2
    assert table.to_arrow().equals(expected, check_metadata=True)
    assert original.to_arrow().equals(before, check_metadata=True)


@pytest.mark.parametrize('operation', ['sum', 'product', 'min', 'max'])
def test_small_numeric_reductions_keep_list_and_entry_missingness(early_product, operation):
    mt = expanded(source())
    mt = mt.filter_entries(mt.col_idx != 1)
    reduce = getattr(hl, operation)
    out = mt.select_entries(
        value=reduce(hl.array([mt.prepared, mt.choices])),
        empty=reduce(hl.empty_array(hl.tint32)),
        missing=reduce(hl.array([hl.missing(hl.tint32)])),
    )
    identity = {'sum': 0, 'product': 1, 'min': None, 'max': None}[operation]
    for row in out.localize_entries('entries', 'columns').collect():
        assert row.entries[1] is None
        for column in [0, 2]:
            values = [20 * row.row_idx + column]
            if row.choices is not None:
                values.append(row.choices)
            expected = {'sum': sum(values), 'product': math.prod(values),
                        'min': min(values), 'max': max(values)}[operation]
            assert row.entries[column].value == expected
            assert row.entries[column].empty == identity
            assert row.entries[column].missing == identity
