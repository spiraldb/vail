"""Sibling reductions share an iteration domain without changing values or demand."""
import math
import pytest
import vail as hl


def _numeric_matrix(partitions):
    m = hl.range_matrix_table(5, 129, n_partitions=partitions)
    m = m.annotate_entries(
        a=hl.or_missing(m.col_idx % 7 != 0, hl.int32(m.col_idx - 65)),
        b=hl.int64(m.col_idx * 3 - 7),
    )
    return m.filter_entries((m.row_idx != 2) & ((m.row_idx != 3) | (m.col_idx % 5 == 0)))


@pytest.mark.parametrize('partitions', [1, 3])
def test_shared_numeric_reductions_keep_signed_values_presence_and_empty_rows(partitions):
    m = _numeric_matrix(partitions)
    rows = m.annotate_rows(
        yes=hl.agg.count_where(m.a % 3 == 0),
        no=hl.agg.count_where(m.a % 3 != 0),
        low=hl.agg.fraction(hl.float64(m.a) / 7 < 2),
        high=hl.agg.fraction(hl.float64(m.a) / 7 >= 2),
        other_yes=hl.agg.fraction(m.b % 11 == 0),
        other_no=hl.agg.fraction(m.b % 11 != 0),
    ).rows().collect()
    for row in rows:
        columns = [c for c in range(129) if row.row_idx != 2 and (row.row_idx != 3 or c % 5 == 0)]
        defined = [c - 65 for c in columns if c % 7 != 0]
        assert row.yes == sum(a % 3 == 0 for a in defined)
        assert row.no == sum(a % 3 != 0 for a in defined)
        if not columns:
            assert all(math.isnan(row[k]) for k in ['low', 'high', 'other_yes', 'other_no'])
        else:
            assert row.low == sum(a / 7 < 2 for a in defined) / len(columns)
            assert row.high == sum(a / 7 >= 2 for a in defined) / len(columns)
            assert row.other_yes == sum((c * 3 - 7) % 11 == 0 for c in columns) / len(columns)
            assert row.other_no == sum((c * 3 - 7) % 11 != 0 for c in columns) / len(columns)


def test_reductions_with_different_filtered_domains_keep_distinct_denominators():
    m = _numeric_matrix(2)
    rows = m.annotate_rows(
        all_yes=hl.agg.fraction(m.b % 3 == 0),
        all_no=hl.agg.fraction(m.b % 3 != 0),
        half_yes=hl.agg.filter(m.col_idx % 2 == 0, hl.agg.fraction(m.b % 3 == 0)),
        half_no=hl.agg.filter(m.col_idx % 2 == 0, hl.agg.fraction(m.b % 3 != 0)),
    ).rows().collect()
    for row in rows:
        cols = [c for c in range(129) if row.row_idx != 2 and (row.row_idx != 3 or c % 5 == 0)]
        for prefix, chosen in [('all', cols), ('half', [c for c in cols if c % 2 == 0])]:
            if not chosen:
                assert (row[f'{prefix}_yes'] is None and row[f'{prefix}_no'] is None) if prefix == 'half' else (math.isnan(row[f'{prefix}_yes']) and math.isnan(row[f'{prefix}_no']))
            else:
                assert row[f'{prefix}_yes'] == sum((c * 3 - 7) % 3 == 0 for c in chosen) / len(chosen)
                assert row[f'{prefix}_no'] == sum((c * 3 - 7) % 3 != 0 for c in chosen) / len(chosen)


def test_shared_predicates_keep_row_captures_and_repeated_execution():
    m = _numeric_matrix(2)
    query = m.annotate_rows(
        below=hl.agg.count_where(hl.float64(m.b) / 11 < m.row_idx + 2),
        above=hl.agg.count_where(hl.float64(m.b) / 11 >= m.row_idx + 2),
    ).rows()
    first = query.collect()
    assert query.collect() == first
    for row in first:
        cols = [c for c in range(129) if row.row_idx != 2 and (row.row_idx != 3 or c % 5 == 0)]
        assert row.below == sum((c * 3 - 7) / 11 < row.row_idx + 2 for c in cols)
        assert row.above == sum((c * 3 - 7) / 11 >= row.row_idx + 2 for c in cols)


def test_mixed_call_and_numeric_reductions_retain_each_result():
    m = hl.range_matrix_table(3, 33, n_partitions=2)
    m = m.annotate_entries(a=hl.int32(m.col_idx - 17), gt=hl.call(m.col_idx % 2, 0))
    rows = m.annotate_rows(
        hets=hl.agg.count_where(m.gt.is_het()),
        even=hl.agg.fraction(m.a % 5 == 0),
        odd=hl.agg.fraction(m.a % 5 != 0),
        all_calls=hl.agg.count_where(hl.is_defined(m.gt)),
    ).rows().collect()
    for row in rows:
        assert row.hets == 16 and row.all_calls == 33
        assert row.even == sum((c - 17) % 5 == 0 for c in range(33)) / 33
        assert row.odd == sum((c - 17) % 5 != 0 for c in range(33)) / 33


def test_fallible_sibling_reductions_keep_first_error():
    m = hl.range_matrix_table(1, 3)
    m = m.annotate_entries(a=hl.int32(m.col_idx), xs=hl.empty_array(hl.tint32))
    query = m.annotate_rows(
        first=hl.agg.count_where(hl.case().when(m.col_idx < 0, True).or_error('first predicate error')),
        even=hl.agg.fraction(m.a % 3 == 0),
        odd=hl.agg.fraction(m.a % 3 != 0),
        later=hl.agg.count_where(m.xs[0] > 0),
    ).rows()
    with pytest.raises(Exception, match='first predicate error'):
        query.collect()


def test_float_reductions_keep_nan_infinity_and_signed_zero():
    m = hl.range_matrix_table(1, 8)
    values = hl.literal([float('nan'), float('inf'), float('-inf'), -0.0, 0.0, -7.0, 7.0, None], hl.tarray(hl.tfloat64))
    m = m.annotate_entries(x=values[m.col_idx])
    row = m.annotate_rows(
        low=hl.agg.count_where(m.x / 7 < 0),
        high=hl.agg.count_where(m.x / 7 >= 0),
        equal=hl.agg.fraction(m.x / 7 == 0),
        unequal=hl.agg.fraction(m.x / 7 != 0),
    ).rows().collect()[0]
    # Scalar comparisons retain the engine's ordering of NaN above finite values.
    assert (row.low, row.high) == (2, 5)
    assert row.equal == 2 / 8 and row.unequal == 5 / 8


def test_joint_float_comparisons_match_independent_single_sink_actions():
    import struct
    positive_nan = struct.unpack('d', struct.pack('Q', 0x7ff8000000000001))[0]
    negative_nan = struct.unpack('d', struct.pack('Q', 0xfff8000000000002))[0]
    m = hl.range_matrix_table(2, 9, n_partitions=2)
    values = hl.literal([positive_nan, negative_nan, float('inf'), float('-inf'), -0.0, 0.0, -3.0, 3.0, None], hl.tarray(hl.tfloat64))
    m = m.annotate_entries(x=values[m.col_idx])
    q = m.x / 11
    predicates = {'lt': q < 0, 'le': q <= 0, 'gt': q > 0, 'ge': q >= 0,
                  'eq': q == 0, 'ne': q != 0, 'same': q == q, 'different': q != q}
    expected = {name: [r.answer for r in m.annotate_rows(answer=hl.agg.count_where(p)).rows().collect()]
                for name, p in predicates.items()}
    rows = m.annotate_rows(**{name: hl.agg.count_where(p) for name, p in predicates.items()}).rows().collect()
    for name in predicates:
        assert [r[name] for r in rows] == expected[name]


def test_joint_numeric_sinks_preserve_string_and_nested_siblings(tmp_path):
    m = hl.range_matrix_table(2, 8, n_partitions=2)
    labels = hl.literal([None, 'é', 'α', '猫', '🙂', '', 'abc', 'z'], hl.tarray(hl.tstr))
    m = m.annotate_entries(a=hl.int32(m.col_idx - 5), label=labels[m.col_idx])
    path = str(tmp_path / 'text.mt')
    m.write(path)
    m = hl.read_matrix_table(path)
    query = m.annotate_rows(
        labels=hl.agg.collect(m.label),
        yes=hl.agg.fraction(m.a % 3 == 0),
        no=hl.agg.fraction(m.a % 3 != 0),
        nested=hl.agg.take(hl.struct(label=m.label, xs=hl.array([m.a])), 2),
        collected=hl.agg.take(m.label, 2),
    ).rows()
    for row in query.collect():
        assert sorted(map(repr, row.labels)) == sorted(map(repr, [None, 'é', 'α', '猫', '🙂', '', 'abc', 'z']))
        assert row.nested == [hl.Struct(label=None, xs=[-5]), hl.Struct(label='é', xs=[-4])]
        assert row.collected == [None, 'é']
        assert row.yes == 2 / 8 and row.no == 6 / 8


def test_projected_joint_result_retains_baseline_eager_sibling_error():
    m = hl.range_matrix_table(2, 7)
    m = m.annotate_entries(a=hl.int32(m.col_idx))
    result = m.annotate_rows(
        yes=hl.agg.fraction(m.a % 3 == 0),
        no=hl.agg.fraction(m.a % 3 != 0),
        unused=hl.agg.count_where(hl.case().when(m.col_idx < 0, True).or_error('unused reducer')),
    ).rows().select('yes')
    # The current shared annotation evaluates this sibling before projection.
    with pytest.raises(Exception, match='unused reducer'):
        result.collect()


def test_shared_counts_keep_long_rows_and_sparse_entry_presence():
    width = 16_451
    m = hl.range_matrix_table(4, width, n_partitions=1)
    m = m.annotate_entries(value=hl.or_missing(m.col_idx % 7 != 0, m.col_idx - 8_225))
    m = m.filter_entries((m.row_idx == 0) | ((m.row_idx == 2) & (m.col_idx % 3 == 0)) |
                         ((m.row_idx == 3) & ((m.col_idx == 63) | (m.col_idx == width - 1))))
    rows = m.annotate_rows(
        yes=hl.agg.count_where(m.value % 11 == 0),
        no=hl.agg.count_where(m.value % 11 != 0),
        fraction=hl.agg.fraction(m.value % 11 == 0),
    ).rows().collect()
    for row in rows:
        columns = [c for c in range(width) if row.row_idx == 0 or
                   (row.row_idx == 2 and c % 3 == 0) or
                   (row.row_idx == 3 and c in (63, width - 1))]
        values = [c - 8_225 for c in columns if c % 7 != 0]
        assert row.yes == sum(v % 11 == 0 for v in values)
        assert row.no == sum(v % 11 != 0 for v in values)
        assert row.fraction == row.yes / len(columns) if columns else math.isnan(row.fraction)
