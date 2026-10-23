"""Named matrix annotations preserve one aggregation scope and public schemas."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('method', ['annotate_cols', 'select_cols', 'transmute_cols'])
@pytest.mark.parametrize('partitions', [1, 3])
def test_named_column_aggregations_keep_fields_keys_and_sparse_positions(method, partitions):
    m = hl.utils.range_matrix_table(7, 4, n_partitions=partitions)
    m = m.annotate_globals(offset=3)
    m = m.annotate_entries(x=hl.or_missing(m.row_idx != 2, m.row_idx + m.col_idx))
    m = m.filter_entries((m.row_idx != 4) & (m.col_idx != 3))
    result = getattr(m, method)(
        total=hl.agg.sum(m.x), n=hl.agg.count(), positive=hl.agg.count_where(m.x > 4),
        fraction=hl.agg.fraction(m.x > 4), row_total=hl.agg.sum(m.row_idx),
        shifted=hl.agg.sum(m.x + m.offset), label=m.col_idx + 10,
        # Unsupported coordinate reducer must retain its existing implementation
        # beside the compatible shared scope.
        distinct=hl.agg.collect_as_set(m.x),
    )
    outputs = result.cols().collect()
    for col, out in enumerate(outputs):
        present = [r for r in range(7) if r != 4 and col != 3]
        defined = [r + col for r in present if r != 2]
        assert out.col_idx == col and out.label == col + 10
        assert out.total == sum(defined) and out.n == len(present)
        assert out.positive == sum(x > 4 for x in defined)
        assert out.row_total == sum(present)
        assert out.shifted == sum(x + 3 for x in defined)
        assert out.fraction == (sum(x > 4 for x in defined) / len(present) if present else None)
        assert out.distinct == set(defined + ([None] if 2 in present else []))


@pytest.mark.parametrize('rows,cols', [(0, 3), (5, 0)])
def test_named_column_aggregate_empty_axes(rows, cols):
    m = hl.utils.range_matrix_table(rows, cols, n_partitions=1)
    m = m.annotate_cols(n=hl.agg.count(), total=hl.agg.sum(m.row_idx),
                        mean=hl.agg.mean(m.row_idx), all_true=hl.agg.all(m.row_idx >= 0))
    values = m.cols().collect()
    assert len(values) == cols
    for value in values:
        assert value.n == value.total == 0 and value.all_true
        assert math.isnan(value.mean)


def test_named_projection_can_discard_unused_aggregate_fields():
    m = hl.utils.range_matrix_table(5, 3, n_partitions=1)
    m = m.annotate_entries(x=m.row_idx + m.col_idx)
    m = m.annotate_cols(total=hl.agg.sum(m.x), mean=hl.agg.mean(m.x))
    assert [x.total for x in m.cols().select('total').collect()] == [10, 15, 20]


def test_shared_row_genomic_reducers_keep_empty_and_defined_probabilities():
    m = hl.utils.range_matrix_table(4, 3, n_partitions=2)
    m = m.annotate_entries(GP=hl.if_else(m.col_idx == 1,
        hl.missing(hl.tarray(hl.tfloat64)),
        hl.if_else(m.col_idx == 0, hl.array([1., 0., 0.]), hl.array([0., 1., 0.]))))
    m = m.filter_entries(m.row_idx != 2)
    summary = hl.agg.info_score(m.GP)
    m = m.annotate_rows(info=summary, n=summary.n_included,
                        twice=summary.n_included + summary.n_included)
    values = m.rows().collect()
    for row, value in enumerate(values):
        assert value.info.n_included == value.n == (0 if row == 2 else 2)
        assert value.twice == 2 * value.n
        assert value.info.score == (None if row == 2 else 1.)


def test_shared_row_reducers_preserve_unselected_result_branch():
    m = hl.utils.range_matrix_table(3, 4, n_partitions=1)
    m = m.annotate_entries(x=m.row_idx + m.col_idx)
    total = hl.agg.sum(m.x)
    result = hl.if_else(m.row_idx == 0, total, total + total)
    m = m.annotate_rows(result=result)
    assert [x.result for x in m.rows().collect()] == [6, 20, 28]


def test_repeated_failing_row_aggregate_stays_in_unselected_branch():
    m = hl.utils.range_matrix_table(3, 4, n_partitions=1)
    bad = hl.agg.sum(hl.case().when(m.col_idx < 0, 1).or_error('unselected reducer'))
    good = hl.agg.sum(m.col_idx)
    m = m.annotate_rows(result=hl.if_else(m.row_idx < 0, bad + bad, good + good))
    assert [x.result for x in m.rows().collect()] == [12, 12, 12]
