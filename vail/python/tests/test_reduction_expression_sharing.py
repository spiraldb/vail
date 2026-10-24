"""Public semantics of reusable reductions with independently bound callbacks."""
import math

import pytest
import vail as hl


@pytest.mark.parametrize("partitions", [1, 3])
def test_sibling_fractions_preserve_sparse_entries_and_missing_predicates(partitions):
    matrix = hl.utils.range_matrix_table(7, 6, n_partitions=partitions)
    matrix = matrix.annotate_entries(
        value=hl.or_missing(matrix.col_idx % 3 != 0, matrix.row_idx + matrix.col_idx))
    matrix = matrix.filter_entries((matrix.row_idx + matrix.col_idx) % 4 != 0)
    thresholds = [-1, 3, 7, 20]
    rows = matrix.annotate_rows(**{
        f"fraction_{i}": hl.agg.fraction(matrix.value > threshold)
        for i, threshold in enumerate(thresholds)
    }).rows().collect()
    for row in rows:
        present = [col for col in range(6) if (row.row_idx + col) % 4 != 0]
        for i, threshold in enumerate(thresholds):
            count = sum(col % 3 != 0 and row.row_idx + col > threshold for col in present)
            assert row[f"fraction_{i}"] == count / len(present)


def test_reused_nested_callbacks_distinguish_capture_scope_and_fold_arguments():
    table = hl.Table.parallelize([
        {"xs": [1, None, 4], "offset": 10},
        {"xs": [], "offset": 20},
        {"xs": None, "offset": 30},
    ], schema=hl.tstruct(xs=hl.tarray(hl.tint32), offset=hl.tint32))
    table = table.select(
        a=table.xs.map(lambda x: hl.array([2, 3]).map(lambda y: x + y + table.offset)),
        b=table.xs.map(lambda other: hl.array([2, 3]).map(lambda inner: other + inner + table.offset)),
        c=table.xs.map(lambda x: hl.array([2, 3]).map(lambda y: y + table.offset)),
        left=table.xs.filter(lambda item: hl.is_defined(item)).fold(lambda state, item: state * 2 + item, 0),
        right=table.xs.filter(lambda value: hl.is_defined(value)).fold(lambda previous, value: previous + value * 2, 0))
    rows = table.collect()
    assert rows[0].a == rows[0].b == [[13, 14], [None, None], [16, 17]]
    assert rows[0].c == [[12, 13], [12, 13], [12, 13]]
    assert (rows[0].left, rows[0].right) == (6, 10)
    assert rows[1] == hl.Struct(a=[], b=[], c=[], left=0, right=0)
    assert rows[2] == hl.Struct(a=None, b=None, c=None, left=None, right=None)


def test_equivalent_callbacks_do_not_hoist_domain_errors_from_unselected_branches():
    table = hl.Table.parallelize([{"xs": [0, 2]}, {"xs": [4]}])
    values = table.select(
        first=table.xs.map(lambda x: hl.if_else(x == 0, 0, 8 // x)),
        second=table.xs.map(lambda value: hl.if_else(value == 0, 0, 8 // value)),
        empty=hl.if_else(False, table.xs.map(lambda x: 8 // (x - x)), hl.empty_array(hl.tint32)))
    assert values.collect() == [hl.Struct(first=[0, 4], second=[0, 4], empty=[]), hl.Struct(first=[2], second=[2], empty=[])]
    with pytest.raises(RuntimeError):
        table.select(fail=table.xs.map(lambda value: 8 // value)).collect()


def test_empty_entry_fraction_keeps_nan_contract():
    matrix = hl.utils.range_matrix_table(2, 3)
    matrix = matrix.filter_entries(False)
    rows = matrix.annotate_rows(
        a=hl.agg.fraction(matrix.col_idx > 1),
        b=hl.agg.fraction(matrix.col_idx > 2)).rows().collect()
    assert len(rows) == 2
    assert all(math.isnan(row.a) and math.isnan(row.b) for row in rows)
