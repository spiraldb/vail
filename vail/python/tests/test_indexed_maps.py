import pytest
import vail as hl


def test_indexed_maps_keep_missing_lists_and_missing_elements_distinct():
    table = hl.range_table(4)
    values = hl.literal([0.0, None, 2.0], hl.tarray(hl.tfloat64))
    table = table.annotate(xs=hl.if_else(
        table.idx == 1, hl.missing(values.dtype), values))
    rows = table.select(
        answer=table.xs.map(lambda x: x * 2.0 + 1.0)[0],
        element_missing=table.xs.map(lambda x: hl.is_missing(x))[1],
    ).collect()
    assert [row.answer for row in rows] == [1.0, None, 1.0, 1.0]
    assert [row.element_missing for row in rows] == [True, None, True, True]


def test_indexed_maps_preserve_ancestor_nulls_and_row_captures():
    table = hl.range_table(4)
    payload = hl.struct(xs=hl.literal([5.0, None], hl.tarray(hl.tfloat64)),
                        unused='x' * 4096)
    table = table.annotate(nested=hl.if_else(
        table.idx == 2, hl.missing(payload.dtype), payload))
    rows = table.select(
        answer=table.nested.xs.map(lambda x: x - 1.0)[0],
        captured=table.nested.xs.map(lambda x: x + table.idx)[0],
    ).collect()
    assert [row.answer for row in rows] == [4.0, 4.0, None, 4.0]
    assert [row.captured for row in rows] == [5.0, 6.0, None, 8.0]


def test_indexing_does_not_skip_a_failure_in_an_unrequested_map_element():
    table = hl.range_table(1).annotate(xs=hl.literal([1.0, -1.0]))
    answer = table.xs.map(lambda x: hl.case().when(x > 0, x).or_error(
        'unrequested map element failed'))[0]
    with pytest.raises(RuntimeError, match='unrequested map element failed'):
        table.select(answer=answer).collect()
