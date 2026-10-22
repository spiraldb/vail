"""Singleton expression captures retain nested values across row execution."""
import vail as hl


def test_external_nested_globals_are_independent_and_repeatable():
    first = hl.range_table(1).annotate_globals(
        payload=hl.struct(items=hl.array([3, 7]), missing=hl.missing(hl.tstr)),
        empty=hl.empty_array(hl.tint32),
    ).index_globals()
    second = hl.range_table(1).annotate_globals(
        payload=hl.struct(items=hl.array([11, 13]), missing='defined'),
        empty=hl.empty_array(hl.tint32),
    ).index_globals()
    table = hl.range_table(7, 3)
    table = table.annotate(
        a=first.payload['items'].map(lambda value: value + table.idx),
        b=second.payload['items'].map(lambda value: value - table.idx),
        null=first.payload.missing,
        text=second.payload.missing,
        empty=first.empty,
    )
    for _ in range(2):
        result = table.collect()
        assert [(r.idx, r.a, r.b, r.null, r.text, r.empty) for r in result] == [
            (i, [i+3, i+7], [11-i, 13-i], None, 'defined', []) for i in range(7)
        ]


def test_scalar_context_matrix_columns_and_null_records():
    matrix = hl.range_matrix_table(5, 4, 3)
    matrix = matrix.annotate_globals(
        offset=17,
        record=hl.missing(hl.tstruct(label=hl.tstr)),
    )
    matrix = matrix.annotate_cols(coeff=hl.array([matrix.col_idx, matrix.col_idx+1]))
    matrix = matrix.choose_cols([3, 1, 3, 0])
    matrix = matrix.annotate_entries(
        value=matrix.row_idx + matrix.coeff[1] + matrix.offset,
        missing=matrix.record.label,
    )
    rows = matrix.localize_entries('entries', 'columns')
    for _ in range(2):
        result = rows.collect()
        assert [r.row_idx for r in result] == list(range(5))
        for row in result:
            assert [(e.value, e.missing) for e in row.entries] == [
                (row.row_idx+c+18, None) for c in [3, 1, 3, 0]
            ]
    assert hl.range_table(0).annotate(value=matrix.index_globals().offset).collect() == []
