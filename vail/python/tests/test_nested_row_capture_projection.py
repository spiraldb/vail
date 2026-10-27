"""Public aligned callbacks retain capture values, nulls, and shared consumers."""
import pytest
import vail as hl


def matrix():
    m = hl.range_matrix_table(4, 3, 2)
    m = m.annotate_entries(v=m.row_idx * 10 + m.col_idx)
    m = m.annotate_rows(meta=hl.or_missing(m.row_idx != 1, hl.struct(
        small=hl.array([m.row_idx, 1]), other=hl.array([2 * m.row_idx, 2]),
        scalar=m.row_idx, unused=hl.str(m.row_idx) + 'x' * 128)))
    return m.filter_entries((m.row_idx + m.col_idx) % 4 != 0)


def test_nested_callback_unions_shared_capture_leaf_demands():
    m = matrix()
    capture = m.meta
    score = hl.sum(capture.small.map(lambda x: x + hl.sum(capture.other))) + m.v
    result = m.select_entries(score=score).entries().collect()
    assert sorted((x.row_idx, x.col_idx, x.score) for x in result) == [
        (r, c, None if r == 1 else 15 * r + c + 5)
        for r in range(4) for c in range(3) if (r + c) % 4 != 0]


def test_whole_capture_use_preserves_actual_unused_values_and_shared_source():
    m = matrix()
    source_rows = m.rows().collect()
    result = m.select_entries(score=hl.sum(m.meta.small) + m.v, saved=m.meta).entries().collect()
    assert [(x.row_idx, x.col_idx) for x in result] == [
        (r,c) for r in range(4) for c in range(3) if (r+c)%4 != 0]
    for row in result:
        if row.row_idx == 1:
            assert row.saved is None and row.score is None
        else:
            assert row.saved.small == [row.row_idx, 1]
            assert row.saved.other == [2 * row.row_idx, 2]
            assert row.saved.unused == str(row.row_idx) + 'x' * 128
            assert row.score == 11 * row.row_idx + row.col_idx + 1
    assert m.rows().collect() == source_rows


def test_scalar_row_control_preserves_missing_capture():
    m = matrix()
    result = m.select_entries(score=m.v + m.meta.scalar).entries().collect()
    assert sorted((x.row_idx, x.col_idx, x.score) for x in result) == [
        (r, c, None if r == 1 else 11 * r + c)
        for r in range(4) for c in range(3) if (r + c) % 4 != 0]


def test_opaque_call_capture_keeps_its_logical_descriptor():
    m = hl.range_matrix_table(3, 2, 2)
    m = m.annotate_rows(meta=hl.struct(small=hl.array([m.row_idx, 1]),
                                     opaque=hl.call(m.row_idx % 2, 1)))
    result = m.select_entries(score=hl.sum(m.meta.small), call=m.meta.opaque).entries().collect()
    assert [(x.row_idx, x.col_idx, x.score, x.call) for x in result] == [
        (r, c, r + 1, hl.Call([r % 2, 1])) for r in range(3) for c in range(2)]


@pytest.mark.parametrize('width', [8, 48])
def test_wide_shared_capture_has_all_requested_leaves(width):
    m = hl.range_matrix_table(2, 2)
    m = m.annotate_rows(meta=hl.struct(**{
        **{f's{i}': hl.array([m.row_idx, i]) for i in range(width)},
        'unused': hl.str(m.row_idx) + 'z' * 256,
    }))
    capture = m.meta
    score = hl.sum(hl.array([hl.sum(capture[f's{i}']) for i in range(width)]))
    rows = m.select_entries(score=score).entries().collect()
    assert [(x.row_idx, x.col_idx, x.score) for x in rows] == [
        (r, c, width * r + width * (width - 1) // 2) for r in range(2) for c in range(2)]


@pytest.mark.parametrize('whole_prefix', [False, True])
def test_deep_shared_capture_prefixes_preserve_selected_and_whole_values(whole_prefix):
    depth = 24
    m = hl.range_matrix_table(2, 2)
    nested = hl.struct(values=hl.array([m.row_idx, 1]), discarded='leaf-payload')
    for _ in range(depth):
        nested = hl.struct(child=nested)
    m = m.annotate_rows(meta=hl.struct(child=nested, unused='outer-payload'))
    capture = m.meta.child
    prefix = capture
    for _ in range(depth):
        capture = capture.child
    selected = {'score': hl.sum(capture['values'])}
    if whole_prefix:
        selected['saved'] = prefix
    rows = m.select_entries(**selected).entries().collect()
    assert [(x.row_idx,x.col_idx) for x in rows] == [(r,c) for r in range(2) for c in range(2)]
    for row in rows:
        assert row.score == row.row_idx + 1
        if whole_prefix:
            saved = row.saved
            for _ in range(depth):
                saved = saved.child
            assert saved['values'] == [row.row_idx, 1]
            assert saved.discarded == 'leaf-payload'


@pytest.mark.parametrize('depth', [0, 3])
@pytest.mark.parametrize('opaque', ['call', 'set', 'dict', 'nested_array'])
def test_unused_logical_siblings_do_not_block_nested_capture_projection(depth, opaque):
    m = hl.range_matrix_table(4, 3, 2)
    special = {
        'call': lambda: hl.call(m.row_idx % 2, 1),
        'set': lambda: hl.set([m.row_idx, 9]),
        'dict': lambda: hl.dict([(m.row_idx, 'payload')]),
        'nested_array': lambda: hl.array([hl.struct(value=m.row_idx)]),
    }[opaque]()
    nested = hl.struct(values=hl.array([m.row_idx, 1]), special=special,
                       unused=hl.str(m.row_idx) + 'x' * 1024)
    for _ in range(depth):
        nested = hl.struct(child=nested, unused=special)
    m = m.annotate_rows(meta=hl.or_missing(m.row_idx != 1, nested))
    m = m.filter_entries((m.row_idx + m.col_idx) % 4 != 0)
    capture = m.meta
    for _ in range(depth):
        capture = capture.child
    source = m.rows().collect()
    rows = m.select_entries(score=hl.sum(capture['values'])).entries().collect()
    assert [(x.row_idx, x.col_idx, x.score) for x in rows] == [
        (r, c, None if r == 1 else r + 1)
        for r in range(4) for c in range(3) if (r + c) % 4 != 0]
    assert m.rows().collect() == source


@pytest.mark.parametrize('kind', ['call', 'set', 'dict', 'nested_array'])
def test_selected_logical_leaf_keeps_its_type_and_values_while_siblings_are_unused(kind):
    m = hl.range_matrix_table(3, 2, 2)
    special = {
        'call': lambda: hl.call(m.row_idx % 2, 1),
        'set': lambda: hl.set([m.row_idx, 9]),
        'dict': lambda: hl.dict([(m.row_idx, 'payload')]),
        'nested_array': lambda: hl.array([hl.struct(value=m.row_idx)]),
    }[kind]()
    m = m.annotate_rows(meta=hl.struct(child=hl.struct(
        small=hl.array([m.row_idx, 1]), special=special,
        unused=hl.str(m.row_idx) + 'z' * 1024), unused=special))
    rows = m.select_entries(score=hl.sum(m.meta.child.small),
                            saved=m.meta.child.special).entries().collect()
    assert [(x.row_idx, x.col_idx, x.score) for x in rows] == [
        (r, c, r + 1) for r in range(3) for c in range(2)]
    for row in rows:
        expected = {
            'call': lambda: hl.Call([row.row_idx % 2, 1]),
            'set': lambda: {row.row_idx, 9},
            'dict': lambda: {row.row_idx: 'payload'},
            'nested_array': lambda: [hl.Struct(value=row.row_idx)],
        }[kind]()
        assert row.saved == expected
