"""Public native reads preserve nested masks and ordinary relational consumers."""
import pytest
import vail as hl


@pytest.mark.parametrize("rows,cols", [(0, 5), (3, 0), (11, 65)])
def test_native_wide_entry_schema_sum_and_empty_axes(tmp_path, rows, cols):
    m = hl.range_matrix_table(rows, cols, n_partitions=3)
    m = m.annotate_entries(
        prefix=hl.str(m.col_idx),
        payload=hl.or_missing(m.col_idx % 7 != 0, hl.struct(
            ignored=hl.int64(1 << 40),
            depth=hl.or_missing((m.row_idx + m.col_idx) % 11 != 0,
                                hl.int32(m.row_idx * 301 - m.col_idx * 13)))),
        suffix=hl.int32(17),
    )
    m = m.filter_entries((m.row_idx + m.col_idx) % 17 != 0)
    path = str(tmp_path / "wide.mt")
    m.write(path)
    r = hl.read_matrix_table(path)
    expected = sum(i * 301 - j * 13 for i in range(rows) for j in range(cols)
                   if j % 7 and (i+j) % 11 and (i+j) % 17)
    assert r.aggregate_entries(hl.agg.sum(r.payload.depth)) == expected
    # A composite aggregate remains ordinary relational aggregation, and must
    # preserve the same semantics even when its whole expression is unsupported.
    got = r.aggregate_entries(hl.struct(total=hl.agg.sum(r.payload.depth),
                                       again=hl.agg.sum(r.payload.depth),
                                       count=hl.agg.count()))
    assert got.total == got.again == expected
    assert got.count == sum((i+j) % 17 != 0 for i in range(rows) for j in range(cols))


def test_native_computed_projection_pruning_keeps_row_counts(tmp_path):
    import pyarrow as pa
    data = pa.Table.from_pylist([
        {"label": "one", "items": [{"prefix": 91, "n": 2147483647}, {"prefix": 92, "n": 1}]},
        {"label": "two", "items": [None, {"prefix": 93, "n": None}]},
        {"label": "three", "items": []},
        {"label": "four", "items": None},
    ], schema=pa.schema([("label", pa.string()), ("items", pa.list_(pa.struct([
        ("prefix", pa.int64()), ("n", pa.int32())])))]))
    path = str(tmp_path / "arrays.ht")
    hl.Table.parallelize(data).write(path)
    t = hl.read_table(path)
    mapped = t.select(label=t.label, total=hl.sum(t.items.map(lambda x: hl.int64(x.n))))
    assert mapped.count() == 4
    assert mapped.aggregate(hl.agg.count()) == 4
    assert [(r.label, r.total) for r in mapped.collect()] == [
        ("one", 2147483648), ("two", 0), ("three", 0), ("four", None)]
    assert mapped.aggregate(hl.agg.sum(mapped.total)) == 2147483648


def test_native_dense_projection_aliases_keys_globals_and_order(tmp_path):
    t = hl.utils.range_table(137, n_partitions=5)
    t = t.annotate(items=hl.range(41).map(
        lambda c: hl.struct(n=hl.int32(t.idx * 3 - c))))
    t = t.annotate_globals(description="dense-list")
    path = str(tmp_path / "dense.ht")
    t.write(path)
    r = hl.read_table(path)
    mapped = r.select(total=hl.sum(r.items.map(lambda e: hl.int64(e.n))))
    expected = [sum(i * 3 - c for c in range(41)) for i in range(137)]
    assert [(x.idx, x.total) for x in mapped.collect()] == list(enumerate(expected))
    assert mapped.aggregate(hl.agg.sum(mapped.total)) == sum(expected)
    assert hl.eval(mapped.index_globals().description) == "dense-list"
    # A shape-changing consumer must preserve ordinary relational semantics.
    filtered = mapped.filter(mapped.idx % 3 == 0)
    assert filtered.aggregate(hl.agg.sum(filtered.total)) == sum(expected[::3])
