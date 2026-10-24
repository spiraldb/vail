"""Generic native list shapes preserve semantics through relational consumers."""
import pytest
import vail as hl


@pytest.mark.parametrize("width", [0, 7, 1025])
def test_native_list_length_null_elements_aliases_and_consumers(tmp_path, width):
    table = hl.utils.range_table(37, n_partitions=4)
    items = hl.range(width).map(lambda c: hl.or_missing(
        (table.idx + c) % 3 != 0,
        hl.struct(text=hl.str(c), nested=hl.array([c, c + 1]), score=hl.float64(c))))
    table = table.annotate(items=hl.or_missing(table.idx % 5 != 0, items))
    table = table.annotate_globals(description="shape-only", delta=hl.int32(11))
    path = str(tmp_path / "shapes.ht")
    table.write(path)
    source = hl.read_table(path)
    lengths = source.select(alias=hl.len(source.items))
    expected = [(i, None if i % 5 == 0 else width) for i in range(37)]
    assert [(row.idx, row.alias) for row in lengths.collect()] == expected
    assert lengths.alias.dtype == hl.tint32
    assert lengths.count() == 37
    assert lengths.aggregate(hl.agg.sum(lengths.alias)) == 29 * width
    assert source.aggregate(hl.agg.sum(hl.len(source.items))) == 29 * width
    assert [(row.idx, row.alias) for row in lengths.filter(
        lengths.alias >= width).head(9).collect()] == [p for p in expected if p[1] is not None][:9]
    assert hl.eval(lengths.index_globals().description) == "shape-only"
    # Bindings and arithmetic are outside the closed scan projection; retain
    # original query scope, missing values and the ordinary global aggregate.
    assert source.aggregate(hl.agg.sum(
        hl.len(source.items) + source.index_globals().delta)) == 29 * (width + 11)


def test_native_list_length_variable_nested_and_missing_values(tmp_path):
    import pyarrow as pa
    values = [[], [None], [[1, 2], None, []], None, [None, None]]
    data = pa.Table.from_pydict({"id": list(range(len(values))), "items": values},
        schema=pa.schema([("id", pa.int32()), ("items", pa.list_(pa.list_(pa.int32())))]))
    path = str(tmp_path / "variable.ht")
    hl.Table.parallelize(data, n_partitions=2).write(path)
    source = hl.read_table(path)
    got = source.select("id", n=hl.len(source.items)).collect()
    assert [(row.id, row.n) for row in got] == [(i, None if v is None else len(v))
                                               for i, v in enumerate(values)]


def test_native_list_length_can_drop_other_computed_columns(tmp_path):
    table = hl.utils.range_table(41, n_partitions=3)
    table = table.annotate(items=hl.range(19).map(lambda c: hl.struct(
        value=hl.int32(table.idx - c), payload=hl.str(c))))
    path = str(tmp_path / "siblings.ht")
    table.write(path)
    source = hl.read_table(path)
    siblings = source.select(n=hl.len(source.items), total=hl.sum(
        source.items.map(lambda e: hl.int64(e.value))))
    assert [(row.idx, row.n) for row in siblings.select("n").collect()] == [(i, 19) for i in range(41)]
    assert siblings.aggregate(hl.struct(
        n=hl.agg.sum(siblings.n), total=hl.agg.sum(siblings.total))) == hl.Struct(
            n=41 * 19, total=sum(i - c for i in range(41) for c in range(19)))


def test_native_list_lengths_across_many_files_and_independent_reads(tmp_path):
    table = hl.utils.range_table(258, n_partitions=129)
    table = table.annotate(items=hl.or_missing(table.idx % 7 != 0,
        hl.range(11).map(lambda c: hl.struct(value=table.idx + c))))
    path = str(tmp_path / "many.ht")
    table.write(path)
    expected = [(i, None if i % 7 == 0 else 11) for i in range(258)]
    for _ in range(2):
        source = hl.read_table(path)
        result = source.select(n=hl.len(source.items))
        assert [(row.idx, row.n) for row in result.collect()] == expected
        assert result.aggregate(hl.agg.sum(result.n)) == sum(
            n for _, n in expected if n is not None)
