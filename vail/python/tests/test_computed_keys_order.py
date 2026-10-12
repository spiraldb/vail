import pytest

import vail as hl
from vail.expressions import ExpressionException


def test_computed_keys_replace_existing_keys_simultaneously():
    table = hl.range_table(6, n_partitions=3).annotate_globals(offset=10)
    keyed = table.key_by(idx=-table.idx, old=table.idx + table.offset)
    assert list(keyed.key) == ["idx", "old"]
    assert [(row.idx, row.old) for row in keyed.collect()] == [
        (-i, i + 10) for i in reversed(range(6))
    ]
    assert [row.idx for row in table.collect()] == list(range(6))
    assert hl.eval(keyed.globals).offset == 10


def test_mixed_and_nested_keys_and_missing_values_round_trip(tmp_path):
    table = hl.Table.parallelize(
        [{"id": 0, "s": hl.Struct(x=2)}, {"id": 1, "s": hl.Struct(x=None)},
         {"id": 2, "s": hl.Struct(x=1)}],
        schema=hl.tstruct(id=hl.tint32, s=hl.tstruct(x=hl.tint32)),
        n_partitions=2,
    )
    nested = table.key_by(table.s.x)
    assert list(nested.key) == ["x"]
    assert [row.id for row in nested.collect()] == [2, 0, 1]
    keyed = table.key_by("id", bounds=hl.interval(table.id, table.id + 1))
    path = tmp_path / "computed.viceberg"
    keyed.write(path)
    assert keyed._same(hl.read_table(path))


def test_computed_sort_directions_ties_globals_and_private_name_collisions():
    table = hl.Table.parallelize([
        {"id": 0, "x": 1, "y": 3, "__vail_sort_0": 4},
        {"id": 1, "x": 2, "y": 2, "__vail_sort_0": 5},
        {"id": 2, "x": 1, "y": 2, "__vail_sort_0": 6},
        {"id": 3, "x": 1, "y": 3, "__vail_sort_0": 7},
    ], key="id", n_partitions=2).annotate_globals(offset=10, __vail_sort_1=20)
    ordered = table.order_by(hl.desc(table.x + table.y + table.offset), -table.x)
    assert [row.id for row in ordered.collect()] == [1, 0, 3, 2]
    assert ordered._schema == table._schema
    assert list(ordered.key) == []
    assert hl.eval(ordered.globals) == hl.eval(table.globals)
    plan = ordered._native.physical_plan()
    assert "SortExec" in plan and "ProjectionExec" in plan


def test_nested_sort_and_missing_values():
    table = hl.Table.parallelize([
        {"id": 0, "s": hl.Struct(x=None)},
        {"id": 1, "s": hl.Struct(x=2)},
        {"id": 2, "s": hl.Struct(x=1)},
    ], schema=hl.tstruct(id=hl.tint32, s=hl.tstruct(x=hl.tint32)))
    assert [row.id for row in table.order_by(table.s.x).collect()] == [2, 1, 0]
    assert [row.id for row in table.order_by(hl.desc(table.s.x + 1)).collect()] == [1, 2, 0]


def test_invalid_key_and_sort_expression_scopes():
    table = hl.range_table(3).annotate_globals(g=1)
    other = hl.range_table(3)
    with pytest.raises(ExpressionException):
        table.key_by(table.idx + 1)
    with pytest.raises(ExpressionException):
        table.key_by(k=other.idx + 1)
    with pytest.raises(ValueError, match="duplicate"):
        table.key_by("idx", idx=table.idx + 1)
    with pytest.raises(ValueError):
        table.order_by(other.idx + 1)
    with pytest.raises(ValueError):
        table.order_by(hl.literal(1))
    with pytest.raises(ValueError, match="row-indexed"):
        table.order_by(table.g)


def test_computed_key_and_sort_remain_lazy_when_files_are_unavailable(tmp_path):
    table = hl.range_table(4, n_partitions=2)
    path = tmp_path / "lazy.viceberg"
    table.write(path)
    loaded = hl.read_table(path)
    data_files = list(path.rglob("*.vortex"))
    assert data_files
    for data_file in data_files:
        data_file.unlink()
    # Constructing either transformation must not start a source scan.
    keyed = loaded.key_by(idx=-loaded.idx)
    ordered = loaded.order_by(-loaded.idx)
    assert keyed._schema == loaded._schema
    assert ordered._schema == loaded._schema
    with pytest.raises(FileNotFoundError):
        keyed.collect()
    with pytest.raises(FileNotFoundError):
        ordered.collect()
