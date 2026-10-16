"""A present struct stays present even when every child is missing."""
import pytest

import vail as hl


@pytest.mark.parametrize("n_partitions", [1, 3])
@pytest.mark.parametrize("fields", ["one", "two"])
def test_struct_parent_validity_survives_storage_and_projection(tmp_path, n_partitions, fields):
    schema = hl.tstruct(id=hl.tint32, value=hl.tstruct(x=hl.tint32, y=hl.tstr))
    rows = [
        {"id": 0, "value": hl.Struct(x=1, y=None)},
        {"id": 1, "value": hl.Struct(x=None, y=None)},
        {"id": 2, "value": None},
        {"id": 3, "value": hl.Struct(x=None, y="defined")},
    ]
    if fields == "one":
        schema = hl.tstruct(id=hl.tint32, value=hl.tstruct(x=hl.tint32))
        rows = [{"id": row["id"], "value": None if row["value"] is None else hl.Struct(x=row["value"].x)} for row in rows]
    table = hl.Table.parallelize(rows, schema=schema, key="id", n_partitions=n_partitions)
    path = tmp_path / "structs"
    table.write(path)
    reopened = hl.read_table(path)
    assert reopened.collect() == table.collect()
    assert reopened.to_arrow().equals(table.to_arrow(), check_metadata=True)
    assert reopened.select("value").collect() == table.select("value").collect()
    defined = reopened.select(defined=hl.is_defined(reopened.value))
    assert [row.defined for row in defined.collect()] == [True, True, False, True]
    assert [row.id for row in reopened.filter(hl.is_defined(reopened.value)).collect()] == [0, 1, 3]


def test_nested_struct_validities_are_independent(tmp_path):
    inner = hl.tstruct(x=hl.tint32)
    schema = hl.tstruct(id=hl.tint32, value=hl.tstruct(child=inner))
    table = hl.Table.parallelize([
        {"id": 0, "value": hl.Struct(child=hl.Struct(x=None))},
        {"id": 1, "value": hl.Struct(child=None)},
        {"id": 2, "value": None},
    ], schema=schema, key="id")
    path = tmp_path / "nested"
    table.write(path)
    reopened = hl.read_table(path)
    assert reopened.collect() == table.collect()
    assert reopened.to_arrow().equals(table.to_arrow(), check_metadata=True)
    assert [row.present for row in reopened.select(present=hl.is_defined(reopened.value)).collect()] == [True, True, False]
    assert [row.present for row in reopened.select(present=hl.is_defined(reopened.value.child)).collect()] == [True, False, False]


@pytest.mark.parametrize("field_names", [
    ["__vortex_parent_validity"],
    ["__vortex_parent_validity", "__vortex_parent_validity_0", "__vortex_parent_validity_1"],
])
def test_parent_validity_does_not_conflict_with_user_field_names(tmp_path, field_names):
    structure_type = hl.tstruct(**{name: hl.tint32 for name in field_names})
    table = hl.Table.parallelize([
        {"id": 0, "value": hl.Struct(**{name: None for name in field_names})},
        {"id": 1, "value": None},
    ], schema=hl.tstruct(id=hl.tint32, value=structure_type), key="id")
    path = tmp_path / "colliding-name"
    table.write(path)
    assert hl.read_table(path).collect() == table.collect()


def test_present_empty_struct_remains_distinct_from_missing_struct(tmp_path):
    table = hl.Table.parallelize([
        {"id": 0, "value": hl.Struct()},
        {"id": 1, "value": None},
    ], schema=hl.tstruct(id=hl.tint32, value=hl.tstruct()), key="id")
    path = tmp_path / "empty"
    table.write(path)
    reopened = hl.read_table(path)
    assert reopened.collect() == table.collect()
    assert [row.present for row in reopened.select(present=hl.is_defined(reopened.value)).collect()] == [True, False]


def test_sliced_arrow_struct_validity_across_batches_and_key_filter(tmp_path):
    import pyarrow as pa

    child = pa.StructArray.from_arrays(
        [pa.array([0, None, None, None, None, None], type=pa.int32())],
        names=["x"],
        mask=pa.array([False, False, True, True, False, True]),
    )
    value = pa.StructArray.from_arrays(
        [child], names=["child"], mask=pa.array([True, False, True, False, False, True]),
    )
    batch = pa.RecordBatch.from_arrays([pa.array(range(6)), value], names=["id", "value"])
    # Both nested validity buffers have a nonzero offset in the first input batch.
    arrow = pa.Table.from_batches([batch.slice(1, 3), batch.slice(4, 2)])
    table = hl.Table.parallelize(arrow, key="id", n_partitions=2)
    path = tmp_path / "sliced-structs"
    table.write(path)
    reopened = hl.read_table(path)
    assert reopened.collect() == table.collect()
    assert reopened.to_arrow().equals(table.to_arrow(), check_metadata=True)
    selected = reopened.filter((reopened.id >= 2) & (reopened.id < 5))
    assert selected.collect() == table.filter((table.id >= 2) & (table.id < 5)).collect()
    assert [row.present for row in selected.select(present=hl.is_defined(selected.value)).collect()] == [False, True, True]
    assert [row.present for row in selected.select(present=hl.is_defined(selected.value.child)).collect()] == [False, False, True]
