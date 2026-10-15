import json

import pytest

import vail as hl


def test_explode_array_is_lazy_and_drops_empty_or_missing_arrays():
    table = hl.Table.parallelize(
        [{"id": 0, "xs": [2, None, 3]}, {"id": 1, "xs": []}, {"id": 2, "xs": None}],
        schema=hl.tstruct(id=hl.tint32, xs=hl.tarray(hl.tint32)),
        key="id",
        n_partitions=2,
    )
    exploded = table.explode("xs")
    assert exploded._native.ir()["node"] == "TableExplode"
    assert exploded._schema == hl.tstruct(id=hl.tint32, xs=hl.tint32)
    assert exploded.count() == 3
    assert [(row.id, row.xs) for row in exploded.collect()] == [(0, 2), (0, None), (0, 3)]
    assert [(row.id, row.xs) for row in table.collect()] == [
        (0, [2, None, 3]), (1, []), (2, None)
    ]


def test_explode_set_and_nested_array():
    sets = hl.Table.parallelize(
        [{"id": 0, "xs": {3, 1, 2}}],
        schema=hl.tstruct(id=hl.tint32, xs=hl.tset(hl.tint32)),
    )
    assert {row.xs for row in sets.explode("xs").collect()} == {1, 2, 3}

    nested = hl.Table.parallelize(
        [{"id": 0, "foo": {"label": "a", "bar": [4, 5]}},
         {"id": 1, "foo": None}],
        schema=hl.tstruct(id=hl.tint32, foo=hl.tstruct(label=hl.tstr, bar=hl.tarray(hl.tint32))),
    )
    rows = nested.explode(nested.foo.bar).collect()
    assert [(row.id, row.foo.label, row.foo.bar) for row in rows] == [(0, "a", 4), (0, "a", 5)]


def test_explode_renamed_field_and_key_restrictions():
    table = hl.Table.parallelize(
        [{"id": 0, "xs": [1, 2]}],
        schema=hl.tstruct(id=hl.tint32, xs=hl.tarray(hl.tint32)),
    )
    renamed = table.explode("xs", name="x")
    assert list(renamed._schema) == ["id", "x"]
    assert [row.x for row in renamed.collect()] == [1, 2]
    with pytest.raises(ValueError, match="key field"):
        table.key_by("xs").explode("xs")
    nested = table.annotate(foo=hl.struct(bar=table.xs))
    with pytest.raises(ValueError, match="nested"):
        nested.explode(nested.foo.bar, name="x")
