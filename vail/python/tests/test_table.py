import json
import unittest

import vail as hl


class TableTests(unittest.TestCase):
    def test_global_key_order_and_missing(self):
        table = hl.Table.parallelize(
            [{"k": k, "label": str(i)} for i, k in enumerate([3, None, 1, 4, 2])],
            schema=hl.tstruct(k=hl.tint32, label=hl.tstr), n_partitions=3,
        ).key_by("k")
        self.assertEqual([row.k for row in table.collect()], [1, 2, 3, 4, None])
        self.assertEqual(list(table.key), ["k"])
        self.assertEqual(list(table.row_value), ["label"])
        self.assertEqual(table.key.dtype, hl.tstruct(k=hl.tint32))

    def test_descending_nulls_last_and_unkey(self):
        table = hl.Table.parallelize([{"k": k} for k in [2, None, 1, 4]], key="k", n_partitions=2)
        ordered = table.order_by(hl.desc(table.k))
        self.assertEqual([row.k for row in ordered.collect()], [4, 2, 1, None])
        self.assertEqual(list(ordered.key), [])
        self.assertEqual(list(table.key), ["k"])

    def test_head_before_resort_is_not_moved_after_sort(self):
        table = hl.Table.parallelize([{"k": k} for k in [9, 1, 5, 3]], n_partitions=3)
        result = table.order_by("k").head(2).order_by(hl.desc("k"))
        self.assertEqual([row.k for row in result.collect()], [3, 1])
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.take(1), [hl.Struct(k=3)])

    def test_empty_and_zero_column_tables(self):
        empty = hl.Table.parallelize([], schema=hl.tstruct(k=hl.tint64), n_partitions=3).key_by("k")
        self.assertEqual(empty.collect(), [])
        self.assertEqual(empty.count(), 0)
        table = hl.Table.parallelize([{}, {}, {}], schema=hl.tstruct(), n_partitions=2)
        self.assertEqual(table.count(), 3)
        self.assertEqual(table.collect(), [hl.Struct(), hl.Struct(), hl.Struct()])
        self.assertEqual(table.head(0).collect(), [])

    def test_duplicate_keys_and_compound_order(self):
        rows = [{"a": 1, "b": "b"}, {"a": 1, "b": "a"}, {"a": 1, "b": "a"}, {"a": 0, "b": "z"}]
        table = hl.Table.parallelize(rows, n_partitions=3).key_by("a", "b")
        self.assertEqual([(r.a, r.b) for r in table.collect()], [(0, "z"), (1, "a"), (1, "a"), (1, "b")])

    def test_union_and_distinct_lower_lazily(self):
        left = hl.Table.parallelize(
            [{"k": 1, "value": "left"}, {"k": 2, "value": "two"}],
            schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k", n_partitions=2,
        )
        right = hl.Table.parallelize(
            [{"k": 1, "value": "right"}, {"k": 3, "value": "three"}],
            schema=hl.tstruct(k=hl.tint32, value=hl.tstr), key="k",
        )
        union = left.union(right)
        self.assertEqual(union.count(), 4)
        self.assertEqual(union._native.ir()["node"], "TableUnion")
        self.assertEqual([row.k for row in union.collect()], [1, 1, 2, 3])
        self.assertEqual({row.k for row in union.distinct().collect()}, {1, 2, 3})

    def test_unkeyed_union_preserves_partitions_and_rows(self):
        left = hl.Table.parallelize([{"x": i} for i in [3, 1]], n_partitions=3)
        right = hl.Table.parallelize([{"x": i} for i in [2, 0]], n_partitions=2)
        union = left.union(right)
        self.assertEqual(union.collect(), [hl.Struct(x=i) for i in [3, 1, 2, 0]])
        self.assertEqual(union.collect(), [hl.Struct(x=i) for i in [3, 1, 2, 0]])
        self.assertEqual(union.n_partitions(), 5)
        with self.assertRaises(ValueError):
            left.distinct()

    def test_union_rejects_schema_mismatch(self):
        left = hl.Table.parallelize([{"x": 1}], schema=hl.tstruct(x=hl.tint32))
        right = hl.Table.parallelize([{"x": "one"}], schema=hl.tstruct(x=hl.tstr))
        with self.assertRaises(ValueError):
            left.union(right)

    def test_union_unifies_numeric_fields_and_missing_tuple_fields(self):
        base = hl.range_table(2)
        integer = base.annotate(x=hl.int32(1), y="A")
        tupled = base.annotate(z=(1, 2, 3), x=hl.float64(1.5))
        union = base.union(integer, tupled, unify=True)
        self.assertEqual(list(union.row), ["idx", "x", "y", "z"])
        self.assertEqual(union.x.dtype, hl.tfloat64)
        self.assertEqual(union.z.dtype, hl.ttuple(hl.tint32, hl.tint32, hl.tint32))
        self.assertEqual(
            union.collect(),
            [
                hl.Struct(idx=0, x=None, y=None, z=None),
                hl.Struct(idx=0, x=1.0, y="A", z=None),
                hl.Struct(idx=0, x=1.5, y=None, z=(1, 2, 3)),
                hl.Struct(idx=1, x=None, y=None, z=None),
                hl.Struct(idx=1, x=1.0, y="A", z=None),
                hl.Struct(idx=1, x=1.5, y=None, z=(1, 2, 3)),
            ],
        )
        with self.assertRaises(ValueError):
            base.union(integer, unify=False)

    def test_union_unifies_nested_numeric_fields(self):
        left = hl.Table.parallelize(
            [{"k": 0, "nested": {"x": [1, 2]}}],
            schema=hl.tstruct(k=hl.tint32, nested=hl.tstruct(x=hl.tarray(hl.tint32))),
            key="k",
        )
        right = hl.Table.parallelize(
            [{"k": 1, "nested": {"x": [1.5]}}],
            schema=hl.tstruct(k=hl.tint32, nested=hl.tstruct(x=hl.tarray(hl.tfloat64))),
            key="k",
        )
        union = left.union(right, unify=True)
        self.assertEqual(union.nested.dtype, hl.tstruct(x=hl.tarray(hl.tfloat64)))
        self.assertEqual([row.nested.x for row in union.collect()], [[1.0, 2.0], [1.5]])

    def test_union_unifies_tuple_and_dictionary_values(self):
        left = hl.Table.parallelize(
            [{"value": (1, {"a": 2})}],
            schema=hl.tstruct(value=hl.ttuple(hl.tint32, hl.tdict(hl.tstr, hl.tint32))),
        )
        right = hl.Table.parallelize(
            [{"value": (3.5, {"b": 4.5})}],
            schema=hl.tstruct(value=hl.ttuple(hl.tfloat64, hl.tdict(hl.tstr, hl.tfloat64))),
        )
        union = left.union(right, unify=True)
        self.assertEqual(union.value.dtype, hl.ttuple(hl.tfloat64, hl.tdict(hl.tstr, hl.tfloat64)))
        self.assertEqual([row.value for row in union.collect()], [(1.0, {"a": 2.0}), (3.5, {"b": 4.5})])

    def test_union_promotes_int64_and_float32_without_precision_loss(self):
        integers = hl.Table.parallelize([{"v": 2**40}], schema=hl.tstruct(v=hl.tint64))
        floats = hl.Table.parallelize([{"v": 1.5}], schema=hl.tstruct(v=hl.tfloat32))
        union = integers.union(floats, unify=True)
        self.assertEqual(union.v.dtype, hl.tfloat64)
        self.assertEqual([row.v for row in union.collect()], [float(2**40), 1.5])

    def test_union_unify_puts_key_fields_first(self):
        left = hl.Table.parallelize([{"value": "a", "key": 1}], key="key")
        right = hl.Table.parallelize([{"key": 2, "extra": True, "value": "b"}], key="key")
        union = left.union(right, unify=True)
        self.assertEqual(list(union.row), ["key", "value", "extra"])

    def test_union_rejects_key_and_global_schema_mismatches(self):
        left = hl.Table.parallelize([{"k": 1, "v": 1}], key="k")
        unkeyed = hl.Table.parallelize([{"k": 1, "v": 1}])
        with self.assertRaises(ValueError):
            left.union(unkeyed)
        wrong_key_type = hl.Table.parallelize(
            [{"k": 1, "v": 1}], schema=hl.tstruct(k=hl.tint64, v=hl.tint32), key="k"
        )
        with self.assertRaises(ValueError):
            left.union(wrong_key_type, unify=True)
        # Hail union carries the first child's globals; a later child's globals
        # are not part of row-schema unification or compatibility checking.
        with_globals = left.annotate_globals(g=1)
        union = with_globals.union(left)
        self.assertEqual(union._global_schema, hl.tstruct(g=hl.tint32))

    def test_distinct_uses_hail_null_key_equality(self):
        table = hl.Table.parallelize(
            [{"k": 1, "v": 0}, {"k": None, "v": 1}, {"k": None, "v": 2}, {"k": 1, "v": 3}],
            schema=hl.tstruct(k=hl.tint32, v=hl.tint32), key="k", n_partitions=3,
        )
        self.assertEqual([row.k for row in table.distinct().collect()], [1, None])

    def test_distinct_returns_an_original_whole_row(self):
        table = hl.Table.parallelize(
            [{"k": 1, "a": 10, "b": None}, {"k": 1, "a": None, "b": 20}],
            schema=hl.tstruct(k=hl.tint32, a=hl.tint32, b=hl.tint32),
            key="k",
        )
        row = table.distinct().collect()[0]
        self.assertIn((row.a, row.b), {(10, None), (None, 20)})

    def test_source_validation_and_unsupported_paths(self):
        first = hl.range_table(3, n_partitions=2)
        second = hl.range_table(3)
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            first.order_by(second.idx)
        with self.assertRaises(ValueError):
            first.take(-1)
        with self.assertRaises(TypeError):
            first.head(True)
        self.assertEqual(hl.eval(first.collect(_localize=False)), first.collect())
        with self.assertRaises(ValueError):
            hl.Table.parallelize([{"k": None}])
        with self.assertRaises(TypeError):
            hl.Table.parallelize([{"k": True}], schema=hl.tstruct(k=hl.tint32))
        with self.assertRaises(OverflowError):
            hl.Table.parallelize([{"k": 2**32}], schema=hl.tstruct(k=hl.tint32))

    def test_int64_bool_and_collect_field_order(self):
        table = hl.Table.parallelize(
            [{"z": 2**40, "a": True}, {"z": -(2**40), "a": False}], n_partitions=2
        ).key_by("a", "z")
        result = table.collect()
        self.assertEqual([row.z for row in result], [-(2**40), 2**40])
        self.assertEqual(list(result[0]), ["z", "a"])
        self.assertEqual(table.row.dtype["z"], hl.tint64)

    def test_expressions_are_lazy_and_immutable(self):
        table = hl.range_table(3)
        with self.assertRaises(TypeError):
            bool(table.idx)
        self.assertEqual((table.idx == 1).collect(), [False, True, False])
        self.assertEqual((table.idx + 1).collect(), [1, 2, 3])
        with self.assertRaises(TypeError):
            table.idx = 42
        with self.assertRaises(TypeError):
            del table._keys

    def test_drop_rename_and_globals(self):
        table = hl.Table.parallelize(
            [{"id": 1, "value": 10}, {"id": 2, "value": 20}], key="id"
        ).annotate_globals(version=1)
        renamed = table.rename({"id": "sample", "value": "amount", "version": "release"})
        self.assertEqual(list(renamed.key), ["sample"])
        self.assertEqual(renamed.collect(), [hl.Struct(sample=1, amount=10), hl.Struct(sample=2, amount=20)])
        self.assertEqual(renamed.aggregate(renamed.release), 1)
        dropped = renamed.drop("amount", "release")
        self.assertEqual(dropped.collect(), [hl.Struct(sample=1), hl.Struct(sample=2)])
        with self.assertRaises(ValueError):
            table.drop("id")
        nested = hl.Table.parallelize([{"foo": {"bar": 1}, "bar": 2}])
        with self.assertRaises(hl.ExpressionException):
            nested.drop(nested.foo.bar)
        with self.assertRaises(ValueError):
            table.rename({"id": "value"})
        with self.assertRaises(ValueError):
            table.rename({"id": "version"})

    def test_flatten_nested_struct_fields(self):
        table = hl.Table.parallelize([
            {"outer": {"inner": {"x": 1}, "tag": "a"}, "keep": 2}
        ])
        flat = table.flatten()
        self.assertEqual(list(flat.row), ["outer.inner.x", "outer.tag", "keep"])
        self.assertEqual(flat.collect(), [hl.Struct(**{"outer.inner.x": 1, "outer.tag": "a", "keep": 2})])
        self.assertEqual(list(flat.key), [])

    def test_flatten_keyed_table_and_missing_struct(self):
        table = hl.Table.parallelize(
            [{"key": {"part": key}, "value": str(key)} for key in [3, 1, 2]],
            schema=hl.tstruct(key=hl.tstruct(part=hl.tint32), value=hl.tstr),
        ).key_by("key")
        flat = table.flatten()
        self.assertEqual(list(flat.key), [])
        self.assertEqual(flat.collect(), [
            hl.Struct(**{"key.part": 1, "value": "1"}),
            hl.Struct(**{"key.part": 2, "value": "2"}),
            hl.Struct(**{"key.part": 3, "value": "3"}),
        ])
        missing = hl.Table.parallelize(
            [{"outer": None}], schema=hl.tstruct(outer=hl.tstruct(inner=hl.tint32))
        ).flatten()
        self.assertEqual(missing.collect(), [hl.Struct(**{"outer.inner": None})])

    def test_expand_types_tuple_and_array(self):
        table = hl.Table.parallelize(
            [{"value": (1, "x"), "items": [2, 3]}],
            schema=hl.tstruct(value=hl.ttuple(hl.tint32, hl.tstr), items=hl.tarray(hl.tint32)),
        )
        expanded = table.expand_types()
        self.assertEqual(expanded.collect(), [hl.Struct(value=hl.Struct(_0=1, _1="x"), items=[2, 3])])
        expanded = table.annotate_globals(meta=("build", 1)).expand_types()
        self.assertEqual(expanded.aggregate(expanded.meta._0), "build")

    def test_expand_types_set_dict_call_ndarray_and_globals(self):
        import numpy as np

        table = hl.Table.parallelize(
            [{
                "values": {2, 1},
                "mapping": {1: "one", 2: "two"},
                "genotype": hl.Call((0, 1), phased=True),
                "tensor": np.array([[1, 2], [3, 4]], dtype=np.int32),
                "nested": {"items": {3, 4}, "call": hl.Call((2, 3), phased=False)},
            }],
            schema=hl.tstruct(
                values=hl.tset(hl.tint32),
                mapping=hl.tdict(hl.tint32, hl.tstr),
                genotype=hl.tcall,
                tensor=hl.tndarray(hl.tint32, 2),
                nested=hl.tstruct(items=hl.tset(hl.tint32), call=hl.tcall),
            ),
        ).annotate_globals(
            labels=hl.literal({"left": 1, "right": 2}, hl.tdict(hl.tstr, hl.tint32)),
            global_call=hl.Call((1, 1), phased=False),
        )
        expanded = table.expand_types()
        row = expanded.collect()[0]
        self.assertEqual(row["values"], [1, 2])
        self.assertEqual(row["mapping"], [hl.Struct(key=1, value="one"), hl.Struct(key=2, value="two")])
        self.assertEqual(row.genotype, hl.Struct(alleles=[0, 1], phased=True))
        self.assertEqual(row.tensor, hl.Struct(shape=[2, 2], data=[1, 2, 3, 4]))
        self.assertEqual(row.nested, hl.Struct(items=[3, 4], call=hl.Struct(alleles=[2, 3], phased=False)))
        self.assertEqual(expanded.aggregate(expanded.labels.map(lambda entry: entry.key)), ["left", "right"])
        self.assertEqual(expanded.aggregate(expanded.global_call.alleles), [1, 1])
        missing_call = hl.Table.parallelize([{"call": None}], schema=hl.tstruct(call=hl.tcall)).expand_types()
        self.assertEqual(missing_call.collect(), [hl.Struct(call=hl.Struct(alleles=None, phased=None))])


if __name__ == "__main__":
    unittest.main()
