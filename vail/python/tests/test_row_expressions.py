import unittest
import math

import vail as hl


class RowExpressionTests(unittest.TestCase):
    def test_annotate_replaces_in_place_and_appends_typed_missing_expression(self):
        table = hl.Table.parallelize(
            [{"k": 1, "a": 4, "b": 8}, {"k": 2, "a": 9, "b": 3}],
            schema=hl.tstruct(k=hl.tint32, a=hl.tint32, b=hl.tint32),
        ).key_by("k")

        result = table.annotate(
            a=table.a + hl.int32(1),
            c=hl.if_else(table.b > 5, hl.int64(table.a), hl.missing(hl.tint64)),
        )

        self.assertEqual(list(result.row), ["k", "a", "b", "c"])
        self.assertEqual(result.row.dtype, hl.tstruct(k=hl.tint32, a=hl.tint32, b=hl.tint32, c=hl.tint64))
        self.assertEqual(
            result.collect(),
            [hl.Struct(k=1, a=5, b=8, c=4), hl.Struct(k=2, a=10, b=3, c=None)],
        )

    def test_float32_division_preserves_dtype_and_stale_sources_are_rejected(self):
        table = hl.Table.parallelize([{"x": 3.0}], schema=hl.tstruct(x=hl.tfloat32))
        result = table.annotate(y=table.x / hl.float32(2))
        self.assertEqual(result.y.dtype, hl.tfloat32)
        self.assertEqual(result.y.collect(), [1.5])
        with self.assertRaisesRegex(ValueError, "source|mismatch"):
            result.annotate(z=table.x)

    def test_select_keys_first_and_zero_field_shapes(self):
        table = hl.Table.parallelize(
            [{"k": 2, "a": 20, "b": "x"}, {"k": 1, "a": 10, "b": "y"}],
            schema=hl.tstruct(k=hl.tint32, a=hl.tint32, b=hl.tstr),
        ).key_by("k")

        selected = table.select(table.b, value=table.a + hl.int32(1))
        self.assertEqual(list(selected.row), ["k", "b", "value"])
        self.assertEqual(list(selected.key), ["k"])
        self.assertEqual(
            selected.collect(),
            [hl.Struct(k=1, b="y", value=11), hl.Struct(k=2, b="x", value=21)],
        )

        keys_only = table.select()
        self.assertEqual(list(keys_only.row), ["k"])
        self.assertEqual(keys_only.collect(), [hl.Struct(k=1), hl.Struct(k=2)])

        zero_field = hl.Table.parallelize([{}, {}, {}], schema=hl.tstruct()).select()
        self.assertEqual(list(zero_field.row), [])
        self.assertEqual(zero_field.collect(), [hl.Struct(), hl.Struct(), hl.Struct()])

    def test_transmute_drops_old_nonkeys_but_keeps_keys_and_outputs(self):
        table = hl.Table.parallelize(
            [
                {"k": "a", "left": 3, "right": 7, "discard": True},
                {"k": "b", "left": 5, "right": 11, "discard": False},
            ],
            schema=hl.tstruct(k=hl.tstr, left=hl.tint32, right=hl.tint32, discard=hl.tbool),
        ).key_by("k")

        result = table.transmute(total=table.left + table.right, keep=table.discard)
        self.assertEqual(list(result.row), ["k", "total", "keep"])
        self.assertEqual(
            result.collect(),
            [hl.Struct(k="a", total=10, keep=True), hl.Struct(k="b", total=16, keep=False)],
        )

    def test_nested_struct_missing_parent_propagates_through_if_else(self):
        table = hl.Table.parallelize(
            [{"g": {"active": True, "x": 4}}, {"g": None}],
            schema=hl.tstruct(g=hl.tstruct(active=hl.tbool, x=hl.tint32)),
        )

        result = table.annotate(
            y=hl.if_else(table.g.active, table.g.x + hl.int32(2), hl.missing(hl.tint32))
        )
        self.assertEqual(result.y.collect(), [6, None])

    def test_typed_nan_is_distinct_from_a_struct_float_tag_field(self):
        table = hl.Table.parallelize([{"id": 1}], schema=hl.tstruct(id=hl.tint32))
        result = table.annotate(
            value=hl.float64(float("nan")),
            payload=hl.struct(**{"$float": hl.literal("nan")}),
        ).collect()[0]

        self.assertTrue(math.isnan(result.value))
        self.assertEqual(result["payload"]["$float"], "nan")

    def test_int32_arithmetic_wraps_and_missing_if_else_skips_dead_branch(self):
        table = hl.Table.parallelize(
            [
                {"x": 2**31 - 1, "zero": 0, "condition": None},
                {"x": -(2**31), "zero": 0, "condition": None},
            ],
            schema=hl.tstruct(x=hl.tint32, zero=hl.tint32, condition=hl.tbool),
        )
        result = table.annotate(
            incremented=table.x + hl.int32(1),
            negated=-table.x,
            guarded=hl.if_else(table.condition, table.x // table.zero, hl.int32(7)),
        ).collect()

        self.assertEqual([row.incremented for row in result], [-(2**31), -(2**31) + 1])
        self.assertEqual([row.negated for row in result], [-(2**31) + 1, -(2**31)])
        self.assertEqual([row.guarded for row in result], [None, None])

    def test_rejects_key_overwrite_foreign_sources_and_duplicate_selection(self):
        table = hl.Table.parallelize(
            [{"k": 1, "v": 3}], schema=hl.tstruct(k=hl.tint32, v=hl.tint32)
        ).key_by("k")
        other = hl.Table.parallelize(
            [{"k": 2, "v": 5}], schema=hl.tstruct(k=hl.tint32, v=hl.tint32)
        )

        with self.assertRaisesRegex(ValueError, "key"):
            table.annotate(k=table.v + hl.int32(1))
        with self.assertRaisesRegex(ValueError, "source|mismatch"):
            table.select(other.v)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            table.select(table.v, table.v)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            table.select(table.v, v=table.k)

    def test_order_and_head_remain_in_sequence_with_projection(self):
        table = hl.Table.parallelize(
            [
                {"rank": 7, "name": "zed"},
                {"rank": 1, "name": "amy"},
                {"rank": 5, "name": "bob"},
                {"rank": 3, "name": "cal"},
            ],
            schema=hl.tstruct(rank=hl.tint32, name=hl.tstr),
        )

        before_projection = table.order_by("rank").head(3).select("name").order_by("name")
        self.assertEqual(before_projection.collect(), [hl.Struct(name="amy"), hl.Struct(name="bob"), hl.Struct(name="cal")])

        after_projection = table.select("name", rank=table.rank).order_by("rank").head(2)
        self.assertEqual(after_projection.collect(), [hl.Struct(name="amy", rank=1), hl.Struct(name="cal", rank=3)])

    def test_row_projection_keeps_literal_ownership_and_lazy_errors(self):
        values = [3, None]
        table = hl.Table.parallelize([{"x": 2}, {"x": 4}], schema=hl.tstruct(x=hl.tint32))
        projected = table.annotate(y=table.x + 3, values=hl.literal(values, hl.tarray(hl.tint32)))
        projected = projected.select("y", "values")
        values[:] = [99]
        del table
        self.assertEqual(projected.collect(), [hl.Struct(y=5, values=[3, None]), hl.Struct(y=7, values=[3, None])])
        lazy = projected.select(value=hl.int32('invalid'))
        with self.assertRaises(RuntimeError):
            lazy.collect()


if __name__ == "__main__":
    unittest.main()
