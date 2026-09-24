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

    def test_source_validation_and_unsupported_paths(self):
        first = hl.range_table(3, n_partitions=2)
        second = hl.range_table(3)
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            first.order_by(second.idx)
        with self.assertRaises(ValueError):
            first.take(-1)
        with self.assertRaises(TypeError):
            first.head(True)
        with self.assertRaises(NotImplementedError):
            first.collect(_localize=False)
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


if __name__ == "__main__":
    unittest.main()
