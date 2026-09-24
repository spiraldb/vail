import unittest

import vail as hl


class TailPartitionTests(unittest.TestCase):
    def test_head_tail_preserve_sequence_and_partitions(self):
        table = hl.utils.range_table(10, n_partitions=3)
        self.assertEqual(table.n_partitions(), 3)
        self.assertEqual(table.head(7).idx.collect(), list(range(7)))
        self.assertEqual(table.head(7).n_partitions(), 2)
        self.assertEqual(table.tail(5).idx.collect(), list(range(5, 10)))
        self.assertEqual(table.tail(5).n_partitions(), 2)
        self.assertEqual(table.head(0).n_partitions(), 0)
        self.assertEqual(table.tail(0).n_partitions(), 0)
        self.assertEqual(table.tail(20).idx.collect(), list(range(10)))

    def test_more_partitions_than_rows_and_force_count(self):
        table = hl.utils.range_table(10, n_partitions=11)
        self.assertEqual(table.n_partitions(), 10)
        parallel = hl.Table.parallelize([{"x": x} for x in range(10)], n_partitions=11)
        self.assertEqual(parallel.n_partitions(), 11)
        for n in (0, 7, 9, 10, 11):
            for truncator in (table.head, table.tail):
                with self.subTest(n=n, truncator=truncator.__name__):
                    result = truncator(n)
                    self.assertEqual(result.count(), min(n, 10))
                    self.assertEqual(result._force_count(), min(n, 10))

    def test_operation_order_and_native_field_collection(self):
        table = hl.Table.parallelize([{"x": x} for x in (9, 1, 5, 3, 7)], n_partitions=3)
        result = table.order_by("x").tail(3).order_by(hl.desc("x")).head(2)
        self.assertEqual(result.x.collect(), [9, 7])
        self.assertEqual(result._force_count(), 2)
        self.assertEqual(result.n_partitions(), 1)

    def test_range_metadata_zero_and_flat_schema(self):
        table = hl.utils.range_table(26, n_partitions=5)
        self.assertEqual(table.globals.dtype, hl.tstruct())
        self.assertEqual(table.row.dtype, hl.tstruct(idx=hl.tint32))
        self.assertEqual(table.row_value.dtype, hl.tstruct())
        self.assertEqual(list(table.key), ["idx"])
        self.assertEqual(table.n_partitions(), 5)
        zero = hl.utils.range_table(0)
        self.assertEqual(zero._force_count(), 0)
        self.assertEqual(zero.idx.collect(), [])
        self.assertEqual(hl.utils.range_table(0, n_partitions=5).n_partitions(), 1)
        self.assertEqual(hl.utils.range_table(2, n_partitions=5).n_partitions(), 2)
        self.assertNotEqual(hl.tstruct(a=hl.tint32, b=hl.tstr), hl.tstruct(b=hl.tstr, a=hl.tint32))
        parallel = hl.Table.parallelize([hl.Struct(x=x) for x in range(10)], schema="struct{x: int32}", n_partitions=3)
        self.assertEqual(parallel.tail(2).x.collect(), [8, 9])
        nested = hl.Table.parallelize([], schema="struct{x: array<int32>}")
        self.assertEqual(nested.row.dtype, hl.tstruct(x=hl.tarray(hl.tint32)))
        self.assertEqual(nested.collect(), [])


if __name__ == "__main__":
    unittest.main()
