import unittest

import vail as hl


class FilterTests(unittest.TestCase):
    def test_keep_and_drop_preserve_order_and_missing_is_dropped(self):
        table = hl.Table.parallelize(
            [{"k": i, "keep": value} for i, value in enumerate([True, False, None, True])],
            schema=hl.tstruct(k=hl.tint32, keep=hl.tbool),
            key="k",
            n_partitions=3,
        )

        kept = table.filter(table.keep)
        self.assertEqual([row.k for row in kept.collect()], [0, 3])
        self.assertEqual(list(kept.key), ["k"])
        self.assertEqual([row.k for row in table.filter(table.keep, keep=False).collect()], [1])
        self.assertEqual(table.filter(hl.missing(hl.tbool)).count(), 0)
        self.assertEqual([row.k for row in table.filter(True).collect()], [0, 1, 2, 3])

    def test_zero_column_row_count_and_empty_partitions(self):
        table = hl.Table.parallelize(
            [{"flag": True}, {"flag": False}],
            schema=hl.tstruct(flag=hl.tbool),
            n_partitions=4,
        ).select()

        kept = table.filter(True)
        self.assertEqual(kept.count(), 2)
        self.assertEqual(kept.collect(), [hl.Struct(), hl.Struct()])
        self.assertEqual(kept.n_partitions(), 4)
        self.assertEqual(table.filter(False).count(), 0)

    def test_filter_composes_in_order_with_head_and_projection(self):
        table = hl.Table.parallelize(
            [{"x": i, "flag": i % 2 == 0} for i in range(6)],
            schema=hl.tstruct(x=hl.tint32, flag=hl.tbool),
            n_partitions=3,
        )

        projected = table.select(x=table.x, pass_row=table.flag)
        projected = projected.filter(projected.pass_row)
        self.assertEqual([row.x for row in projected.collect()], [0, 2, 4])
        prefix = table.head(3)
        self.assertEqual(prefix.filter(prefix.flag).count(), 2)
        self.assertEqual(table.filter(table.flag).head(1).count(), 1)
        suffix = table.tail(3)
        self.assertEqual([row.x for row in suffix.filter(suffix.flag).collect()], [4])
        self.assertEqual([row.x for row in table.filter(table.flag).tail(1).collect()], [4])
        self.assertEqual([row.x for row in table.filter(table.flag).order_by(hl.desc("x")).collect()], [4, 2, 0])

    def test_source_and_type_validation(self):
        table = hl.range_table(3)
        other = hl.range_table(3)
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            table.filter(other.idx > 0)
        projected = table.select(y=table.idx)
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            projected.filter(table.idx > 0)
        with self.assertRaisesRegex(TypeError, "must be bool"):
            table.filter(table.idx)
        with self.assertRaises(TypeError):
            table.filter(True, keep=1)


if __name__ == "__main__":
    unittest.main()
