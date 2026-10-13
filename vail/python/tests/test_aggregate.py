import unittest
import math

import vail as hl


class AggregateTests(unittest.TestCase):
    def setUp(self):
        self.table = hl.Table.parallelize(
            [{"x": 1, "keep": True, "xs": [1, 2]},
             {"x": 2, "keep": False, "xs": [3]},
             {"x": None, "keep": True, "xs": []}],
            schema=hl.tstruct(x=hl.tint32, keep=hl.tbool, xs=hl.tarray(hl.tint32)),
            n_partitions=2,
        )

    def test_total_aggregates_and_struct_result(self):
        result = self.table.aggregate(hl.struct(
            n=hl.agg.count(), total=hl.agg.sum(self.table.x),
            values=hl.agg.collect(self.table.x), stats=hl.agg.stats(self.table.x),
        ))
        self.assertEqual(result.n, 3)
        self.assertEqual(result.total, 3)
        self.assertEqual(result["values"], [1, 2, None])
        self.assertEqual(result.stats, hl.Struct(mean=1.5, stdev=0.5, min=1.0,
                                                  max=2.0, n=2, sum=3.0))
        self.assertEqual(list(hl.agg.stats(self.table.x).dtype),
                         ["mean", "stdev", "min", "max", "n", "sum"])

    def test_filtered_and_exploded_aggregates(self):
        self.assertEqual(self.table.aggregate(
            hl.agg.filter(self.table.keep, hl.agg.sum(self.table.x))), 1)
        self.assertEqual(self.table.aggregate(
            hl.agg.explode(lambda x: hl.agg.sum(x), self.table.xs)), 6)
        set_table = hl.Table.parallelize([{"xs": {1, 2}}, {"xs": {3}}],
                                        schema=hl.tstruct(xs=hl.tset(hl.tint32)))
        self.assertEqual(set_table.aggregate(
            hl.agg.explode(lambda x: hl.agg.sum(x), set_table.xs)), 6)

    def test_reducers_on_empty_and_all_missing_tables(self):
        for table, expected_count, expected_values, expected_fraction in (
            (hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint64, b=hl.tbool)), 0, [], None),
            (hl.Table.parallelize([{"x": None, "b": None}, {"x": None, "b": None}],
                                  schema=hl.tstruct(x=hl.tint64, b=hl.tbool)), 2, [None, None], 0.0),
        ):
            x, b = table.x, table.b
            result = table.aggregate({
                "count": hl.agg.count(),
                "sum": hl.agg.sum(x),
                "product": hl.agg.product(x),
                "min": hl.agg.min(x),
                "max": hl.agg.max(x),
                "mean": hl.agg.mean(x),
                "collect": hl.agg.collect(x),
                "count_where": hl.agg.count_where(b),
                "fraction": hl.agg.fraction(b),
                "any": hl.agg.any(b),
                "all": hl.agg.all(b),
                "stats": hl.agg.stats(x),
            })
            self.assertEqual(result.count, expected_count)
            self.assertEqual(result.sum, 0)
            self.assertEqual(result.product, 1)
            self.assertIsNone(result.min)
            self.assertIsNone(result.max)
            self.assertTrue(math.isnan(result.mean))
            self.assertEqual(result.collect, expected_values)
            self.assertEqual(result.count_where, 0)
            self.assertEqual(result.fraction, expected_fraction)
            self.assertFalse(result.any)
            self.assertTrue(result.all)
            self.assertEqual(result.stats.n, 0)
            self.assertEqual(result.stats.sum, 0.0)
            self.assertTrue(math.isnan(result.stats.mean))
            self.assertTrue(math.isnan(result.stats.stdev))
            self.assertIsNone(result.stats.min)
            self.assertIsNone(result.stats.max)

    def test_integer_reducers_preserve_values_above_float_precision(self):
        value = 2**53 + 17
        table = hl.Table.parallelize([{"x": value}], schema=hl.tstruct(x=hl.tint64))
        result = table.aggregate({"sum": hl.agg.sum(table.x),
                                  "product": hl.agg.product(table.x),
                                  "min": hl.agg.min(table.x),
                                  "max": hl.agg.max(table.x),
                                  "mean": hl.agg.mean(table.x),
                                  "collect": hl.agg.collect(table.x),
                                  "count_where": hl.agg.count_where(table.x > value - 1),
                                  "fraction": hl.agg.fraction(table.x > value - 1),
                                  "any": hl.agg.any(table.x > value - 1),
                                  "all": hl.agg.all(table.x > value - 1),
                                  "stats": hl.agg.stats(table.x)})
        self.assertEqual(result.sum, value)
        self.assertEqual(result.product, value)
        self.assertEqual(result.min, value)
        self.assertEqual(result.max, value)
        self.assertEqual(result.mean, float(value))
        self.assertEqual(result.collect, [value])
        self.assertEqual(result.count_where, 1)
        self.assertEqual(result.fraction, 1.0)
        self.assertTrue(result.any)
        self.assertTrue(result.all)
        self.assertEqual(result.stats.n, 1)
        self.assertEqual(result.stats.sum, float(value))

    def test_float_minmax_follow_arrow_nan_policy(self):
        table = hl.Table.parallelize(
            [{"x": float("nan")}, {"x": 1.5}, {"x": None}],
            schema=hl.tstruct(x=hl.tfloat64),
        )
        self.assertEqual(table.aggregate(hl.agg.min(table.x)), 1.5)
        self.assertTrue(math.isnan(table.aggregate(hl.agg.max(table.x))))
        all_nan = hl.Table.parallelize([{"x": float("nan")}, {"x": float("nan")}],
                                       schema=hl.tstruct(x=hl.tfloat64))
        self.assertTrue(math.isnan(all_nan.aggregate(hl.agg.min(all_nan.x))))
        self.assertTrue(math.isnan(all_nan.aggregate(hl.agg.max(all_nan.x))))
        all_missing = hl.Table.parallelize([{"x": None}, {"x": None}],
                                           schema=hl.tstruct(x=hl.tfloat64))
        self.assertIsNone(all_missing.aggregate(hl.agg.min(all_missing.x)))
        self.assertIsNone(all_missing.aggregate(hl.agg.max(all_missing.x)))

    def test_source_and_scope_validation(self):
        other = hl.range_table(2)
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            self.table.aggregate(hl.agg.sum(other.idx))
        with self.assertRaisesRegex(ValueError, "inside an aggregate"):
            self.table.aggregate(self.table.x)
        with self.assertRaisesRegex(ValueError, "nested aggregations"):
            self.table.aggregate(hl.agg.sum(hl.agg.count()))

    def test_global_references_lazy_result_and_zero_row_count(self):
        table = hl.Table.parallelize([{"x": 1}, {"x": 2}], schema=hl.tstruct(x=hl.tint32),
                                    globals={"offset": 10})
        lazy_result = table.aggregate(hl.agg.sum(table.x + table.offset), _localize=False)
        self.assertIsInstance(lazy_result, hl.Expression)
        self.assertEqual(hl.eval(lazy_result), 23)
        empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32), globals={"offset": 10})
        self.assertEqual(empty.aggregate(hl.agg.count()), 0)
        self.assertEqual(empty.aggregate(empty.offset), 10)
        with self.assertRaisesRegex(TypeError, "_localize must be a bool"):
            table.aggregate(hl.agg.count(), _localize=1)

    def test_literal_data_resembling_aggregate_ir_is_not_traversed(self):
        value = hl.literal({"op": "aggregate", "function": "count", "value": "payload"})
        result = self.table.aggregate(hl.agg.collect(value))
        expected = {"op": "aggregate", "function": "count", "value": "payload"}
        self.assertEqual([dict(item) for item in result], [expected] * 3)


if __name__ == "__main__":
    unittest.main()
