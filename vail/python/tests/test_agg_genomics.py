import unittest

import numpy as np
import vail as hl


class GenomicAggregateExpressionTests(unittest.TestCase):
    def test_call_stats_uses_packed_call_and_alleles(self):
        table = hl.Table.parallelize([{"gt": hl.Call((0, 1))}], schema=hl.tstruct(gt=hl.tcall))
        expr = hl.agg.call_stats(table.gt, hl.array(["A", "C"]))
        self.assertEqual(expr.dtype["AC"], hl.tarray(hl.tint32))
        self.assertEqual(expr.dtype["AF"], hl.tarray(hl.tfloat64))
        self.assertEqual(expr.dtype["homozygote_count"], hl.tarray(hl.tint32))
        self.assertEqual(expr._ir["value"]["op"], "struct")

    def test_hwe_options_and_inbreeding_arguments(self):
        table = hl.Table.parallelize([{"gt": hl.Call((0, 1)), "p": 0.25}],
                                     schema=hl.tstruct(gt=hl.tcall, p=hl.tfloat64))
        hwe = hl.agg.hardy_weinberg_test(table.gt, one_sided=True)
        self.assertIs(hwe._ir["options"]["one_sided"], True)
        self.assertEqual(hwe._ir["options"]["__vail_result_type"],
                         hwe.dtype._native_type)
        ib = hl.agg.inbreeding(table.gt, table.p)
        self.assertEqual(ib.dtype["f_stat"], hl.tfloat64)
        self.assertEqual(ib._ir["value"]["op"], "struct")

    def test_info_score_and_ndarray_result_types(self):
        table = hl.Table.parallelize([{"gp": [0.25, 0.5, 0.25]}],
                                     schema=hl.tstruct(gp=hl.tarray(hl.tfloat64)))
        info = hl.agg.info_score(table.gp)
        self.assertEqual(info.dtype["score"], hl.tfloat64)
        self.assertEqual(info.dtype["n_included"], hl.tint32)

        arrays = hl.Table.parallelize([{"x": {"__vail_ndarray_data": [1.] * 6,
                                               "__vail_ndarray_shape": [2, 3]}}],
                                      schema=hl.tstruct(x=hl.tndarray(hl.tfloat64, 2)))
        self.assertEqual(hl.agg.ndarray_sum(arrays.x).dtype, hl.tndarray(hl.tfloat64, 2))

    def test_rejects_wrong_inputs(self):
        with self.assertRaises(TypeError):
            hl.agg.hardy_weinberg_test(1)
        with self.assertRaises(TypeError):
            hl.agg.call_stats(hl.call(0, 1), [0, 1])


class GenomicAggregateExecutionTests(unittest.TestCase):
    def test_call_stats_empty_keeps_literal_allele_shape(self):
        table = hl.Table.parallelize([], schema=hl.tstruct(gt=hl.tcall))
        for alleles in (2, hl.array(["A", "C"])):
            result = table.aggregate(hl.agg.call_stats(table.gt, alleles))
            self.assertEqual(result.AC, [0, 0])
            self.assertEqual(result.homozygote_count, [0, 0])
            self.assertEqual(result.AN, 0)
            self.assertIsNone(result.AF)

    def test_call_stats_counts_haploid_phased_and_missing(self):
        table = hl.Table.parallelize(
            [{"gt": hl.Call((0, 0), False)}, {"gt": hl.Call((0, 1), True)},
             {"gt": hl.Call((1, 1), False)}, {"gt": hl.Call((1,), False)},
             {"gt": None}], schema=hl.tstruct(gt=hl.tcall), n_partitions=3)
        result = table.aggregate(hl.agg.call_stats(table.gt, 2))
        self.assertEqual(result.AC, [3, 4])
        self.assertEqual(result.AF, [3 / 7, 4 / 7])
        self.assertEqual(result.AN, 7)
        self.assertEqual(result.homozygote_count, [1, 1])

    def test_hwe_exact_and_empty(self):
        table = hl.Table.parallelize(
            [{"gt": hl.Call((0, 0))}, {"gt": hl.Call((0, 1))},
             {"gt": hl.Call((1, 1))}, {"gt": hl.Call((1,))}, {"gt": None}],
            schema=hl.tstruct(gt=hl.tcall), n_partitions=2)
        result = table.aggregate(hl.agg.hardy_weinberg_test(table.gt))
        self.assertAlmostEqual(result.het_freq_hwe, 0.6)
        self.assertGreaterEqual(result.p_value, 0)
        self.assertLessEqual(result.p_value, 1)
        one_sided = table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=True))
        self.assertGreaterEqual(one_sided.p_value, 0)
        self.assertLessEqual(one_sided.p_value, 1)
        empty = hl.Table.parallelize([], schema=hl.tstruct(gt=hl.tcall))
        result = empty.aggregate(hl.agg.hardy_weinberg_test(empty.gt))
        self.assertTrue(np.isnan(result.het_freq_hwe))
        self.assertEqual(result.p_value, 0.5)

    def test_inbreeding_and_invalid_call(self):
        table = hl.Table.parallelize(
            [{"gt": hl.Call((0, 0)), "p": .25},
             {"gt": hl.Call((0, 1)), "p": .25},
             {"gt": hl.Call((1, 1)), "p": None},
             {"gt": None, "p": .5}],
            schema=hl.tstruct(gt=hl.tcall, p=hl.tfloat64), n_partitions=2)
        result = table.aggregate(hl.agg.inbreeding(table.gt, table.p))
        self.assertEqual(result.n_called, 2)
        self.assertEqual(result.expected_homs, 1.25)
        self.assertEqual(result.observed_homs, 1)
        self.assertAlmostEqual(result.f_stat, -1 / 3)
        invalid = hl.Table.parallelize([{"gt": hl.Call((0, 2)), "p": .25}],
                                       schema=hl.tstruct(gt=hl.tcall, p=hl.tfloat64))
        with self.assertRaises(Exception):
            invalid.aggregate(hl.agg.inbreeding(invalid.gt, invalid.p))

    def test_info_score_missing_and_invalid_length(self):
        table = hl.Table.parallelize(
            [{"gp": [1., 0., 0.]}, {"gp": [.25, .5, .25]}, {"gp": None}],
            schema=hl.tstruct(gp=hl.tarray(hl.tfloat64)), n_partitions=2)
        result = table.aggregate(hl.agg.info_score(table.gp))
        self.assertEqual(result.n_included, 2)
        self.assertAlmostEqual(result.score, .5)
        invalid = hl.Table.parallelize([{"gp": [1., 0.]}],
                                       schema=hl.tstruct(gp=hl.tarray(hl.tfloat64)))
        with self.assertRaises(Exception):
            invalid.aggregate(hl.agg.info_score(invalid.gp))

    def test_ndarray_sum_and_shape_mismatch(self):
        dtype = hl.tndarray(hl.tfloat64, 2)
        def array(data, shape):
            return {"__vail_ndarray_data": data, "__vail_ndarray_shape": shape}
        table = hl.Table.parallelize(
            [{"x": array([1., 2., 3., 4.], [2, 2])},
             {"x": array([5., 6., 7., 8.], [2, 2])}, {"x": None}],
            schema=hl.tstruct(x=dtype), n_partitions=2)
        np.testing.assert_array_equal(table.aggregate(hl.agg.ndarray_sum(table.x)),
                                      np.array([[6., 8.], [10., 12.]]))
        bad = hl.Table.parallelize(
            [{"x": array([1., 2., 3., 4.], [2, 2])},
             {"x": array([1., 2., 3., 4.], [1, 4])}],
            schema=hl.tstruct(x=dtype), n_partitions=2)
        with self.assertRaises(Exception):
            bad.aggregate(hl.agg.ndarray_sum(bad.x))

    def test_ndarray_sum_preserves_large_int64_values(self):
        value = 2**53 + 7
        table = hl.Table.parallelize(
            [{"x": {"__vail_ndarray_data": [value], "__vail_ndarray_shape": [1]}},
             {"x": {"__vail_ndarray_data": [3], "__vail_ndarray_shape": [1]}}],
            schema=hl.tstruct(x=hl.tndarray(hl.tint64, 1)), n_partitions=2)
        np.testing.assert_array_equal(table.aggregate(hl.agg.ndarray_sum(table.x)),
                                      np.array([value + 3], dtype=np.int64))

    def test_ndarray_sum_treats_missing_element_as_zero(self):
        table = hl.Table.parallelize(
            [{"x": {"__vail_ndarray_data": [1., None], "__vail_ndarray_shape": [2]}},
             {"x": {"__vail_ndarray_data": [None, 4.], "__vail_ndarray_shape": [2]}}],
            schema=hl.tstruct(x=hl.tndarray(hl.tfloat64, 1)), n_partitions=2)
        np.testing.assert_array_equal(table.aggregate(hl.agg.ndarray_sum(table.x)),
                                      np.array([1., 4.]))

    def test_grouped_call_stats_merges_partition_states(self):
        table = hl.Table.parallelize(
            [{"group": "a", "gt": hl.Call((0, 0))},
             {"group": "b", "gt": hl.Call((0, 1))},
             {"group": "a", "gt": hl.Call((1, 1))}],
            schema=hl.tstruct(group=hl.tstr, gt=hl.tcall), n_partitions=3)
        rows = table.group_by("group").aggregate(stats=hl.agg.call_stats(table.gt, 2)).collect()
        by_group = {row.group: row.stats for row in rows}
        self.assertEqual(by_group["a"].AC, [2, 2])
        self.assertEqual(by_group["a"].homozygote_count, [1, 1])
        self.assertEqual(by_group["b"].AC, [1, 1])
