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
        self.assertEqual(table.aggregate(expr).AC, [1, 1])

    def test_hwe_options_and_inbreeding_arguments(self):
        table = hl.Table.parallelize([{"gt": hl.Call((0, 1)), "p": 0.25}],
                                     schema=hl.tstruct(gt=hl.tcall, p=hl.tfloat64))
        hwe = hl.agg.hardy_weinberg_test(table.gt, one_sided=True)
        self.assertAlmostEqual(table.aggregate(hwe).p_value, 0.5)
        ib = hl.agg.inbreeding(table.gt, table.p)
        self.assertEqual(ib.dtype["f_stat"], hl.tfloat64)
        self.assertEqual(table.aggregate(ib).n_called, 1)

    def test_info_score_and_ndarray_result_types(self):
        table = hl.Table.parallelize([{"gp": [0.25, 0.5, 0.25]}],
                                     schema=hl.tstruct(gp=hl.tarray(hl.tfloat64)))
        info = hl.agg.info_score(table.gp)
        self.assertEqual(info.dtype["score"], hl.tfloat64)
        self.assertEqual(info.dtype["n_included"], hl.tint32)

        arrays = hl.Table.parallelize([{"x": np.ones((2, 3))}],
                                      schema=hl.tstruct(x=hl.tndarray(hl.tfloat64, 2)))
        self.assertEqual(hl.agg.ndarray_sum(arrays.x).dtype, hl.tndarray(hl.tfloat64, 2))
        np.testing.assert_array_equal(arrays.aggregate(hl.agg.ndarray_sum(arrays.x)), np.ones((2, 3)))

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
            return np.asarray(data).reshape(shape)
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
            [{"x": np.array([value], dtype=np.int64)},
             {"x": np.array([3], dtype=np.int64)}],
            schema=hl.tstruct(x=hl.tndarray(hl.tint64, 1)), n_partitions=2)
        np.testing.assert_array_equal(table.aggregate(hl.agg.ndarray_sum(table.x)),
                                      np.array([value + 3], dtype=np.int64))

    def test_ndarray_sum_ignores_missing_arrays(self):
        table = hl.Table.parallelize(
            [{"x": np.array([1., 2.])}, {"x": None},
             {"x": np.array([3., 4.])}],
            schema=hl.tstruct(x=hl.tndarray(hl.tfloat64, 1)), n_partitions=2)
        np.testing.assert_array_equal(table.aggregate(hl.agg.ndarray_sum(table.x)),
                                      np.array([4., 6.]))

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


def test_public_hwe_reference_examples_and_partitioned_equivalence():
    import pytest
    for counts, frequency, p in [
        ((1, 1, 1), .6, .7),
        ((250, 500, 250), .5002501250625313, .9747844394217698),
        ((37, 200, 85), .48964964307448583, 1.1337210383168987e-6),
    ]:
        scalar = hl.eval(hl.hardy_weinberg_test(*counts))
        assert scalar.het_freq_hwe == pytest.approx(frequency, abs=1e-12)
        assert scalar.p_value == pytest.approx(p, abs=1e-10)
        calls = [hl.Call([0, 0])] * counts[0] + [hl.Call([0, 1])] * counts[1] + [hl.Call([1, 1])] * counts[2]
        calls += [hl.Call([0]), hl.Call([1]), hl.Call([]), None]
        for parts in [1, 7]:
            table = hl.Table.parallelize([{'gt': c} for c in calls],
                                         schema=hl.tstruct(gt=hl.tcall), n_partitions=parts)
            actual = table.aggregate(hl.agg.hardy_weinberg_test(table.gt))
            assert actual.het_freq_hwe == pytest.approx(scalar.het_freq_hwe)
            assert actual.p_value == pytest.approx(scalar.p_value)
            one = table.aggregate(hl.agg.hardy_weinberg_test(table.gt, one_sided=True))
            assert one.p_value == pytest.approx(hl.eval(hl.hardy_weinberg_test(*counts, True)).p_value)


def test_call_stats_public_invalid_initializer_and_indices():
    import pytest
    table = hl.Table.parallelize([{'gt': hl.Call([2])}], schema=hl.tstruct(gt=hl.tcall))
    for n in [2, -1, hl.missing(hl.tint32)]:
        with pytest.raises((RuntimeError, ValueError), match='allele|initializer'):
            table.aggregate(hl.agg.call_stats(table.gt, n))
