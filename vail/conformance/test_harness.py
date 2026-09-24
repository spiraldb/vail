import json
import math
import tempfile
import unittest
from pathlib import Path

from conformance.codec import MISSING, decode_value, encode_value
from conformance.compare import assert_bag_equal, assert_ordered_equal, assert_ordered_with_ties_equal
from conformance.protocol import check_outcome, load_fixture


class TypedCodecTests(unittest.TestCase):
    def test_nested_missing_round_trip(self):
        value = {"s": {"a": MISSING, "b": [1, MISSING]}, "empty": []}
        self.assertEqual(decode_value(encode_value(value)), value)

    def test_nan_infinities_signed_zero_round_trip(self):
        values = [float("nan"), float("inf"), float("-inf"), -0.0, 0.0]
        encoded = encode_value(values)
        decoded = decode_value(encoded)
        self.assertTrue(math.isnan(decoded[0]))
        self.assertEqual(decoded[1:3], values[1:3])
        self.assertLess(math.copysign(1.0, decoded[3]), 0)
        self.assertGreater(math.copysign(1.0, decoded[4]), 0)
        self.assertEqual(encoded[0], {"$float64": "NaN"})
        self.assertEqual(encoded[3], {"$float64": "-0"})

    def test_reject_json_null(self):
        with self.assertRaisesRegex(TypeError, "Use MISSING"):
            encode_value(None)
        with self.assertRaisesRegex(ValueError, "JSON null is not a fixture value"):
            decode_value(None)


class ComparisonTests(unittest.TestCase):
    def test_ordered_is_sensitive_to_sequence_bag_is_not(self):
        left = [{"id": 1}, {"id": 2}, {"id": 1}]
        right = [{"id": 1}, {"id": 1}, {"id": 2}]
        assert_bag_equal(left, right)
        with self.assertRaises(AssertionError):
            assert_ordered_equal(left, right)

    def test_bag_preserves_duplicate_multiplicity(self):
        with self.assertRaises(AssertionError):
            assert_bag_equal(["x", "x"], ["x", "y"])

    def test_ties_permit_permutation_but_keep_key_groups_ordered(self):
        expected = [{"k": 1, "v": "a"}, {"k": 1, "v": "b"}, {"k": 2, "v": "c"}]
        tied_permutation = [{"k": 1, "v": "b"}, {"k": 1, "v": "a"}, {"k": 2, "v": "c"}]
        assert_ordered_with_ties_equal(tied_permutation, expected, ["k"])
        with self.assertRaises(AssertionError):
            assert_ordered_with_ties_equal([expected[2], expected[0], expected[1]], expected, ["k"])

    def test_nan_and_signed_zero_comparison(self):
        assert_ordered_equal([float("nan"), -0.0], [float("nan"), -0.0])
        with self.assertRaises(AssertionError):
            assert_ordered_equal([-0.0], [0.0])
        with self.assertRaises(AssertionError):
            assert_bag_equal([-0.0], [0.0])


class FixtureProtocolTests(unittest.TestCase):
    def test_fixture_files_validate_and_expect_source_not_oracle(self):
        fixture_dir = Path(__file__).parent / "fixtures"
        fixtures = sorted(fixture_dir.glob("case_*.json"))
        self.assertGreaterEqual(len(fixtures), 3)
        for path in fixtures:
            fixture = load_fixture(path)
            self.assertEqual(fixture["baseline"]["provenance"], "source-derived-not-oracle-captured")

    def test_fixture_rejects_missing_nonnullable_top_level_field(self):
        fixture = {
            "protocol": "vail-conformance/1", "id": "bad",
            "baseline": {"hail_commit": "x", "provenance": "source-derived-not-oracle-captured"},
            "tables": {"t": {"schema": [{"name": "x", "type": "int32", "nullable": False}], "key": [], "rows": [{"x": {"$missing": True}}], "globals": {}}},
            "steps": [], "expectations": [],
        }
        from conformance.protocol import validate_fixture
        with self.assertRaisesRegex(ValueError, "non-nullable fixture field"):
            validate_fixture(fixture)

    def test_native_unsupported_is_reported_and_checked(self):
        fixture = {
            "id": "x",
            "expectations": [{"step": "g", "mode": "ordered", "expected": {}, "capability": "oracle-only-until-native-op"}],
        }
        outcome = {"protocol": "vail-conformance/1", "fixture_id": "x", "status": "ok", "steps": [{"id": "g", "error": {"category": "unsupported"}}]}
        self.assertEqual(check_outcome(fixture, outcome, "native"), ["g"])
        outcome["steps"][0] = {"id": "g", "value": {}}
        with self.assertRaisesRegex(AssertionError, "declared unsupported"):
            check_outcome(fixture, outcome, "native")


if __name__ == "__main__":
    unittest.main()
