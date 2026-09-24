from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import unittest

from conformance.protocol import check_outcome, dumps_wire, load_fixture, loads_wire
from scripts.native_conformance_worker import run_fixture


ROOT = Path(__file__).resolve().parents[2]


class NativeConformanceTests(unittest.TestCase):
    def test_primitive_fixtures_across_partition_counts(self):
        for name in ["key_order_nulls", "order_head_take", "duplicate_keys"]:
            fixture = load_fixture(ROOT / "conformance/fixtures" / f"case_{name}.json")
            for partitions in [1, 2, 5]:
                with self.subTest(fixture=name, partitions=partitions):
                    case = deepcopy(fixture)
                    for table in case["tables"].values():
                        table["n_partitions"] = partitions
                    skipped = check_outcome(case, run_fixture(case), backend="native")
                    self.assertEqual(skipped, [])

    def test_json_line_worker_in_fresh_process(self):
        fixture = load_fixture(ROOT / "conformance/fixtures/case_key_order_nulls.json")
        result = subprocess.run(
            [sys.executable, "-m", "scripts.native_conformance_worker"],
            cwd=ROOT, input=dumps_wire(fixture) + "\n", capture_output=True,
            text=True, check=True, timeout=30,
        )
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(check_outcome(fixture, loads_wire(lines[0]), backend="native"), [])

    def test_unsupported_globals_are_not_counted_as_pass(self):
        fixture = load_fixture(ROOT / "conformance/fixtures/case_globals_source_errors.json")
        outcome = run_fixture(fixture)
        self.assertEqual(outcome["status"], "unsupported")
        with self.assertRaises(AssertionError):
            check_outcome(fixture, outcome, backend="native")


if __name__ == "__main__":
    unittest.main()
