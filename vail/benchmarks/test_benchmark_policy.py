"""Public runner boundaries: exclusions prevent work, probes explicitly opt in."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import upstream_benchmark_coverage as runner


class BenchmarkPolicyTests(unittest.TestCase):
    def run_case(self, engine, case_id, *extra, result=None):
        case = {"id": case_id, "module": "benchmark_table.py", "name": "test",
                "args": [], "fixtures": [], "parameters": [], "arguments": {},
                "fixture_parameters": {}, "upstream_marks": []}
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for name in (*runner.SOURCES, "fixtures.py"):
                (root / name).write_text("")
            output = root / "result.json"
            argv = ["runner", "--engine", engine, "--case", case_id,
                    "--output", str(output), *extra]
            with patch("sys.argv", argv), patch.object(runner, "inventory", return_value=[case]), \
                    patch.object(runner, "UPSTREAM", root), \
                    patch.object(runner, "run_one", return_value=result or {"status": "passed"}) as execute, \
                    contextlib.redirect_stdout(io.StringIO()):
                runner.main()
                return json.loads(output.read_text()), execute.call_count

    def test_hail_exclusion_never_starts_worker(self):
        report, calls = self.run_case("hail", "benchmark_table::test_table_aggregate_approx_cdf")
        self.assertEqual(calls, 0)
        self.assertEqual(report["cases"][0]["status"], "comparison_not_applicable")
        self.assertFalse(report["cases"][0]["actual_sample"])
        self.assertIn("sha256", report["exclusion_policy"])

    def test_vail_exclusion_never_starts_worker(self):
        report, calls = self.run_case("vail", "benchmark_table::test_table_aggregate_approx_cdf")
        self.assertEqual(calls, 0)
        self.assertEqual(report["cases"][0]["status"], "unsupported")
        self.assertFalse(report["cases"][0]["actual_sample"])

    def test_exact_probe_after_suspected_fix_runs_worker(self):
        report, calls = self.run_case("vail", "benchmark_table::test_table_aggregate_approx_cdf",
                                      "--probe-excluded", "--probe-reason", "Suspected approx_cdf implementation fix")
        self.assertEqual(calls, 1)
        self.assertEqual(report["cases"][0]["status"], "passed")
        self.assertEqual(report["targeted_probe"]["reason"], "Suspected approx_cdf implementation fix")

    def test_generic_runtime_bug_is_not_excluded(self):
        report, calls = self.run_case("vail", "benchmark_table::test_table_range_array_range_force_count",
                                      result={"status": "failed", "error": "runtime panic"})
        self.assertEqual(calls, 1)
        self.assertEqual(report["cases"][0]["status"], "failed")

    def test_probe_requires_exact_cases_and_suspected_fix_reason(self):
        for argv in (["runner", "--probe-excluded"],
                     ["runner", "--probe-excluded", "--case", "example"],
                     ["runner", "--probe-excluded", "--probe-reason", "suspected fix"]):
            with self.subTest(argv=argv), patch("sys.argv", argv), \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exit:
                runner.main()
            self.assertEqual(exit.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
