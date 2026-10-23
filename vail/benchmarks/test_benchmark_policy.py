"""Exercise exclusion policy through real CLI processes and worker workloads."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).with_name("upstream_benchmark_coverage.py")
EXCLUDED = "benchmark_table::test_table_aggregate_approx_cdf"
RUNTIME_FAILURE = "benchmark_table::test_table_range_array_range_force_count"


class BenchmarkPolicyTests(unittest.TestCase):
    def run_case(self, engine, case_id, *, workload="fail", extra=()):
        with tempfile.TemporaryDirectory(prefix="benchmark-policy-cli-") as raw:
            root = Path(raw)
            upstream = root / "upstream"
            upstream.mkdir()
            marker = root / "workload-marker.txt"
            # Inventory reads fixture decorators as source. These fixtures are
            # intentionally unused by the tiny executable benchmark workload.
            (upstream / "fixtures.py").write_text(
                "@pytest.fixture(params=[1])\n"
                "def many_partitions_ht():\n    pass\n"
            )
            (upstream / "benchmark_matrix_table.py").write_text("")
            body = (
                "    assert hl.utils.range_table(7).count() == 7\n"
                "    marker.write_text('completed')\n"
                if workload == "succeed" else
                "    marker.write_text('started')\n"
                "    raise RuntimeError('deliberate_public_workload_failure')\n"
            )
            (upstream / "benchmark_table.py").write_text(
                "import os\nfrom pathlib import Path\n"
                "marker = Path(os.environ['VAIL_POLICY_TEST_MARKER'])\n"
                "marker.write_text('module-loaded')\n"
                f"def {case_id.split('::')[1]}():\n{body}"
            )
            output = root / "report.json"
            env = {**os.environ, "VAIL_BENCHMARK_UPSTREAM": str(upstream),
                   "VAIL_POLICY_TEST_MARKER": str(marker)}
            env.pop("VAIL_BENCHMARK_TELEMETRY_DIR", None)
            command = [sys.executable, str(RUNNER), "--engine", engine,
                       "--case", case_id, "--data-dir", str(root / "data"),
                       "--timeout", "20", "--fixture-timeout", "20",
                       "--output", str(output), *extra]
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=40)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return json.loads(output.read_text()), marker.read_text() if marker.exists() else None

    def test_hail_exclusion_does_not_execute_workload(self):
        report, marker = self.run_case("hail", EXCLUDED)
        self.assertEqual(report["cases"][0]["status"], "comparison_not_applicable")
        self.assertFalse(report["cases"][0]["actual_sample"])
        self.assertIsNone(marker)
        self.assertIn("sha256", report["exclusion_policy"])

    def test_vail_exclusion_does_not_execute_workload(self):
        report, marker = self.run_case("vail", EXCLUDED)
        self.assertEqual(report["cases"][0]["status"], "unsupported")
        self.assertFalse(report["cases"][0]["actual_sample"])
        self.assertIsNone(marker)

    def test_exact_probe_executes_real_small_vail_workload(self):
        reason = "Suspected functionality fix; exercise targeted policy probe"
        report, marker = self.run_case("vail", EXCLUDED, workload="succeed",
                                       extra=("--probe-excluded", "--probe-reason", reason))
        self.assertEqual(report["cases"][0]["status"], "passed", report["cases"][0])
        self.assertEqual(marker, "completed")
        self.assertEqual(report["targeted_probe"]["reason"], reason)

    def test_runtime_failure_is_observed_instead_of_excluded(self):
        report, marker = self.run_case("vail", RUNTIME_FAILURE)
        self.assertEqual(report["cases"][0]["status"], "failed")
        self.assertIn("deliberate_public_workload_failure", report["cases"][0]["error"])
        self.assertEqual(marker, "started")

    def test_probe_cli_requires_exact_selection_and_suspected_fix(self):
        for flags in (("--probe-excluded",),
                      ("--probe-excluded", "--case", EXCLUDED),
                      ("--probe-excluded", "--probe-reason", "suspected fix")):
            with self.subTest(flags=flags):
                result = subprocess.run([sys.executable, str(RUNNER), *flags],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertIn("requires exact --case selection", result.stderr)


if __name__ == "__main__":
    unittest.main()
