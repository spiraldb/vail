#!/usr/bin/env python3
"""Run Vail-owned test suites and preserve complete, structured results."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from contextlib import redirect_stdout
from io import StringIO


ROOT = Path(__file__).resolve().parents[1]
EXCLUDE_DIRS = {".git", ".venv", "target", "__pycache__", "runs"}
SOURCE_SUFFIXES = {".py", ".rs", ".toml", ".json", ".lock", ".md", ".sh"}
TIMEOUT_SECONDS = 1800
COUNT_KEYS = (
    "passed_tests", "passed_subtests", "passed_cases", "failed_assertions",
    "unsupported", "unavailable", "blocked", "infra_failures",
)


def source_hashes() -> dict[str, str]:
    """Hash project sources without descending into large generated trees."""
    result = {}
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = sorted(name for name in dirs if name not in EXCLUDE_DIRS)
        for filename in sorted(files):
            path = Path(directory) / filename
            if path.suffix in SOURCE_SUFFIXES:
                result[path.relative_to(ROOT).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def extension_fingerprint() -> dict[str, str] | None:
    """Find the installed native extension without importing Vail."""
    spec = importlib.machinery.PathFinder.find_spec("vail", sys.path)
    if not spec or not spec.submodule_search_locations:
        return None
    native = importlib.machinery.PathFinder.find_spec("_native", spec.submodule_search_locations)
    if not native or not native.origin:
        return None
    path = Path(native.origin)
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return {"path": str(path), "sha256": "unreadable"}
    return {"path": str(path), "sha256": digest}


def decode_stream(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def blank_counts() -> dict[str, int]:
    return dict.fromkeys(COUNT_KEYS, 0)


def classify(name: str, code: int | None, stdout: str, stderr: str,
             build_ok: bool, output: Path) -> dict:
    counts = blank_counts()
    if name in {"python_tests", "native_conformance", "upstream_table_cases"} and not build_ok:
        counts["blocked"] = 1
        return {"result": "blocked", "counts": counts}
    if name == "build_extension":
        counts["infra_failures"] = int(code != 0)
        return {"result": "passed" if code == 0 else "failed", "counts": counts}
    if name == "runner_self_tests":
        match = re.search(r"Ran (\d+) tests?", stdout + stderr)
        if match:
            counts["passed_tests"] = int(match.group(1)) if code == 0 else 0
        failure_match = re.search(r"FAILED \(failures=(\d+)(?:, errors=(\d+))?\)", stdout + stderr)
        if failure_match:
            counts["failed_assertions"] = int(failure_match.group(1)) + int(failure_match.group(2) or 0)
        if code != 0 and counts["failed_assertions"] == 0:
            counts["infra_failures"] = 1
        result = "failed" if counts["failed_assertions"] or counts["infra_failures"] else "passed"
        return {"result": result, "counts": counts}

    if code == 124:
        counts["infra_failures"] = 1
        return {"result": "failed", "counts": counts}

    if name == "rust_workspace":
        rows = re.findall(r"test result: .*?(\d+) passed; (\d+) failed; (\d+) ignored", stdout + stderr)
        if rows:
            counts["passed_tests"] = sum(int(row[0]) for row in rows)
            counts["failed_assertions"] = sum(int(row[1]) for row in rows)
        if (code != 0 and counts["failed_assertions"] == 0) or (code == 0 and not rows):
            counts["infra_failures"] = 1
    elif name == "python_tests":
        final_line = next((line.strip() for line in reversed(stdout.splitlines()) if line.strip()), "")
        counts["passed_tests"] = int(re.search(r"(\d+) passed", final_line).group(1)) if re.search(r"(\d+) passed", final_line) else 0
        counts["passed_subtests"] = int(re.search(r"(\d+) subtests passed", final_line).group(1)) if re.search(r"(\d+) subtests passed", final_line) else 0
        counts["failed_assertions"] = int(re.search(r"(\d+) failed", final_line).group(1)) if re.search(r"(\d+) failed", final_line) else 0
        if code != 0 and counts["failed_assertions"] == 0:
            counts["infra_failures"] = 1
    elif name == "native_conformance":
        counts["passed_cases"] = len(re.findall(r"^PASS ", stdout, re.MULTILINE))
        counts["unsupported"] = len(re.findall(r"^UNSUPPORTED ", stdout, re.MULTILINE))
        counts["failed_assertions"] = len(re.findall(r"^FAIL ", stdout, re.MULTILINE))
        has_fixture_outcome = counts["passed_cases"] + counts["unsupported"] + counts["failed_assertions"] > 0
        if (code == 2 and counts["unsupported"] == 0) or (code != 0 and code != 2 and not has_fixture_outcome) or (
            code == 0 and not has_fixture_outcome
        ):
            counts["infra_failures"] = 1
    elif name == "upstream_table_cases":
        try:
            data = json.loads((output / "upstream-table-results.json").read_text())
            passed = int(data["summary"]["passed"])
            failed = int(data["summary"]["failed"])
            outcomes = data["outcomes"]
            if not isinstance(outcomes, list) or len(outcomes) != passed + failed:
                raise ValueError("outcome count does not match summary")
            if sum(item.get("status") == "passed" for item in outcomes) != passed or sum(
                item.get("status") == "failed" for item in outcomes
            ) != failed:
                raise ValueError("outcomes do not match summary counts")
            counts["passed_cases"] = passed
            counts["failed_assertions"] = failed
            if code != 0 and failed == 0:
                counts["infra_failures"] = 1
        except (OSError, ValueError, KeyError, TypeError):
            counts["infra_failures"] = 1
    elif name == "hail_oracle":
        counts["passed_cases"] = len(re.findall(r"^PASS ", stdout, re.MULTILINE))
        counts["unavailable"] = len(re.findall(r"^UNAVAILABLE ", stdout, re.MULTILINE))
        counts["failed_assertions"] = len(re.findall(r"^FAIL ", stdout, re.MULTILINE))
        if code != 0 and counts["unavailable"] == 0 and counts["failed_assertions"] == 0:
            counts["infra_failures"] = 1
        if code == 0 and counts["passed_cases"] == 0:
            counts["infra_failures"] = 1

    failed = counts["failed_assertions"] or counts["infra_failures"]
    if failed:
        result = "failed"
    elif counts["unavailable"]:
        result = "unavailable"
    elif counts["unsupported"]:
        result = "unsupported"
    else:
        result = "passed"
    return {"result": result, "counts": counts}


class ClassifyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.output = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_pytest_keeps_tests_and_subtests_separate_and_reads_summary_only(self):
        result = classify("python_tests", 0,
                          "diagnostic: 999 failed\n40 passed, 19 subtests passed in 1.2s\n",
                          "", True, self.output)
        self.assertEqual(result["counts"]["passed_tests"], 40)
        self.assertEqual(result["counts"]["passed_subtests"], 19)
        self.assertEqual(result["counts"]["failed_assertions"], 0)

    def test_native_unsupported_is_not_a_pass_or_failure(self):
        result = classify("native_conformance", 2,
                          "PASS a\nUNSUPPORTED b: missing op\n3/4 fixtures passed\n",
                          "", True, self.output)
        self.assertEqual(result["result"], "unsupported")
        self.assertEqual(result["counts"]["passed_cases"], 1)
        self.assertEqual(result["counts"]["unsupported"], 1)
        self.assertEqual(result["counts"]["failed_assertions"], 0)

    def test_native_assertion_failure_is_not_duplicated_as_infrastructure_failure(self):
        result = classify("native_conformance", 1, "FAIL x: assertion mismatch\n", "", True, self.output)
        self.assertEqual(result["counts"]["failed_assertions"], 1)
        self.assertEqual(result["counts"]["infra_failures"], 0)

    def test_oracle_unavailable_is_not_counted_as_failure(self):
        result = classify("hail_oracle", 1, "UNAVAILABLE x: missing dependency\n", "", True, self.output)
        self.assertEqual(result["result"], "unavailable")
        self.assertEqual(result["counts"]["unavailable"], 1)
        self.assertEqual(result["counts"]["failed_assertions"], 0)

    def test_malformed_upstream_result_is_an_infrastructure_failure(self):
        (self.output / "upstream-table-results.json").write_text("not json")
        result = classify("upstream_table_cases", 0, "", "", True, self.output)
        self.assertEqual(result["result"], "failed")
        self.assertEqual(result["counts"]["infra_failures"], 1)

    def test_upstream_summary_must_match_each_outcome(self):
        (self.output / "upstream-table-results.json").write_text(json.dumps({
            "summary": {"passed": 1, "failed": 0}, "outcomes": []
        }))
        result = classify("upstream_table_cases", 0, "", "", True, self.output)
        self.assertEqual(result["counts"]["infra_failures"], 1)

    def test_rust_compile_failure_is_infrastructure_not_failed_test(self):
        result = classify("rust_workspace", 101, "", "error: could not compile", True, self.output)
        self.assertEqual(result["counts"]["failed_assertions"], 0)
        self.assertEqual(result["counts"]["infra_failures"], 1)


class RunnerEndToEndTests(unittest.TestCase):
    def run_mocked_main(self, output: Path, upstream_failed: bool) -> tuple[int, dict]:
        def fake_run_suite(command, env):
            if "--self-test" in command:
                return 0, 0.001, "", "Ran 9 tests\nOK\n"
            if "maturin" in command:
                return 0, 0.001, "built\n", ""
            if "cargo" in command:
                return 0, 0.001, "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; finished in 0.00s\n", ""
            if "pytest" in command:
                return 0, 0.001, "2 passed, 3 subtests passed in 0.01s\n", ""
            if "conformance.run" in command and "native" in command:
                return 0, 0.001, "PASS fixture\n1/1 fixtures passed; 0 unsupported; 0 failed\n", ""
            if any("upstream_table_cases" in argument for argument in command):
                path = Path(command[command.index("--output") + 1])
                failed = int(upstream_failed)
                payload = {
                    "summary": {"passed": 1, "failed": failed},
                    "outcomes": ([{"status": "passed"}] + ([{"status": "failed"}] if failed else [])),
                }
                path.write_text(json.dumps(payload))
                return failed, 0.001, json.dumps(payload["summary"]) + "\n", ""
            if "conformance.run" in command and "oracle" in command:
                return 0, 0.001, "PASS oracle_fixture\n1/1 fixtures passed; 0 unsupported; 0 failed\n", ""
            raise AssertionError(f"unexpected command: {command}")

        args = ["scripts/run_all_tests.py", "--label", "smoke", "--output", str(output)]
        with mock.patch.object(sys, "argv", args), \
             mock.patch(__name__ + ".run_suite", side_effect=fake_run_suite), \
             mock.patch(__name__ + ".source_hashes", return_value={"source.py": "stable"}), \
             mock.patch(__name__ + ".extension_fingerprint", return_value={"path": "stub.so", "sha256": "stable"}), \
             redirect_stdout(StringIO()):
            exit_code = main()
        return exit_code, json.loads((output / "summary.json").read_text())

    def test_main_writes_summary_and_returns_success_for_clean_run(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "clean"
            code, summary = self.run_mocked_main(output, upstream_failed=False)
            self.assertEqual(code, 0)
            self.assertTrue((output / "summary.json").is_file())
            self.assertFalse(summary["integrity_failure"])
            self.assertEqual(summary["totals"]["passed_tests"], 12)
            self.assertEqual(summary["totals"]["passed_subtests"], 3)
            self.assertEqual(summary["totals"]["failed_assertions"], 0)

    def test_main_returns_failure_for_upstream_assertion_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "failed"
            code, summary = self.run_mocked_main(output, upstream_failed=True)
            self.assertEqual(code, 1)
            self.assertTrue((output / "summary.json").is_file())
            self.assertEqual(summary["totals"]["failed_assertions"], 1)

def run_suite(command: list[str], env: dict[str, str]) -> tuple[int, float, str, str]:
    started = time.monotonic()
    try:
        run = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True,
                             timeout=TIMEOUT_SECONDS)
        return run.returncode, round(time.monotonic() - started, 3), run.stdout, run.stderr
    except subprocess.TimeoutExpired as error:
        stderr = decode_stream(error.stderr) + f"\nrunner timeout after {TIMEOUT_SECONDS}s\n"
        return 124, round(time.monotonic() - started, 3), decode_stream(error.stdout), stderr
    except OSError as error:
        return 127, round(time.monotonic() - started, 3), "", f"runner error: {error}\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="run")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--self-test", action="store_true", help="run runner-classifier unit tests only")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
        return 0 if result.wasSuccessful() else 1

    started = datetime.now(timezone.utc)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out = args.output or ROOT / "conformance" / "runs" / f"{stamp}-{args.label}"
    out.mkdir(parents=True, exist_ok=False)
    initial_hashes = source_hashes()
    (out / "source_hashes.json").write_text(json.dumps(initial_hashes, indent=2) + "\n")
    initial_extension = extension_fingerprint()
    (out / "extension_at_start.json").write_text(json.dumps(initial_extension, indent=2) + "\n")

    suites = [
        ("runner_self_tests", ["uv", "run", "--no-sync", "python", "scripts/run_all_tests.py", "--self-test"], False),
        ("build_extension", ["uv", "run", "--no-sync", "maturin", "develop", "--locked"], True),
        ("rust_workspace", ["cargo", "test", "--workspace", "--all-targets", "--locked"], True),
        ("python_tests", ["uv", "run", "--no-sync", "python", "-m", "pytest", "python/tests", "conformance/test_harness.py", "-q"], False),
        ("native_conformance", ["uv", "run", "--no-sync", "python", "-m", "conformance.run", "--backend", "native", "--command", "uv run --no-sync python -m scripts.native_conformance_worker", "--write-outcomes", str(out / "native-outcomes.json")], False),
        ("upstream_table_cases", ["uv", "run", "--no-sync", "python", "-m", "conformance.upstream_table_cases", "--output", str(out / "upstream-table-results.json")], False),
        ("hail_oracle", ["uv", "run", "--no-sync", "python", "-m", "conformance.run", "--backend", "oracle", "--write-outcomes", str(out / "oracle-outcomes.json")], False),
    ]
    results = []
    build_ok = False
    extension_after_build = None

    def save_summary(final: bool = False) -> None:
        current_hashes = source_hashes() if final else initial_hashes
        if final:
            (out / "final_source_hashes.json").write_text(json.dumps(current_hashes, indent=2) + "\n")
        drift = sorted(set(initial_hashes) ^ set(current_hashes))
        drift.extend(name for name in sorted(set(initial_hashes) & set(current_hashes))
                     if initial_hashes[name] != current_hashes[name])
        current_extension = extension_fingerprint() if final else extension_after_build
        extension_changed = bool(extension_after_build and current_extension != extension_after_build)
        totals = {key: sum(item["classification"]["counts"][key] for item in results)
                  for key in COUNT_KEYS}
        manifest = {
            "runner_version": 3,
            "label": args.label,
            "started_at_utc": started.isoformat(),
            "finished_at_utc": datetime.now(timezone.utc).isoformat() if final else None,
            "project_root": str(ROOT),
            "python": sys.version,
            "platform": sys.platform,
            "source_drift": {"detected": bool(drift), "paths": drift},
            "extension_fingerprints": {
                "at_start": initial_extension,
                "after_build": extension_after_build,
                "at_end": current_extension if final else None,
                "changed_during_suites": extension_changed,
            },
            "suites": results,
            "totals": totals,
            "integrity_failure": bool(final and (drift or extension_changed)),
            "notes": [
                "Only the Vail workspace crates and Vail-owned Python/conformance tests are included; cloned upstream ecosystems are not recursively tested.",
                "Hail oracle unavailable fixtures are separate from passes and assertion failures.",
                "The complete Hail test universe is inventoried but not executed here.",
                "Native conformance expectations are source-derived; its fixture passes do not prove Hail parity.",
                "Unsupported fixture results are not counted as passing assertions.",
            ],
        }
        (out / "summary.json").write_text(json.dumps(manifest, indent=2) + "\n")

    for name, command, cargo in suites:
        env = os.environ.copy()
        if cargo:
            env["CARGO_TARGET_DIR"] = "target/integration"
        if name in {"python_tests", "native_conformance", "upstream_table_cases"} and not build_ok:
            item = {
                "name": name, "command": command, "cwd": str(ROOT),
                "environment": {}, "exit_code": None, "duration_seconds": 0,
                "stdout_log": f"{name}.stdout.log", "stderr_log": f"{name}.stderr.log",
                "skipped_reason": "extension build failed; avoiding stale installed extension",
            }
            (out / item["stdout_log"]).write_text("")
            (out / item["stderr_log"]).write_text("")
            item["classification"] = classify(name, None, "", "", build_ok, out)
            results.append(item)
            print(f"{name}: blocked because extension build failed")
            save_summary()
            continue
        code, duration, stdout, stderr = run_suite(command, env)
        (out / f"{name}.stdout.log").write_text(stdout)
        (out / f"{name}.stderr.log").write_text(stderr)
        item = {
            "name": name, "command": command, "cwd": str(ROOT),
            "environment": {"CARGO_TARGET_DIR": "target/integration"} if cargo else {},
            "exit_code": code, "duration_seconds": duration,
            "stdout_log": f"{name}.stdout.log", "stderr_log": f"{name}.stderr.log",
        }
        if name == "build_extension":
            build_ok = code == 0
            extension_after_build = extension_fingerprint()
        item["classification"] = classify(name, code, stdout, stderr, build_ok, out)
        results.append(item)
        print(f"{name}: {item['classification']['result']} exit={code} duration={duration}s")
        save_summary()
    save_summary(final=True)
    print(f"results: {out}")
    manifest = json.loads((out / "summary.json").read_text())
    totals = manifest["totals"]
    print(json.dumps(totals, sort_keys=True))
    return 1 if manifest["integrity_failure"] or totals["failed_assertions"] or totals["infra_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
