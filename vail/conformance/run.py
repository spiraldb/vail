"""Run typed Table fixtures against an isolated native or Hail oracle worker."""
from __future__ import annotations

import argparse
import glob
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from .protocol import PROTOCOL, check_outcome, dumps_wire, load_fixture, loads_wire


def fixture_files(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        matches = [Path(p) for p in sorted(glob.glob(pattern))]
        if not matches and Path(pattern).is_file():
            matches = [Path(pattern)]
        files.extend(matches)
    result = sorted(set(files))
    if not result:
        raise SystemExit("no fixture files matched")
    return result


def verify_oracle_checkout(fixtures: list[dict[str, Any]]) -> str:
    hail_repo = Path(__file__).resolve().parents[2] / "hail"
    expected = {fixture["baseline"]["hail_commit"] for fixture in fixtures}
    if len(expected) != 1:
        raise SystemExit(f"oracle fixtures use multiple Hail commits: {sorted(expected)!r}")
    try:
        actual = subprocess.check_output(
            ["git", "-C", str(hail_repo), "rev-parse", "HEAD"], text=True, stderr=subprocess.PIPE
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f"cannot verify pinned Hail oracle checkout at {hail_repo}: {exc}") from exc
    wanted = next(iter(expected))
    if actual != wanted:
        raise SystemExit(f"Hail oracle commit mismatch: fixture requires {wanted}, checkout is {actual}")
    return actual


def backend_command(backend: str, command: list[str] | None) -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    if backend == "native":
        if not command:
            raise SystemExit("--backend native requires --command followed by a worker command")
        return command, env
    hail_python = env.get("VAIL_HAIL_PYTHON", sys.executable)
    worker_path = str(Path(__file__).resolve().parents[2] / "hail" / "hail" / "python")
    old_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = worker_path + (os.pathsep + old_pythonpath if old_pythonpath else "")
    return [hail_python, "-m", "conformance.oracle_worker"], env


def send_fixtures(command: list[str], env: dict[str, str], fixtures: list[dict[str, Any]], timeout: float) -> list[dict[str, Any]]:
    payload = "".join(dumps_wire(fixture) + "\n" for fixture in fixtures)
    try:
        completed = subprocess.run(command, input=payload, text=True, capture_output=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"worker timed out after {timeout:g}s: {command!r}") from exc
    if completed.returncode != 0:
        raise SystemExit(f"worker exited {completed.returncode}:\n{completed.stderr}")
    outcomes = []
    for line in completed.stdout.splitlines():
        if not line.strip() or not line.lstrip().startswith("{"):
            continue  # Spark can print startup diagnostics to stdout.
        try:
            outcome = loads_wire(line)
        except Exception as exc:
            raise SystemExit(f"worker emitted invalid JSON line: {line[:300]!r}") from exc
        if outcome.get("protocol") != PROTOCOL:
            raise SystemExit(f"worker emitted an unexpected JSON line: {line[:300]!r}")
        outcomes.append(outcome)
    if len(outcomes) != len(fixtures):
        raise SystemExit(
            f"worker protocol mismatch: expected {len(fixtures)} responses, got {len(outcomes)}; "
            f"stderr={completed.stderr!r}, stdout={completed.stdout[:400]!r}"
        )
    return outcomes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("oracle", "native"), required=True)
    parser.add_argument("--command", help="Native worker argv as a shell-like string; executed without a shell")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--write-outcomes", type=Path, help="Save raw backend responses as JSON")
    parser.add_argument("fixtures", nargs="*", default=["conformance/fixtures/case_*.json"])
    args = parser.parse_args()
    command = shlex.split(args.command) if args.command else None
    files = fixture_files(args.fixtures)
    fixtures = [load_fixture(path) for path in files]
    if args.backend == "oracle":
        verify_oracle_checkout(fixtures)
    command, env = backend_command(args.backend, command)
    outcomes = send_fixtures(command, env, fixtures, args.timeout)
    failures = 0
    unsupported = 0
    for path, fixture, outcome in zip(files, fixtures, outcomes):
        if outcome.get("status") == "unavailable" and args.backend == "oracle":
            print(f"UNAVAILABLE {fixture['id']}: {outcome.get('diagnostic')}")
            failures += 1
            continue
        if outcome.get("status") == "unsupported" and args.backend == "native":
            declared_unsupported = all(
                expectation.get("capability") == "oracle-only-until-native-op"
                for expectation in fixture["expectations"]
            )
            if declared_unsupported:
                print(f"UNSUPPORTED {fixture['id']}: {outcome.get('diagnostic')}")
                unsupported += 1
            else:
                print(f"FAIL {fixture['id']} ({path}): unexpected native unsupported status: {outcome.get('diagnostic')}")
                failures += 1
            continue
        try:
            skipped = check_outcome(fixture, outcome, "native" if args.backend == "native" else "oracle")
        except Exception as exc:
            print(f"FAIL {fixture['id']} ({path}): {exc}")
            failures += 1
            continue
        print(f"PASS {fixture['id']} ({path})")
        for step in skipped:
            print(f"  UNSUPPORTED {step}: native operation has no compatibility implementation yet")
    if args.write_outcomes:
        args.write_outcomes.write_text(dumps_wire({"protocol": PROTOCOL, "outcomes": outcomes}) + "\n", encoding="utf-8")
    passed = len(fixtures) - failures - unsupported
    print(f"{passed}/{len(fixtures)} fixtures passed; {unsupported} unsupported; {failures} failed")
    return 1 if failures else (2 if unsupported else 0)


if __name__ == "__main__":
    raise SystemExit(main())
