"""Run original Hail MatrixTable definitions against Vail in isolated workers.

Imports, backend decorators and initialization are omitted. Original assertions,
class helpers, fixture bodies and declared parameter variants are retained. This
is neither full upstream pytest collection nor a Hail oracle comparison.
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import __future__
import hashlib
import inspect
import json
import operator
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from uuid import uuid4

CASES = (
    "test_range_count",
    "test_head_rows_and_cols",
    "test_head_rows",
    "test_head_cols",
    "test_tail_cols",
    "test_tail_entries",
    "test_col_agg_no_rows",
    "test_col_collect",
    "test_group_by_field_lifetimes",
    "test_aggregate_cols_by",
    "test_aggregate_rows_by",
    "test_choose_cols",
    "test_aggregation_with_no_aggregators",
    "test_collect_cols_by_key",
)


class BackendDependencyUnavailable(Exception):
    pass


class BackendDependency:
    def __init__(self, name):
        self.name = name

    def __getattr__(self, name):
        raise BackendDependencyUnavailable(
            f"Hail Scala/backend dependency: {self.name}.{name}"
        )


def definitions(module):
    result = {}
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            result[node.name] = (None, node)
        elif isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name.startswith(
                    "test_"
                ):
                    result[method.name] = (node.name, method)
    return result


def categorize(error_type, message, trace):
    text = message + "\n" + trace
    if error_type == "BackendDependencyUnavailable" or "base_search" in message:
        return "backend_internals"
    if any(
        s in message
        for s in (
            "import_vcf",
            "import_bgen",
            "import_plink",
            "export_vcf",
            "export_bgen",
            "export_plink",
        )
    ):
        return "format_io"
    if "unsupported matrix descriptor" in message or "_vail_matrix.json" in message:
        return "legacy_matrix_format"
    if any(s in message for s in ("rand_", "sampling requires", "random expression")):
        return "randomness"
    if any(
        s in message
        for s in (
            "balding_nichols",
            "variant_qc",
            "sample_qc",
            "split_multi",
            "filter_alleles",
            "call_stats",
            "locus",
            "tcall",
            "get_reference",
            "tlocus",
            "hwe_",
            "hl.call",
        )
    ):
        return "genetics"
    if any(
        s in message
        for s in (
            "caching requires",
            "persistence levels",
            "result-cache",
            "storage_level",
        )
    ):
        return "cache_persist"
    if any(s in message for s in ("codec", "staging", "_with_flags")):
        return "storage_options"
    if error_type == "PanicException" or "panicked at" in text:
        return "native_panic"
    if error_type == "AssertionError" or error_type == "Failed":
        return "assertion_mismatch"
    if "aggregation expressions require Table.aggregate" in message:
        return "table_scan_aggregation"
    if "keywords must be strings" in message:
        return "literal_inference"
    if error_type in ("NameError", "ModuleNotFoundError", "ImportError"):
        return "harness_dependency"
    if "module 'vail'" in message or "module 'vail." in message:
        return "shared_api_missing"
    if error_type in ("AttributeError", "NotImplementedError"):
        return "matrix_or_expression_api_missing"
    if error_type == "FileNotFoundError":
        return "fixture_resource"
    if error_type in ("ValueError", "TypeError", "ExpressionException"):
        return "expression_or_validation"
    return "execution_error"


def failure(exc, phase):
    trace = traceback.format_exc()
    message = str(exc)
    return dict(
        status="failed_or_blocked",
        phase=phase,
        error_type=type(exc).__name__,
        message=message,
        category=categorize(type(exc).__name__, message, trace),
        error=trace[-12000:],
    )


def execute(source, name, temp_root, output):
    import gzip
    import math
    import random
    import pytest
    import vail as hl

    module = ast.parse(source.read_text())
    class_name, function = definitions(module)[name]
    parameter_decorators = [
        d
        for d in function.decorator_list
        if isinstance(d, ast.Call) and ast.unparse(d.func) == "pytest.mark.parametrize"
    ]
    resources = source.parents[4] / "hail" / "test" / "resources"

    def new_temp_file(prefix=None, extension=None):
        return str(
            temp_root
            / (
                (prefix + "-" if prefix else "")
                + uuid4().hex
                + ("." + extension if extension else "")
            )
        )

    scope = dict(
        hl=hl,
        agg=hl.agg,
        pytest=pytest,
        operator=operator,
        unittest=unittest,
        os=os,
        gzip=gzip,
        math=math,
        random=random,
        json=json,
        new_temp_file=new_temp_file,
        resource=lambda filename: str(resources / filename),
        ir=BackendDependency("ir"),
        Env=BackendDependency("Env"),
    )
    # Copy relevant original shared helpers; do not substitute simplified fixtures.
    helpers = ast.parse((source.parents[1] / "helpers.py").read_text())
    wanted = {
        "schema_eq",
        "convert_struct_to_dict",
        "create_all_values",
        "prefix_struct",
        "create_all_values_matrix_table",
        "get_dataset",
    }
    helper_nodes = [
        n for n in helpers.body if isinstance(n, ast.FunctionDef) and n.name in wanted
    ]
    scope["_dataset"] = None
    nodes = helper_nodes + [
        n for n in module.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))
    ]
    for node in nodes:
        node.decorator_list = []
        if isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, ast.FunctionDef):
                    method.decorator_list = []
    program = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    report = dict(case=name, variants=[], status="failed_or_blocked")

    def publish():
        output.write_text(json.dumps(report, indent=2) + "\n")

    try:
        exec(
            compile(
                program, str(source), "exec", flags=__future__.annotations.compiler_flag
            ),
            scope,
        )
        target = getattr(scope[class_name](), name) if class_name else scope[name]
        variants = [{}]
        for decorator in parameter_decorators:
            names = eval(
                compile(ast.Expression(decorator.args[0]), str(source), "eval"), scope
            )
            names = names.split(",") if isinstance(names, str) else names
            values = eval(
                compile(ast.Expression(decorator.args[1]), str(source), "eval"), scope
            )
            expanded = []
            for current in variants:
                for value in values:
                    if hasattr(value, "values") and hasattr(value, "marks"):
                        value = value.values
                    if len(names) == 1:
                        value = (value,)
                    expanded.append(current | dict(zip(names, value, strict=True)))
            variants = expanded
    except BaseException as exc:
        report.update(failure(exc, "setup_or_parameters"))
        publish()
        return
    fixtures = {}
    for index, parameters in enumerate(variants):
        started = time.monotonic()
        phase = "fixture"
        try:
            kwargs = dict(parameters)
            for parameter in inspect.signature(target).parameters:
                if parameter not in kwargs:
                    if parameter not in fixtures:
                        fixtures[parameter] = scope[parameter]()
                    kwargs[parameter] = fixtures[parameter]
            phase = "test_body"
            target(**kwargs)
            result = dict(status="passed", category="passed", phase=phase)
        except BaseException as exc:
            result = failure(exc, phase)
        result.update(
            variant=index,
            parameters={k: repr(v)[:500] for k, v in parameters.items()},
            elapsed_seconds=round(time.monotonic() - started, 3),
        )
        report["variants"].append(result)
        publish()
    failures = [v for v in report["variants"] if v["status"] != "passed"]
    if failures:
        report.update(
            {
                k: failures[0][k]
                for k in (
                    "status",
                    "category",
                    "error_type",
                    "message",
                    "error",
                    "phase",
                )
            }
        )
    else:
        report.update(status="passed", category="passed", error="")
    publish()


def source_fingerprints():
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "python/vail" / n
        for n in ("matrixtable.py", "matrix_list_aggregates.py", "__init__.py", "expressions.py", "lookups.py")
    ]
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in paths
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--case")
    parser.add_argument(
        "--selection", choices=("selected", "remaining", "all"), default="selected"
    )
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--worker-output", type=Path)
    parser.add_argument("--temp-root", type=Path)
    args = parser.parse_args()
    inventory = definitions(ast.parse(args.source.read_text()))
    if not (args.source.parents[1] / "helpers.py").is_file():
        parser.error(
            "the original Hail helpers.py must be adjacent to the test package"
        )
    if args.worker:
        execute(args.source, args.case, args.temp_root, args.worker_output)
        return
    names = (
        list(inventory)
        if args.selection == "all"
        else list(CASES)
        if args.selection == "selected"
        else [n for n in inventory if n not in CASES]
    )
    if args.case:
        if args.case not in inventory:
            parser.error(f"unknown test definition: {args.case}")
        names = [args.case]
    if args.jobs < 1:
        parser.error("jobs must be positive")
    results = {}
    report = dict(
        source_sha256=hashlib.sha256(args.source.read_bytes()).hexdigest(),
        source=str(args.source),
        vail_source_sha256=source_fingerprints(),
        base_commit=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        selection=args.selection,
        timeout_seconds=args.timeout,
        definition_inventory=len(inventory),
        scope="original definition bodies, helpers and parameter variants; no Hail oracle comparison",
        adaptations=[
            "Vail replaces Hail imports",
            "backend/pytest decorators removed",
            "temporary paths use local isolated directories",
            "Scala IR/Env requirements reported as backend dependencies",
        ],
        cases=[],
    )

    def publish():
        report["cases"] = [results[n] for n in names if n in results]
        report["summary"] = dict(
            definitions_requested=len(names),
            definitions_completed=len(results),
            statuses=dict(Counter(r["status"] for r in results.values())),
            categories=dict(Counter(r["category"] for r in results.values())),
            variants_executed=sum(len(r.get("variants", [])) for r in results.values()),
            variants_passed=sum(
                v["status"] == "passed"
                for r in results.values()
                for v in r.get("variants", [])
            ),
        )
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            temporary = args.output.with_suffix(".tmp")
            temporary.write_text(json.dumps(report, indent=2) + "\n")
            temporary.replace(args.output)

    def run(name):
        start = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="vail-upstream-matrix-") as folder:
            temp_root = Path(folder)
            worker_output = temp_root / "result.json"
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--source",
                    str(args.source.resolve()),
                    "--case",
                    name,
                    "--worker",
                    "--worker-output",
                    str(worker_output),
                    "--temp-root",
                    folder,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            timed_out = False
            try:
                stdout, stderr = process.communicate(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            result = (
                json.loads(worker_output.read_text())
                if worker_output.exists()
                else dict(case=name, variants=[])
            )
            if timed_out:
                result.update(
                    status="timeout",
                    category="timeout",
                    message=f"exceeded {args.timeout:g} seconds",
                )
            elif process.returncode != 0 or "category" not in result:
                result.update(
                    status="worker_failed",
                    category="native_crash"
                    if process.returncode < 0
                    else "harness_dependency",
                    message=f"worker exit {process.returncode}",
                )
            result.update(
                elapsed_seconds=round(time.monotonic() - start, 3),
                worker_exit=process.returncode,
            )
            if stderr or stdout:
                result["worker_output"] = (stderr + stdout)[-12000:]
            return result

    publish()
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(run, n): n for n in names}
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
            result = results[name]
            print(
                f"{len(results)}/{len(names)} {name}: {result['status']} ({result['category']})",
                flush=True,
            )
            publish()
    print(json.dumps(report["summary"], indent=2), flush=True)
    if any(r["status"] != "passed" for r in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
