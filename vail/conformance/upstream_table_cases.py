"""Run an explicit subset of original Hail test bodies against Vail.

This is not pytest collection of Hail and does not run a Hail oracle. It extracts
selected functions without importing Hail's Spark/JVM-dependent test module.
Assertions and function bodies are unchanged; decorators, module imports, and
Hail initialization fixtures are omitted. Only cases needing none of those
fixtures belong in this allowlist.
"""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import itertools
import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "hail/python/test/hail/table/test_table.py"
SIMPLE = (
    ("Tests", "test_range_table"),
    ("Tests", "test_range_table_zero"),
    ("Tests", "test_table_parallelize_error_both_schema_partial_type_defined"),
    (None, "test_large_number_of_partitions"),
)
ROW_EXPRESSION_CASES = (
    ("Tests", "test_filter"),
    ("Tests", "test_filter_missing"),
    ("Tests", "test_annotate"),
    ("Tests", "test_transmute_key"),
    ("Tests", "test_transmute"),
    ("Tests", "test_select"),
)


def extract(tree, class_name, name, filename, namespace):
    nodes = tree.body
    if class_name is not None:
        nodes = next(n for n in nodes if isinstance(n, ast.ClassDef) and n.name == class_name).body
    original = next(n for n in nodes if isinstance(n, ast.FunctionDef) and n.name == name)
    selected = copy.deepcopy(original)
    selected.decorator_list = []
    module = ast.Module(body=[selected], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, filename, "exec"), namespace)
    return namespace[name], original.lineno


def run(hail_root: Path):
    import pytest
    import vail

    path = hail_root / SOURCE
    source_bytes = path.read_bytes()
    tree = ast.parse(source_bytes, filename=str(path))
    namespace = {"hl": vail, "pytest": pytest, "__name__": "vail_upstream_test_bodies"}
    helper_path = hail_root / "hail/python/test/hail/helpers.py"
    extract(ast.parse(helper_path.read_text()), None, "convert_struct_to_dict", str(helper_path), namespace)
    outcomes = []

    def execute(class_name, name, parameters=None):
        parameters = parameters or {}
        function, line = extract(tree, class_name, name, str(path), namespace)
        definition_id = "::".join(x for x in (SOURCE, class_name, name) if x)
        case_id = definition_id
        if parameters:
            case_id += "[" + ",".join(f"{k}={v}" for k, v in parameters.items()) + "]"
        outcome = {
            "test_id": definition_id,
            "case_id": case_id,
            "source_line": line,
            "parameters": parameters,
            "status": "passed",
        }
        try:
            args = [unittest.TestCase()] if class_name is not None else []
            function(*args, **parameters)
        except Exception as error:
            outcome.update(status="failed", error_type=type(error).__name__, error=str(error))
        outcomes.append(outcome)

    for class_name, name in (*SIMPLE, *ROW_EXPRESSION_CASES):
        execute(class_name, name)
    names = ("table_name", "num_parts", "n", "truncator_name", "counter_name")
    for values in itertools.product(
        ("rt", "par"), (3, 11), (10, 9, 11, 0, 7), ("head", "tail"), ("count", "_force_count")
    ):
        execute(None, "test_table_head_and_tail", dict(zip(names, values)))

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=hail_root, text=True).strip()
    source_files = (
        "python/vail/__init__.py", "python/vail/utils.py", "python/vail/types.py",
        "python/vail/expressions.py", "crates/vail-python/src/lib.rs",
        "crates/vail-execution/src/lib.rs", "crates/vail-execution/src/expressions.rs",
        "conformance/upstream_table_cases.py", "Cargo.lock",
    )
    source_files = sorted(set(source_files) | {
        path.relative_to(ROOT).as_posix()
        for directory in ("python/vail", "crates/vail-execution/src", "crates/vail-python/src")
        for path in (ROOT / directory).rglob("*")
        if path.suffix in (".py", ".rs")
    })
    source_hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                     for name in source_files if (ROOT / name).exists()}
    return {
        "schema_version": 1,
        "hail_commit": commit,
        "source": SOURCE,
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "vail_source_sha256": source_hashes,
        "mode": "original-test-bodies-against-vail",
        "limitations": [
            "Not a Hail oracle run or full upstream pytest collection.",
            "Original assertions and test bodies execute unchanged; decorators, imports and Hail initialization fixtures are omitted.",
            "Only rt/par variants of test_table_head_and_tail are run; its 80 cache/checkpoint variants remain untested.",
            f"These results cover {len({row['test_id'] for row in outcomes})} test definitions and {len(outcomes)} concrete cases, not the complete Hail Table suite.",
            "The expression cases include unchanged filter, missing-filter, annotate, transmute, keyed-transmute and select bodies.",
        ],
        "summary": {status: sum(row["status"] == status for row in outcomes)
                    for status in ("passed", "failed")},
        "outcomes": outcomes,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hail-root", type=Path, default=ROOT.parent / "hail")
    parser.add_argument("--output", type=Path, default=ROOT / "conformance/upstream-table-results.json")
    args = parser.parse_args()
    result = run(args.hail_root.resolve())
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"]))
    return int(result["summary"]["failed"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
