"""Fixture validation, wire protocol, and assertion execution."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .codec import decode_value, encode_value
from .compare import assert_bag_equal, assert_ordered_equal, assert_ordered_with_ties_equal, value_equal

PROTOCOL = "vail-conformance/1"


def load_fixture(path: str | Path) -> dict[str, Any]:
    fixture = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_fixture(fixture)
    return fixture


def _validate_typed_value(value: Any, type_spec: Any, nullable: bool, location: str) -> None:
    from .codec import MISSING

    value = decode_value(value)
    if value is MISSING:
        if not nullable:
            raise ValueError(f"missing value in non-nullable fixture field at {location}")
        return
    if isinstance(type_spec, dict) and "struct" in type_spec:
        if not isinstance(value, dict):
            raise ValueError(f"expected struct at {location}")
        fields = type_spec["struct"]
        expected = {field["name"] for field in fields}
        if set(value) != expected:
            raise ValueError(f"struct fields mismatch at {location}")
        for field in fields:
            _validate_typed_value(value[field["name"]], field["type"], field["nullable"], f"{location}.{field['name']}")
        return
    if isinstance(type_spec, dict) and "array" in type_spec:
        if not isinstance(value, list):
            raise ValueError(f"expected array at {location}")
        for i, item in enumerate(value):
            _validate_typed_value(item, type_spec["array"], True, f"{location}[{i}]")
        return
    valid = {
        "bool": lambda x: type(x) is bool,
        "int32": lambda x: type(x) is int and -(2**31) <= x < 2**31,
        "int64": lambda x: type(x) is int and -(2**63) <= x < 2**63,
        "float32": lambda x: type(x) in (int, float) and type(x) is not bool,
        "float64": lambda x: type(x) in (int, float) and type(x) is not bool,
        "string": lambda x: isinstance(x, str),
    }
    if type_spec not in valid or not valid[type_spec](value):
        raise ValueError(f"invalid {type_spec!r} value at {location}: {value!r}")

def validate_fixture(fixture: Any) -> None:
    if not isinstance(fixture, dict) or fixture.get("protocol") != PROTOCOL:
        raise ValueError(f"fixture protocol must be {PROTOCOL!r}")
    for field in ("id", "baseline", "tables", "steps", "expectations"):
        if field not in fixture:
            raise ValueError(f"fixture missing required field {field!r}")
    step_ids = [step.get("id") for step in fixture["steps"]]
    if len(step_ids) != len(set(step_ids)):
        raise ValueError("step ids must be unique")
    if any(not isinstance(step_id, str) or not step_id for step_id in step_ids):
        raise ValueError("step ids must be non-empty strings")
    for name, table in fixture["tables"].items():
        fields = [field["name"] for field in table["schema"]]
        if len(fields) != len(set(fields)):
            raise ValueError(f"table {name!r} has duplicate schema fields")
        if len(table["key"]) != len(set(table["key"])) or any(key not in fields for key in table["key"]):
            raise ValueError(f"table {name!r} key references a missing or duplicate field")
        for i, row in enumerate(table["rows"]):
            if set(row) != set(fields):
                raise ValueError(f"table {name!r} row fields do not match schema: {row!r}")
            for field in table["schema"]:
                _validate_typed_value(row[field["name"]], field["type"], field["nullable"], f"{name}.rows[{i}].{field['name']}")
        global_fields = table.get("global_schema")
        if global_fields is not None:
            global_names = [field["name"] for field in global_fields]
            if set(table.get("globals", {})) != set(global_names):
                raise ValueError(f"table {name!r} globals do not match global_schema")
            for field in global_fields:
                _validate_typed_value(table["globals"][field["name"]], field["type"], field["nullable"], f"{name}.globals.{field['name']}")
        elif table.get("globals"):
            raise ValueError(f"table {name!r} has globals but no global_schema")
        if table.get("n_partitions") is not None and table["n_partitions"] < 1:
            raise ValueError(f"table {name!r} n_partitions must be positive")
    if any(expectation["step"] not in step_ids for expectation in fixture["expectations"]):
        raise ValueError("expectation references an unknown step")


def encode_outcome(fixture_id: str, steps: list[dict[str, Any]], status: str = "ok", diagnostic: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"protocol": PROTOCOL, "fixture_id": fixture_id, "status": status, "steps": steps}
    if diagnostic:
        result["diagnostic"] = diagnostic
    return result


def _at_path(value: Any, path: str | None) -> Any:
    if not path:
        return value
    current = value
    for component in path.split("."):
        if isinstance(current, dict) and component in current:
            current = current[component]
        else:
            raise AssertionError(f"result path {path!r} not found")
    return current


def _decode_tree(value: Any) -> Any:
    return decode_value(value)


def check_outcome(fixture: dict[str, Any], outcome: dict[str, Any], backend: str) -> list[str]:
    if outcome.get("protocol") != PROTOCOL:
        raise AssertionError(f"backend returned unexpected protocol: {outcome.get('protocol')!r}")
    if outcome.get("fixture_id") != fixture["id"]:
        raise AssertionError(f"backend returned fixture {outcome.get('fixture_id')!r}, expected {fixture['id']!r}")
    if outcome.get("status") != "ok":
        raise AssertionError(f"backend status {outcome.get('status')!r}: {outcome.get('diagnostic', '')}")
    by_id = {step["id"]: step for step in outcome.get("steps", [])}
    skipped = []
    for expectation in fixture["expectations"]:
        capability = expectation.get("capability", "oracle-and-native")
        if expectation["step"] not in by_id:
            raise AssertionError(f"backend omitted step {expectation['step']!r}")
        got_step = by_id[expectation["step"]]
        if backend == "native" and capability == "oracle-only-until-native-op":
            error = got_step.get("error", {})
            if error.get("category") != "unsupported":
                raise AssertionError(
                    f"step {expectation['step']!r} is declared unsupported in the native backend "
                    f"but returned {got_step!r}"
                )
            skipped.append(expectation["step"])
            continue
        mode = expectation["mode"]
        if mode == "error":
            err = got_step.get("error")
            if not err:
                raise AssertionError(f"step {expectation['step']!r} expected an error")
            want = expectation["expected"]
            if want.get("category") and err.get("category") != want["category"]:
                raise AssertionError(f"error category differs: {err!r}")
            if want.get("class_contains") and want["class_contains"] not in err.get("class", ""):
                raise AssertionError(f"error class differs: {err!r}")
            continue
        if got_step.get("error"):
            raise AssertionError(f"step {expectation['step']!r} failed: {got_step['error']!r}")
        got = _decode_tree(_at_path(got_step.get("value"), expectation.get("path")))
        wanted = _decode_tree(expectation["expected"])
        if mode == "ordered":
            if isinstance(got, list) and isinstance(wanted, list):
                assert_ordered_equal(got, wanted)
            elif not value_equal(got, wanted):
                raise AssertionError(f"step {expectation['step']!r}: {got!r} != {wanted!r}")
        elif mode == "bag":
            assert_bag_equal(got, wanted)
        elif mode == "ordered_with_ties":
            assert_ordered_with_ties_equal(got, wanted, expectation.get("tie_fields", []))
        else:
            raise ValueError(f"unknown comparison mode {mode!r}")
    return skipped


def dumps_wire(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def loads_wire(line: str) -> Any:
    return json.loads(line)


__all__ = ["PROTOCOL", "encode_outcome", "encode_value", "dumps_wire", "loads_wire", "load_fixture", "validate_fixture", "check_outcome"]
