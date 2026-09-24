"""Hail oracle worker. Run only in a process/environment containing the pinned Hail package."""
from __future__ import annotations

import sys
from typing import Any

from .codec import MISSING, encode_value
from .protocol import PROTOCOL, dumps_wire, encode_outcome, loads_wire


HAIL_TYPES = {
    "bool": "tbool",
    "int32": "tint32",
    "int64": "tint64",
    "float32": "tfloat32",
    "float64": "tfloat64",
    "string": "tstr",
}


def plain(value: Any) -> Any:
    if value is MISSING:
        return None
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


def hail_type(hl, type_spec: Any):
    if isinstance(type_spec, str):
        return getattr(hl, HAIL_TYPES[type_spec])
    if "struct" in type_spec:
        return hl.tstruct(**{field["name"]: hail_type(hl, field["type"]) for field in type_spec["struct"]})
    if "array" in type_spec:
        return hl.tarray(hail_type(hl, type_spec["array"]))
    raise ValueError(f"unsupported fixture type {type_spec!r}")


def hail_struct_type(hl, fields):
    return hl.tstruct(**{field["name"]: hail_type(hl, field["type"]) for field in fields})


def make_struct(hl, value):
    value = plain(value)
    if isinstance(value, dict):
        return hl.Struct(**{k: make_struct(hl, v) for k, v in value.items()})
    if isinstance(value, list):
        return [make_struct(hl, v) for v in value]
    return value


def python_value(value):
    if value is None:
        return MISSING
    if hasattr(value, "items"):
        return {k: python_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [python_value(v) for v in value]
    return value


def build_table(hl, spec):
    rows = [make_struct(hl, row) for row in spec["rows"]]
    schema = hail_struct_type(hl, spec["schema"])
    kwargs = {"schema": schema, "key": [], "n_partitions": spec.get("n_partitions")}
    if spec.get("globals"):
        kwargs["globals"] = make_struct(hl, spec["globals"])
    table = hl.Table.parallelize(rows, **kwargs)
    if spec.get("key"):
        table = table.key_by(*spec["key"])
    return table


def error_category(exc: Exception) -> str:
    message = str(exc).lower()
    if "source mismatch" in message or "different source" in message:
        return "source_mismatch"
    if "scope violation" in message or "indexed by" in message:
        return "expression_scope"
    return "runtime_error"


def step_value(table, rows=None):
    value = {"key": list(table.key)}
    if rows is not None:
        value["rows"] = [encode_value(python_value(row)) for row in rows]
    return value


def execute_fixture(hl, fixture):
    tables = {name: build_table(hl, spec) for name, spec in fixture["tables"].items()}
    outcomes = []
    for step in fixture["steps"]:
        step_id = step["id"]
        op = step["op"]
        try:
            if op == "source_mismatch":
                table = tables[step["table"]]
                other = tables[step["other"]]
                field = next(iter(other.row))
                table.annotate(_vail_bad_source=other[field])
                outcomes.append({"id": step_id, "value": {}})
                continue
            if op == "get_globals":
                value = python_value(hl.eval(tables[step["table"]].globals))
                outcomes.append({"id": step_id, "value": encode_value(value)})
                continue
            if op == "annotate_globals":
                table = tables[step["table"]]
                fields = {name: table.globals[name] for name in step["fields"]}
                tables[step["result"]] = table.annotate(**fields)
                outcomes.append({"id": step_id, "value": {"key": list(tables[step["result"]].key)}})
                continue
            if op == "key_by":
                table = tables[step["table"]]
                tables[step["result"]] = table.key_by(*step["fields"])
                outcomes.append({"id": step_id, "value": {"key": list(tables[step["result"]].key)}})
                continue
            if op == "order_by":
                table = tables[step["table"]]
                order_exprs = []
                for sort in step["sort"]:
                    column = table[sort["field"]]
                    order_exprs.append(hl.desc(column) if sort["direction"] == "desc" else hl.asc(column))
                tables[step["result"]] = table.order_by(*order_exprs)
                outcomes.append({"id": step_id, "value": {"key": list(tables[step["result"]].key)}})
                continue
            if op == "head":
                tables[step["result"]] = tables[step["table"]].head(step["n"])
                outcomes.append({"id": step_id, "value": {"key": list(tables[step["result"]].key)}})
                continue
            if op == "take":
                rows = tables[step["table"]].take(step["n"])
                outcomes.append({"id": step_id, "value": {"rows": encode_value([python_value(row) for row in rows])}})
                continue
            if op == "collect":
                table = tables[step["table"]]
                rows = table.collect()
                outcomes.append({"id": step_id, "value": step_value(table, rows)})
                continue
            if op == "count":
                outcomes.append({"id": step_id, "value": {"value": tables[step["table"]].count()}})
                continue
            if op == "join":
                table = tables[step["table"]].join(tables[step["other"]], how=step.get("how", "inner"))
                tables[step.get("result", step_id)] = table
                outcomes.append({"id": step_id, "value": {"key": list(table.key)}})
                continue
            outcomes.append({"id": step_id, "error": {"class": "UnsupportedOperation", "category": "unsupported", "message": f"oracle worker does not implement {op!r}"}})
        except Exception as exc:  # Oracle errors are returned as comparable outcome values.
            outcomes.append({"id": step_id, "error": {"class": type(exc).__name__, "category": error_category(exc), "message": str(exc)}})
    return encode_outcome(fixture["id"], outcomes)


def main() -> int:
    try:
        import hail as hl
        hl.init(quiet=True, idempotent=True)
    except Exception as exc:
        message = f"Hail initialization unavailable: {type(exc).__name__}: {exc}"
        for line in sys.stdin:
            try:
                fixture = loads_wire(line)
                print(dumps_wire(encode_outcome(fixture.get("id", "unknown"), [], status="unavailable", diagnostic=message)), flush=True)
            except Exception:
                print(dumps_wire({"protocol": PROTOCOL, "fixture_id": "unknown", "status": "unavailable", "steps": [], "diagnostic": message}), flush=True)
        return 0
    for line in sys.stdin:
        try:
            fixture = loads_wire(line)
            print(dumps_wire(execute_fixture(hl, fixture)), flush=True)
        except Exception as exc:
            print(dumps_wire(encode_outcome("unknown", [], status="error", diagnostic=f"{type(exc).__name__}: {exc}")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
