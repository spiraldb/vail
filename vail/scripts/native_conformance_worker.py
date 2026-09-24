"""JSON-lines adapter from the conformance protocol to the real native Table bridge.

Run from the repository root with the built extension installed:
    python -m scripts.native_conformance_worker
"""
from __future__ import annotations

from collections.abc import Mapping
import json
import sys

import vail as hl

from conformance.codec import MISSING, decode_value, encode_value
from conformance.protocol import dumps_wire, encode_outcome, validate_fixture


def _input(value):
    if value is MISSING:
        return None
    if isinstance(value, dict):
        return {key: _input(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_input(item) for item in value]
    return value


def _output(value):
    if value is None:
        return MISSING
    if isinstance(value, Mapping):
        return {key: _output(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_output(item) for item in value]
    return value


def run_fixture(fixture):
    validate_fixture(fixture)
    types = {"int32": hl.tint32, "int64": hl.tint64, "bool": hl.tbool, "string": hl.tstr}
    tables = {}
    for name, source in fixture["tables"].items():
        if source.get("globals"):
            return encode_outcome(fixture["id"], [], status="unsupported", diagnostic="native globals not implemented")
        try:
            schema = hl.tstruct(**{field["name"]: types[field["type"]] for field in source["schema"]})
        except (KeyError, TypeError):
            return encode_outcome(fixture["id"], [], status="unsupported", diagnostic="unsupported native fixture type")
        tables[name] = hl.Table.parallelize(
            _input(decode_value(source["rows"])), schema=schema,
            key=source["key"], n_partitions=source.get("n_partitions", 1),
        )
    outcomes = []
    for step in fixture["steps"]:
        try:
            table = tables[step["table"]]
            op = step["op"]
            if op == "key_by":
                result = table.key_by(*step["fields"])
            elif op == "order_by":
                result = table.order_by(*[
                    hl.desc(field["field"]) if field["direction"] == "desc" else hl.asc(field["field"])
                    for field in step["sort"]
                ])
            elif op == "head":
                result = table.head(step["n"])
            elif op in ("collect", "take"):
                rows = table.collect() if op == "collect" else table.take(step["n"])
                outcomes.append({"id": step["id"], "value": {
                    "rows": encode_value(_output(rows)), "key": list(table.key),
                }})
                continue
            elif op == "count":
                outcomes.append({"id": step["id"], "value": {"value": table.count()}})
                continue
            else:
                return encode_outcome(fixture["id"], outcomes, status="unsupported", diagnostic=f"native operation {op!r} not implemented")
            tables[step["result"]] = result
            outcomes.append({"id": step["id"], "value": {"key": list(result.key)}})
        except Exception as error:
            outcomes.append({"id": step["id"], "error": {"category": "execution", "class": type(error).__name__, "message": str(error)}})
    return encode_outcome(fixture["id"], outcomes)


def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        fixture = json.loads(line)
        try:
            outcome = run_fixture(fixture)
        except Exception as error:
            outcome = encode_outcome(fixture.get("id", "unknown"), [], status="error", diagnostic=f"{type(error).__name__}: {error}")
        print(dumps_wire(outcome), flush=True)


if __name__ == "__main__":
    main()
