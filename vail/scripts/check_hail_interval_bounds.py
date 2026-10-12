#!/usr/bin/env python3
"""Reproduce Hail's sampled key-boundary construction for _calculate_new_partitions.

Run from the Vail repository root:

    JAVA_HOME=/opt/homebrew/opt/openjdk@21 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=/Users/danielking/projects/vail/hail/hail/python \
    /private/tmp/vail-hail-oracle/bin/python scripts/check_hail_interval_bounds.py
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import hail as hl


def plain(x: Any) -> Any:
    if hasattr(x, "items"):
        return {k: plain(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [plain(v) for v in x]
    return x


def bounds(table, n: int):
    try:
        values = table._calculate_new_partitions(n)
        return {
            "bounds": [
                {
                    "start": plain(b.start),
                    "end": plain(b.end),
                    "includes_start": b.includes_start,
                    "includes_end": b.includes_end,
                }
                for b in values
            ]
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {str(exc).splitlines()[0]}"}


def partition_rows(table):
    unkeyed = table.key_by()
    packed = unkeyed._map_partitions(
        lambda rows: hl.array([hl.struct(rows=rows.to_array())])._to_stream()
    )
    return [plain(row.rows) for row in packed.collect()]


def describe(table):
    return {
        "n_partitions": table.n_partitions(),
        "partition_rows": partition_rows(table),
        "rows": [plain(row) for row in table.collect()],
    }


def main():
    hl.init(quiet=True, master="local[2]")
    output = {"oracle": {"hail_commit": "e10cc355bc8cd0de9c5f29d5182d31527898bae1", "date": "2026-10-01"}}
    rows = [{"k": 1, "v": i} for i in range(5)] + [{"k": 2, "v": 5}]
    t = hl.Table.parallelize(rows, schema=hl.tstruct(k=hl.tint32, v=hl.tint32), key="k", n_partitions=3)
    output["duplicates"] = {
        "input": describe(t),
        "calc10_repeated": [bounds(t, 10) for _ in range(5)],
        "calc2": bounds(t, 2),
    }
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "table")
        t.write(path)
        calculated = t._calculate_new_partitions(10)
        output["duplicates"]["read_n_partitions"] = {
            str(n): describe(hl.read_table(path, _n_partitions=n)) for n in (1, 2, 10)
        }
        output["duplicates"]["read_explicit_calculated_intervals"] = describe(
            hl.read_table(path, _intervals=calculated)
        )
        output["duplicates"]["filter_explicit_calculated_intervals"] = describe(
            hl.filter_intervals(t, calculated)
        )
    output["single_key"] = {}
    output["all_missing_key"] = {}
    for label, key_values in (("single_key", [1] * 6), ("all_missing_key", [None] * 6)):
        table = hl.Table.parallelize(
            [{"k": k, "v": i} for i, k in enumerate(key_values)],
            schema=hl.tstruct(k=hl.tint32, v=hl.tint32),
            key="k",
            n_partitions=3,
        )
        output[label] = {
            "input": describe(table),
            "calc0": bounds(table, 0),
            "calc1": bounds(table, 1),
        }
    hl.stop()
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
