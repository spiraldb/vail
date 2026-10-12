#!/usr/bin/env python3
"""Capture focused partition behavior from the pinned Hail runtime.

Run from the vail project root with the oracle environment, for example:

    JAVA_HOME=/opt/homebrew/opt/openjdk@21 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=/Users/danielking/projects/vail/hail/hail/python \
    /private/tmp/vail-hail-oracle/bin/python scripts/check_hail_partitions.py

The script reports each TableRepartition IR before forcing an action, then
compares Hail's n_partitions result with partition-local row contents.
"""

from __future__ import annotations

import json
from typing import Any

import hail as hl


def plain(value: Any) -> Any:
    if hasattr(value, "items"):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def partition_rows(table):
    # Unkey first so each input partition can yield one row containing all of
    # its rows, including an empty array for an empty partition.
    unkeyed = table.key_by()
    mapped = unkeyed._map_partitions(
        lambda rows: hl.array([hl.struct(rows=rows.to_array())])._to_stream()
    )
    return [plain(row.rows) for row in mapped.collect()]


def table_result(table):
    result: dict[str, Any] = {"reported_partition_count": table.n_partitions()}
    for name, action in (
        ("partition_rows", lambda: partition_rows(table)),
        ("rows", lambda: [plain(row) for row in table.collect()]),
    ):
        try:
            result[name] = action()
        except Exception as exc:
            result[name + "_error"] = type(exc).__name__ + ": " + str(exc).splitlines()[0]
    return result


def repartition_case(label, table, target, shuffle):
    result = table.repartition(target, shuffle=shuffle)
    return {
        "label": label,
        "input_partition_count": table.n_partitions(),
        "requires_lowering": hl.current_backend().requires_lowering,
        "tir_before_action": str(result._tir),
        **table_result(result),
    }


def naive_case(label, table, target):
    result = table.naive_coalesce(target)
    return {
        "label": label,
        "input_partition_count": table.n_partitions(),
        "requires_lowering": hl.current_backend().requires_lowering,
        "tir_before_action": str(result._tir),
        **table_result(result),
    }


def caught(label, fn):
    try:
        value = fn()
        return {"label": label, "value": plain(value)}
    except Exception as exc:  # oracle error type/message are the observation
        return {
            "label": label,
            "error_type": type(exc).__name__,
            "error": str(exc).splitlines()[0],
        }


def main():
    hl.init(quiet=True, master="local[2]")
    try:
        out: dict[str, Any] = {
            "hail_version": getattr(hl, "__version__", None),
            "requires_lowering": hl.current_backend().requires_lowering,
            "cases": {},
        }

        keyed_range = hl.utils.range_table(10, n_partitions=4)
        out["cases"]["keyed_range_naive"] = {
            str(n): naive_case(f"keyed_range_naive_{n}", keyed_range, n)
            for n in (2, 4, 5, 9)
        }

        unkeyed = hl.Table.parallelize(
            [{"x": i} for i in range(10)],
            hl.tstruct(x=hl.tint32),
            key=None,
            n_partitions=4,
        )
        out["cases"]["unkeyed_naive"] = {
            str(n): naive_case(f"unkeyed_naive_{n}", unkeyed, n)
            for n in (2, 4, 5, 9)
        }

        out["cases"]["unkeyed_repartition"] = {
            "shuffle_true_7": repartition_case("shuffle_true_7", unkeyed, 7, True),
            "shuffle_false_2": repartition_case("shuffle_false_2", unkeyed, 2, False),
            "shuffle_false_5": repartition_case("shuffle_false_5", unkeyed, 5, False),
        }

        empty = hl.utils.range_table(0, n_partitions=4)
        out["cases"]["empty_keyed_shuffle"] = {
            str(n): caught(
                f"empty_keyed_shuffle_{n}",
                lambda n=n: table_result(empty.repartition(n, shuffle=True)),
            )
            for n in (1, 4)
        }

        out["cases"]["filter_validation"] = {
            "empty_keep": caught("empty_keep", lambda: table_result(keyed_range._filter_partitions([]))),
            "range_keep": caught("range_keep", lambda: table_result(keyed_range._filter_partitions(range(3)))),
            "duplicate_keep": caught(
                "duplicate_keep", lambda: table_result(keyed_range._filter_partitions([1, 1]))
            ),
            "out_of_range_keep": caught(
                "out_of_range_keep", lambda: table_result(keyed_range._filter_partitions([4]))
            ),
            "out_of_range_exclude": caught(
                "out_of_range_exclude",
                lambda: table_result(keyed_range._filter_partitions([4], keep=False)),
            ),
        }

        print(json.dumps(plain(out), sort_keys=True, indent=2))
    finally:
        hl.stop()


if __name__ == "__main__":
    main()
