"""Semantic comparisons for ordered results, bags, and unspecified sort ties."""
from __future__ import annotations

import math
from typing import Any, Iterable

from .codec import MISSING


def value_equal(left: Any, right: Any) -> bool:
    if left is MISSING or right is MISSING:
        return left is right
    if isinstance(left, float) or isinstance(right, float):
        if not isinstance(left, float) or not isinstance(right, float):
            return False
        if math.isnan(left) or math.isnan(right):
            return math.isnan(left) and math.isnan(right)
        if left == 0.0 and right == 0.0:
            return math.copysign(1.0, left) == math.copysign(1.0, right)
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(value_equal(left[k], right[k]) for k in left)
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(value_equal(a, b) for a, b in zip(left, right))
    return left == right


def _remove_match(items: list[Any], value: Any) -> bool:
    for i, item in enumerate(items):
        if value_equal(item, value):
            del items[i]
            return True
    return False


def assert_bag_equal(actual: Iterable[Any], expected: Iterable[Any]) -> None:
    remaining = list(expected)
    for row in actual:
        if not _remove_match(remaining, row):
            raise AssertionError(f"unexpected row in result bag: {row!r}")
    if remaining:
        raise AssertionError(f"missing rows from result bag: {remaining!r}")


def assert_ordered_equal(actual: list[Any], expected: list[Any]) -> None:
    if len(actual) != len(expected):
        raise AssertionError(f"row count differs: actual {len(actual)}, expected {len(expected)}")
    for i, (got, want) in enumerate(zip(actual, expected)):
        if not value_equal(got, want):
            raise AssertionError(f"row {i} differs: actual {got!r}, expected {want!r}")


def _field(row: Any, name: str) -> Any:
    if not isinstance(row, dict) or name not in row:
        raise AssertionError(f"tie field {name!r} absent from row {row!r}")
    return row[name]


def _same_key(left: Any, right: Any) -> bool:
    return value_equal(left, right)


def _tie_groups(rows: list[Any], fields: list[str]) -> list[tuple[tuple[Any, ...], list[Any]]]:
    groups: list[tuple[tuple[Any, ...], list[Any]]] = []
    for row in rows:
        key = tuple(_field(row, name) for name in fields)
        if groups and all(_same_key(a, b) for a, b in zip(groups[-1][0], key)):
            groups[-1][1].append(row)
        else:
            groups.append((key, [row]))
    return groups


def assert_ordered_with_ties_equal(actual: list[Any], expected: list[Any], fields: list[str]) -> None:
    """Require identical sort-key group sequence and compare rows as bags within each tie."""
    if not fields:
        raise AssertionError("ordered_with_ties requires at least one tie field")
    actual_groups = _tie_groups(actual, fields)
    expected_groups = _tie_groups(expected, fields)
    if len(actual_groups) != len(expected_groups):
        raise AssertionError("number of ordered tie groups differs")
    for i, ((actual_key, actual_rows), (expected_key, expected_rows)) in enumerate(zip(actual_groups, expected_groups)):
        if len(actual_key) != len(expected_key) or not all(_same_key(a, b) for a, b in zip(actual_key, expected_key)):
            raise AssertionError(f"sort key group {i} differs: {actual_key!r} != {expected_key!r}")
        assert_bag_equal(actual_rows, expected_rows)
