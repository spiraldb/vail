"""Behavioral randomness checks from the pinned Hail source.

Original bodies and value assertions are retained. Hail compiler-node checks
are adapted to checking Vail's native IR construction, since Vail has its own
TableIR. Initialization and backend decorators are omitted.
"""

import ast
import copy
import json
from pathlib import Path
import unittest
import pytest
import vail as hl

ROOT = Path(__file__).resolve().parents[3]
SUITE = ROOT / "hail/hail/python/test/hail"
FILES = [
    "test_randomness.py",
    "table/test_table.py",
    "matrixtable/test_matrix_table.py",
    "expr/test_expr.py",
]
CASES = []
for filename in FILES:
    tree = ast.parse((SUITE / filename).read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and (
            filename == "test_randomness.py" or "randomness" in node.name
        ):
            CASES.append((filename, None, node.name))
        if isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name in (
                    "test_seeded_same",
                    "test_seeded_sampling",
                    "test_key_by_random",
                    "test_order_by_head_optimization_with_randomness",
                    "test_aggregators_with_randomness",
                ):
                    CASES.append((filename, node.name, method.name))


class IRNames:
    def __getattr__(self, name):
        return name


def assert_native_node(value, node):
    if isinstance(value, hl.Table):
        tree = value._native.ir()
        aliases = {
            "TableLeftJoinRightDistinct": "TableLookup",
            "TableIntervalJoin": "TableLookup",
            "TableKeyByAndAggregate": "TableGroupedAggregate",
            "TableAggregateByKey": "TableGroupedAggregate",
            "TableParallelize": ("TableParallelize", "TableParallelizeExpression"),
            "TableRename": "TableMapRows",
            "TableFilterIntervals": ("TableReadIntervals", "TableFilter"),
            "TableGen": "TableParallelizeExpression",
        }
        if node.startswith("Table"):
            expected = aliases.get(node, node)
            expected = (expected,) if isinstance(expected, str) else expected

            def find(tree):
                if isinstance(tree, dict):
                    return tree.get("node") in expected or any(
                        find(v) for v in tree.values()
                    )
                return isinstance(tree, list) and any(find(v) for v in tree)

            assert find(tree), (node, tree)
        else:
            assert tree
    elif isinstance(value, hl.MatrixTable):
        assert value._mir.core._native.ir()
    else:
        assert hl.expressions.as_expr(value).to_ir()


class AdaptIRChecks(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        node.decorator_list = []
        if node.name == "assert_contains_node":
            return ast.copy_location(
                ast.FunctionDef(
                    name=node.name,
                    args=node.args,
                    body=[
                        ast.Expr(
                            ast.Call(
                                ast.Name("_assert_native_node", ast.Load()),
                                [ast.Name(a.arg, ast.Load()) for a in node.args.args],
                                [],
                            )
                        )
                    ],
                    decorator_list=[],
                ),
                node,
            )
        return self.generic_visit(node)

    def visit_Attribute(self, node):
        if node.attr == "_tir":
            node.attr = "_native"
        return self.generic_visit(node)


@pytest.mark.parametrize(
    "filename,cls,name", CASES, ids=[f"{f}:{n}" for f, _, n in CASES]
)
def test_upstream_randomness(filename, cls, name, tmp_path):
    source = SUITE / filename
    tree = ast.parse(source.read_text())
    scope = dict(
        hl=hl,
        agg=hl.agg,
        pytest=pytest,
        unittest=unittest,
        ir=IRNames(),
        new_temp_file=lambda extension=None: str(
            tmp_path / ("source." + (extension or "data"))
        ),
        _assert_native_node=assert_native_node,
    )
    selected = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in (
            name,
            "assert_unique_uids",
            "assert_contains_node",
            "query_table_table",
            "query_mt_mt",
        ):
            selected.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == cls:
            selected.extend(
                m
                for m in node.body
                if isinstance(m, ast.FunctionDef) and m.name == name
            )
    adapted = AdaptIRChecks().visit(
        copy.deepcopy(ast.Module(body=selected, type_ignores=[]))
    )
    exec(compile(ast.fix_missing_locations(adapted), str(source), "exec"), scope)
    hl.reset_global_randomness()
    kwargs = {}
    if name == "test_query_table_randomness":
        kwargs["query_table_table"] = scope["query_table_table"](None)
    if name == "test_query_matrix_table_rows_randomness":
        kwargs["query_mt_mt"] = scope["query_mt_mt"]()
    target = scope[name]
    target(unittest.TestCase(), **kwargs) if cls else target(**kwargs)


def test_public_random_inventory():
    source = ROOT / "hail/hail/python/hail/expr/functions.py"
    names = [
        n.name
        for n in ast.parse(source.read_text()).body
        if isinstance(n, ast.FunctionDef) and n.name.startswith("rand_")
    ]
    assert len(names) == 13
    assert all(callable(getattr(hl, name)) for name in names)
    import inspect

    definitions = {
        n.name: n
        for n in ast.parse(source.read_text()).body
        if isinstance(n, ast.FunctionDef)
    }
    for name in names:
        args = definitions[name].args
        signature = inspect.signature(getattr(hl, name))
        assert list(signature.parameters) == [
            arg.arg for arg in args.args + args.kwonlyargs
        ]
        for arg in args.kwonlyargs:
            assert signature.parameters[arg.arg].kind == inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    "make,dtype",
    [
        (lambda: hl.rand_bool(0.3), hl.tbool),
        (lambda: hl.rand_int32(-5, 8), hl.tint32),
        (lambda: hl.rand_int64(-(1 << 40), 1 << 40), hl.tint64),
        (lambda: hl.rand_unif(-2, 3), hl.tfloat64),
        (lambda: hl.rand_norm(), hl.tfloat64),
        (lambda: hl.rand_norm2d(), hl.tarray(hl.tfloat64)),
        (lambda: hl.rand_pois(3), hl.tfloat64),
        (lambda: hl.rand_gamma(2, 3), hl.tfloat64),
        (lambda: hl.rand_beta(2, 3, 0.2, 0.8), hl.tfloat64),
        (lambda: hl.rand_cat([0, 2, 3]), hl.tint32),
        (lambda: hl.rand_dirichlet([0, 2, 3]), hl.tarray(hl.tfloat64)),
        (lambda: hl.rand_hyper(100, 30, 20), hl.tint32),
        (lambda: hl.rand_multi_hyper([20, 30, 50], 40), hl.tarray(hl.tint32)),
    ],
)
def test_public_draws_are_pure_and_typed(make, dtype):
    hl.reset_global_randomness()
    x = make()
    assert x.dtype == dtype
    assert hl.eval(x) == hl.eval(x)
    first = hl.eval(x)
    hl.reset_global_randomness()
    assert hl.eval(make()) == first


def test_uniform_and_normal_goldens():
    hl.reset_global_randomness()
    assert hl.eval(hl.rand_unif()) == 0.9828239225846387
    assert hl.eval(hl.rand_unif()) == 0.49094525115847415
    hl.reset_global_randomness()
    assert hl.eval(hl.rand_norm()) == pytest.approx(0.347110923255205)
    assert hl.eval(hl.rand_norm()) == pytest.approx(-0.9281375348070483)


def test_random_ndarrays():
    import numpy as np

    for fn in (hl.rand_unif, hl.rand_norm):
        a = fn(size=(3, 4), seed=99)
        assert a.dtype == hl.tndarray(hl.tfloat64, 2)
        assert np.array_equal(hl.eval(a), hl.eval(a))
        assert hl.eval(a).shape == (3, 4)
        assert len(set(hl.eval(a).ravel())) == 12
        assert hl.eval(fn(size=(0, 4))).shape == (0, 4)


@pytest.mark.parametrize(
    "make,mean,tolerance,lower,upper",
    [
        (lambda: hl.rand_unif(-2, 3), 0.5, 0.15, -2, 3),
        (lambda: hl.rand_norm(2, 3), 2, 0.35, None, None),
        (lambda: hl.rand_gamma(2, 3), 6, 0.5, 0, None),
        (lambda: hl.rand_beta(2, 3), 0.4, 0.03, 0, 1),
        (lambda: hl.rand_pois(4), 4, 0.2, 0, None),
        (lambda: hl.rand_bool(0.3), 0.3, 0.04, 0, 1),
        (lambda: hl.rand_hyper(100, 30, 20), 6, 0.25, 0, 20),
        (lambda: hl.rand_cat([0, 1, 3]), 1.75, 0.05, 1, 2),
    ],
)
def test_distribution_moments_and_support(make, mean, tolerance, lower, upper):
    import numpy as np

    hl.reset_global_randomness()
    values = np.asarray(hl.eval(hl.range(2000).map(lambda _: make())))
    assert abs(values.mean() - mean) < tolerance
    if lower is not None:
        assert values.min() >= lower
    if upper is not None:
        assert values.max() <= upper


def test_multivariate_distribution_constraints():
    import numpy as np

    values = np.asarray(
        hl.eval(hl.range(500).map(lambda _: hl.rand_dirichlet([0, 2, 3])))
    )
    assert np.allclose(values.sum(axis=1), 1)
    assert np.all(values[:, 0] == 0)
    assert np.allclose(values.mean(axis=0), [0, 0.4, 0.6], atol=0.04)
    values = np.asarray(
        hl.eval(hl.range(500).map(lambda _: hl.rand_multi_hyper([20, 30, 50], 40)))
    )
    assert np.all(values.sum(axis=1) == 40)
    assert np.all((values >= 0) & (values <= [20, 30, 50]))
    assert np.allclose(values.mean(axis=0), [8, 12, 20], atol=0.6)


def test_explicit_seeds_and_missingness():
    for fn in (hl.rand_norm, hl.rand_unif):
        assert hl.eval(fn(seed=7)) == hl.eval(fn(seed=7))
        assert hl.eval(fn(seed=7)) != hl.eval(fn(seed=8))
        assert hl.eval(fn(hl.missing(hl.tfloat64))) is None
    values = hl.eval(hl.range(100).map(lambda _: hl.rand_int64(seed=12)))
    assert len(set(values)) == 100
    assert hl.eval(hl.rand_int32(hl.missing(hl.tint32))) is None
    assert hl.eval(hl.shuffle([1, 2, 3, 4], seed=3)) == hl.eval(
        hl.shuffle([1, 2, 3, 4], seed=3)
    )
    assert sorted(hl.eval(hl.shuffle([1, 2, 3, 4]))) == [1, 2, 3, 4]


def test_context_replay_across_scans_and_actions():
    table = hl.range_table(30, 5).annotate(r=hl.rand_int64(seed=10))
    table = table.annotate(prefix=hl.scan.sum(table.r))
    rows = table.collect()
    expected = 0
    for row in rows:
        assert row.prefix == expected
        expected = ((expected + row.r + (1 << 63)) % (1 << 64)) - (1 << 63)
    assert table.collect() == rows
    assert table.count() == table._force_count() == 30
    reordered = table.order_by(-table.idx).annotate(s=hl.rand_int64(seed=11))
    assert len({row.s for row in reordered.collect()}) == 30


def test_source_partition_count_does_not_change_draws():
    draws = []
    for partitions in (1, 3, 17):
        table = hl.range_table(31, partitions).annotate(r=hl.rand_int64(seed=23))
        draws.append([row.r for row in table.collect()])
    assert draws[0] == draws[1] == draws[2]


def test_stream_scan_inputs_are_not_resampled():
    scan = hl._stream_range(5)._aggregate_scan(lambda x: x + hl.scan.sum(x))
    assert hl.eval(scan.to_array()) == [0, 1, 3, 6, 10]
    scan = hl._stream_range(6)._aggregate_scan(
        lambda _: hl.scan.sum(hl.rand_int32(100, seed=12))
    )
    values = hl.eval(hl.range(6).map(lambda _: hl.rand_int32(100, seed=12)))
    expected = [sum(values[:i]) for i in range(6)]
    assert hl.eval(scan.to_array()) == expected
    assert hl.eval(scan.to_array()) == expected
