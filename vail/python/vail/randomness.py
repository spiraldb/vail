"""Pure random expressions using Hail's Threefry/PMAC stream identifiers.

Each call captures a static UID; consumers bind dynamic row and element UIDs.
"""

import itertools
import json
from uuid import uuid4
from .expressions import Expression, _IR, as_expr, cast, literal, _merge_sources
from .types import tint32, tint64, tfloat64, tbool, tarray, tndarray, type_to_wire

_counter = itertools.count()
_nonce = 0


def reset_global_randomness():
    """Restore the implicit call-site seed counter to its initial state."""
    global _counter
    _counter = itertools.count()


def _call(name, dtype, args, seed):
    if seed is not None and (
        type(seed) is not int or not -(1 << 63) <= seed < (1 << 63)
    ):
        raise TypeError("seed must be a signed 64-bit integer")
    uid = next(_counter) if seed is None else -seed - 1
    args = [as_expr(a) for a in args]
    source = None
    for a in args:
        source = _merge_sources(source, a.source)
    return Expression(
        dtype,
        source,
        {
            "op": "nested_function",
            "function": "random_" + name,
            "dtype": type_to_wire(dtype),
            "args": [
                literal(_nonce, tint64).to_ir(),
                literal(uid, tint64).to_ir(),
                {"op": "array", "elements": [], "element_type": "int64"},
            ]
            + [a.to_ir() for a in args],
        },
    )


def _float(a):
    a = as_expr(a)
    if a.dtype not in (tint32, tint64, tfloat64) and str(a.dtype) != "float32":
        raise TypeError("random distribution parameters must be numeric")
    return cast(a, tfloat64)


def _int(a, dtype):
    a = as_expr(a)
    if a.dtype not in (tint32, tint64):
        raise TypeError("random integer parameters must be integers")
    if dtype == tint32 and a.dtype != tint32:
        raise TypeError("parameter must be int32")
    return cast(a, dtype)


def _sized(name, a, b, seed, size):
    if size is None:
        return _call(name, tfloat64, [_float(a), _float(b)], seed)
    shape = (size,) if isinstance(size, (int, Expression)) else tuple(size)
    if len(shape) != 2:
        raise ValueError("random ndarray size must contain two dimensions")
    dims = [_int(n, tint64) for n in shape]
    return _call(
        name + "_nd", tndarray(tfloat64, 2), dims + [_float(a), _float(b)], seed
    )


def rand_unif(lower=0.0, upper=1.0, seed=None, size=None):
    """Sample uniformly between lower and upper, optionally as an ndarray."""
    return _sized("unif", lower, upper, seed, size)


def rand_norm(mean=0, sd=1, seed=None, size=None):
    """Sample a normal distribution, optionally as an ndarray."""
    return _sized("norm", mean, sd, seed, size)


def rand_int32(a, b=None, *, seed=None):
    """Sample an int32 uniformly from [0, a) or [a, b)."""
    return _call(
        "int32",
        tint32,
        [_int(0 if b is None else a, tint32), _int(a if b is None else b, tint32)],
        seed,
    )


def rand_int64(a=None, b=None, *, seed=None):
    """Sample an int64, optionally restricted to [0, a) or [a, b)."""
    if a is None:
        if b is not None:
            raise ValueError("b requires a")
        return _call("int64", tint64, [], seed)
    return _call(
        "int64",
        tint64,
        [_int(0 if b is None else a, tint64), _int(a if b is None else b, tint64)],
        seed,
    )


def rand_bool(p, seed=None):
    """Return True with probability p."""
    return _call("bool", tbool, [_float(p)], seed)


def rand_pois(lamb, seed=None):
    """Sample a Poisson distribution with the given rate."""
    return _call("pois", tfloat64, [_float(lamb)], seed)


def rand_gamma(shape, scale, seed=None):
    """Sample a gamma distribution with shape and scale parameters."""
    return _call("gamma", tfloat64, [_float(shape), _float(scale)], seed)


def rand_beta(a, b, lower=None, upper=None, seed=None):
    """Sample a beta distribution, optionally truncated to [lower, upper]."""
    return _call(
        "beta",
        tfloat64,
        [
            _float(a),
            _float(b),
            _float(0 if lower is None else lower),
            _float(1 if upper is None else upper),
        ],
        seed,
    )


def _array(a, dtype):
    a = as_expr(a)
    if not isinstance(a.dtype, tarray):
        raise TypeError("parameter must be an array")
    return a.map(lambda x: _int(x, dtype) if dtype == tint32 else _float(x))


def rand_cat(prob, seed=None):
    """Sample a category index proportional to nonnegative weights."""
    return _call("cat", tint32, [_array(prob, tfloat64)], seed)


def rand_dirichlet(a, seed=None):
    """Sample an array of proportions with the given concentrations."""
    return _call("dirichlet", tarray(tfloat64), [_array(a, tfloat64)], seed)


def rand_hyper(popsize, ngood, nsample, seed=None):
    """Sample successes when drawing without replacement from a population."""
    return _call(
        "hyper", tint32, [_int(x, tint32) for x in (popsize, ngood, nsample)], seed
    )


def rand_multi_hyper(colors, nsample, seed=None):
    """Sample category counts without replacement from the given populations."""
    return _call(
        "multi_hyper",
        tarray(tint32),
        [_array(colors, tint32), _int(nsample, tint32)],
        seed,
    )


def rand_norm2d(mean=None, cov=None, seed=None):
    """Sample a bivariate normal with covariance [variance_x, covariance, variance_y]."""
    from . import range, array

    mean = _array([0, 0] if mean is None else mean, tfloat64)
    cov = _array([1, 0, 1] if cov is None else cov, tfloat64)
    x = range(2).map(lambda _: rand_norm(seed=seed))
    root = cov[0] ** 0.5
    return array(
        [
            mean[0] + root * x[0],
            mean[1]
            + cov[1] / root * x[0]
            + ((cov[2] - cov[1] * cov[1] / cov[0]) ** 0.5) * x[1],
        ]
    )


def shuffle(a, seed=None):
    """Randomly permute an array while retaining its element type."""
    a = as_expr(a)
    if not isinstance(a.dtype, tarray):
        raise TypeError("shuffle requires an array")
    return _call("shuffle", a.dtype, [a], seed)


def _is_random(node):
    return (
        isinstance(node, dict)
        and node.get("op") == "nested_function"
        and node.get("function", "").startswith("random_")
    )


def contains(node):
    if isinstance(node, dict):
        return _is_random(node) or (
            node.get("op") != "literal" and any(contains(v) for v in node.values())
        )
    return isinstance(node, list) and any(contains(v) for v in node)


def bind_context(node, uid):
    """Prepend an enclosing lexical context to an expression callback."""
    if isinstance(node, list):
        return [bind_context(v, uid) for v in node]
    if not isinstance(node, dict) or node.get("op") == "literal":
        return node
    result = {k: bind_context(v, uid) for k, v in node.items()}
    if _is_random(node):
        result["args"][2]["elements"] = uid + result["args"][2]["elements"]
    return result


def bind_rows(node, row_uid, mode, inside=None, result_parameters=frozenset()):
    """Distinguish reducer inputs from scalar results in aggregate scopes."""
    if inside is None:
        inside = mode == "row"
    if isinstance(node, list):
        return [bind_rows(v, row_uid, mode, inside, result_parameters) for v in node]
    if not isinstance(node, dict) or node.get("op") == "literal":
        return node
    op = node.get("op")
    scopes = {}
    if op == "aggregate":
        scopes = {"value": True, "init": False}
    elif op == "aggregate_fold":
        scopes = {"initial": False, "seq": True, "combine": False}
    elif op == "agg_filter":
        scopes = {"predicate": True, "body": False}
    elif op in ("agg_explode", "agg_array"):
        scopes = {"value": True, "body": False}
    elif op == "agg_group_by":
        scopes = {"key": True, "body": False}
    result = {
        k: bind_rows(
            v,
            row_uid,
            mode,
            scopes.get(k, inside),
            result_parameters | {node["parameter"]}
            if k == "body" and op == "agg_array"
            else result_parameters,
        )
        for k, v in node.items()
    }
    if _is_random(node):
        uid = (
            [row_uid]
            if inside
            else (
                [
                    {
                        "op": "aggregate",
                        "function": "min",
                        "value": row_uid,
                        "options": {"__vail_result_type": "int64"},
                    }
                ]
                if mode == "group"
                else []
            )
        )
        words = result["args"][2]["elements"]

        def depends(value):
            if isinstance(value, list):
                return any(depends(v) for v in value)
            if not isinstance(value, dict) or value.get("op") == "literal":
                return False
            return (
                value.get("op") == "lambda_variable"
                and value["name"] in result_parameters
            ) or any(depends(v) for v in value.values())

        if not inside and mode == "group":
            words = [
                {
                    "op": "aggregate",
                    "function": "min",
                    "value": word,
                    "options": {"__vail_result_type": "int64"},
                }
                if depends(word)
                else word
                for word in words
            ]
        result["args"][2]["elements"] = uid + words
    return result


def resolve(native, expressions, mode="row"):
    """Bind row identity before optimizable projections/filters/aggregations."""
    if not any(contains(e.to_ir()) for e in expressions):
        return native, expressions
    names = [name for name, _ in json.loads(native.schema_json())]
    occupied = set(names) | {
        name for name, _ in json.loads(native.global_schema_json())
    }
    name = "__vail_rng_row_uid"
    while name in occupied:
        name += "_"
    fields = [[n, {"op": "column", "name": n}] for n in names]
    fields.append(
        [
            name,
            {
                "op": "aggregate",
                "function": "count",
                "value": None,
                "options": {"__vail_scan": True, "__vail_rng_uid": True},
            },
        ]
    )
    native = native.project_json(json.dumps(fields))
    uid = {"op": "column", "name": name}
    modes = [mode] * len(expressions) if isinstance(mode, str) else mode
    return native, [
        Expression(
            e.dtype,
            e.source,
            _IR(bind_rows(e.to_ir(), uid, scope), e._global_refs, e._lookup_refs),
            selection_name=e._selection_name,
            top_level_name=e._top_level_name,
        )
        for e, scope in zip(expressions, modes)
    ]


def index_callback(collection, parameter, body):
    """Use positions, including for repeated equal elements, as dynamic UIDs."""
    if not contains(body.to_ir()):
        return collection, body
    from .expressions import tuple_
    from .streams import stream_range

    position_name = uuid4().hex
    position = Expression(
        tint32, collection.source, {"op": "lambda_variable", "name": position_name}
    )
    pair = tuple_([position, collection[position]])
    indexed = Expression(
        tarray(pair.dtype),
        collection.source,
        {
            "op": "array_map",
            "value": stream_range(collection.length()).to_array().to_ir(),
            "parameter": position_name,
            "body": pair.to_ir(),
        },
    )
    variable = {"op": "lambda_variable", "name": parameter}
    index = {
        "op": "cast",
        "dtype": "int64",
        "value": {"op": "get_field", "value": variable, "name": "__vail_tuple_0"},
    }
    value = {"op": "get_field", "value": variable, "name": "__vail_tuple_1"}

    def replace(node):
        if isinstance(node, list):
            return [replace(v) for v in node]
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        if node.get("op") == "lambda_variable" and node.get("name") == parameter:
            return value
        return {k: replace(v) for k, v in node.items()}

    ir = body.to_ir()
    return indexed, Expression(
        body.dtype,
        body.source,
        _IR(bind_context(replace(ir), [index]), ir.references, ir.lookups),
    )
