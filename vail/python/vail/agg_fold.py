"""Arbitrary stateful aggregation using compiled scalar expressions."""
from __future__ import annotations

from uuid import uuid4

from .expressions import Expression, _merge_sources, as_expr, cast
from .types import unify_types


def fold(initial_value, seq_op, comb_op):
    """Fold table rows with user supplied sequence and combine expressions.

    ``initial_value`` may reference table globals. ``seq_op`` receives the
    current state and may reference fields from its source Table;
    ``comb_op`` receives the two partial states.
    """
    if not callable(seq_op) or not callable(comb_op):
        raise TypeError("fold seq_op and comb_op must be callable")
    initial = as_expr(initial_value)
    from .agg import _validate_initializer
    _validate_initializer(initial, "fold")

    state_id = uuid4().hex
    left_id, right_id = uuid4().hex, uuid4().hex
    dtype = initial.dtype
    # Rebind after promotion: lambda result types can depend on state dtype.
    for _ in range(3):
        state = Expression(dtype, None, {"op": "lambda_variable", "name": state_id})
        left = Expression(dtype, None, {"op": "lambda_variable", "name": left_id})
        right = Expression(dtype, None, {"op": "lambda_variable", "name": right_id})
        seq = as_expr(seq_op(state))
        combine = as_expr(comb_op(left, right))
        promoted = unify_types(dtype, seq.dtype)
        promoted = None if promoted is None else unify_types(promoted, combine.dtype)
        if promoted is None:
            raise TypeError("fold initial_value, seq_op and comb_op have incompatible types")
        if promoted == dtype:
            break
        dtype = promoted
    else:
        raise TypeError("fold state type did not stabilize")
    initial, seq, combine = cast(initial, dtype), cast(seq, dtype), cast(combine, dtype)
    source = _merge_sources(initial.source, _merge_sources(seq.source, combine.source))
    return Expression(initial.dtype, source, {
        "op": "aggregate_fold",
        "initial": initial.to_ir(),
        "seq": seq.to_ir(),
        "combine": combine.to_ir(),
        "state_parameter": state_id,
        "left_parameter": left_id,
        "right_parameter": right_id,
    })
