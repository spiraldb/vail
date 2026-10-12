"""Numeric/statistical aggregators with compile-time parameters.

The public ``agg`` module re-exports these functions. Runtime arguments are
packed in a struct expression so the execution engine always sees one input.
"""
from __future__ import annotations

from .expressions import as_expr, cast
from .types import tarray, tfloat32, tfloat64, tint32, tint64, tstr, tstruct, ttuple


def _numeric(value, name="value"):
    value = as_expr(value)
    if value.dtype not in (tfloat64,):
        # Integer and float32 expressions are cast explicitly by callers when
        # needed; the native numeric kernels accept all four Arrow primitives.
        from .types import tint32, tint64, tfloat32
        if value.dtype not in (tint32, tint64, tfloat32):
            raise TypeError(f"{name} must be numeric")
    return value


def _aggregate(function, value, result_type, *, init=None, **options):
    from .agg import _aggregate as make
    return make(function, value, result_type, options=options, init=init)


def _numeric_parameter(value, name):
    value = as_expr(value)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError(f"{name} must be numeric")
    return value


def corr(x, y):
    """Pearson correlation, using only rows with both values defined."""
    x, y = _numeric(x, "x"), _numeric(y, "y")
    return _aggregate("corr", {"x": x, "y": y}, tfloat64)


def hist(expr, start, end, bins):
    """Aggregate into left-inclusive bins; the final bin also includes end."""
    value = _numeric(expr)
    start_expr = cast(_numeric_parameter(start, "start"), tfloat64)
    end_expr = cast(_numeric_parameter(end, "end"), tfloat64)
    bins_expr = as_expr(bins)
    if bins_expr.dtype != tint32:
        raise TypeError("bins must be an int32 expression")
    result = tstruct(bin_edges=tarray(tfloat64), bin_freq=tarray(tint64),
                     n_smaller=tint64, n_larger=tint64)
    return _aggregate("hist", value, result, init=[("start", start_expr), ("end", end_expr),
                                                    ("bins", bins_expr)])


def approx_quantiles(expr, qs, k=100):
    """Return approximate quantiles using a bounded mergeable sketch."""
    value = _numeric(expr)
    scalar = not isinstance(qs, (list, tuple)) and not isinstance(getattr(qs, "dtype", None), tarray)
    q_expr = _numeric_parameter(qs, "quantiles") if scalar else as_expr(qs)
    if scalar:
        q_expr = cast(q_expr, tfloat64)
    elif not isinstance(q_expr.dtype, tarray) or q_expr.dtype.element_type not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("quantiles must be a numeric expression or array of numeric expressions")
    else:
        q_expr = cast(q_expr, tarray(tfloat64))
    if isinstance(k, bool) or not isinstance(k, int):
        raise TypeError("k must be an integer")
    ret = value.dtype if scalar else tarray(value.dtype)
    quantiles = q_expr if not scalar else as_expr([q_expr])
    return _aggregate("approx_quantiles", value, ret, init=[("quantiles", quantiles)],
                      scalar=scalar, k=k)


def approx_median(expr, k=100):
    """Return the approximate 0.5 quantile."""
    value = _numeric(expr)
    if isinstance(k, bool) or not isinstance(k, int):
        raise TypeError("k must be an integer")
    return _aggregate("approx_median", value, value.dtype,
                      init=[("quantiles", as_expr([0.5]))], scalar=True, k=k)


def approx_cdf(expr, k=100, *, _raw=False):
    """Unsupported until a Hail-compatible CDF sketch representation is available."""
    raise NotImplementedError(
        "approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable"
    )


def linreg(y, x, nested_dim=1, weight=None):
    """Fit a multivariate linear regression over complete observations."""
    y = cast(_numeric(y, "y"), tfloat64)
    xs = x if isinstance(x, (list, tuple)) else [x]
    xs = [cast(_numeric(v, "x"), tfloat64) for v in xs]
    if not xs:
        raise ValueError("x must contain at least one covariate")
    if isinstance(nested_dim, bool) or not isinstance(nested_dim, int):
        raise TypeError("nested_dim must be an integer")
    if not 0 <= nested_dim <= len(xs):
        raise ValueError("nested_dim must be between zero and len(x)")
    data = {"y": y, "x": xs}
    if weight is not None:
        data["weight"] = cast(_numeric(weight, "weight"), tfloat64)
    doubles = tarray(tfloat64)
    result = tstruct(beta=doubles, standard_error=doubles, t_stat=doubles,
                     p_value=doubles, multiple_standard_error=tfloat64,
                     multiple_r_squared=tfloat64, adjusted_r_squared=tfloat64,
                     f_stat=tfloat64, multiple_p_value=tfloat64, n=tint64)
    return _aggregate("linreg", data, result, nested_dim=nested_dim, n_predictors=len(xs))


def downsample(x, y, label=None, n_divisions=500):
    """Select representative (x, y) points from a two-dimensional grid."""
    x, y = _numeric(x, "x"), _numeric(y, "y")
    if isinstance(n_divisions, bool) or not isinstance(n_divisions, int) or n_divisions <= 0:
        raise ValueError("n_divisions must be a positive integer")
    fields = {"x": x, "y": y}
    if label is not None:
        fields["label"] = as_expr(label)
    return _aggregate("downsample", fields, tarray(ttuple(tfloat64, tfloat64, tarray(tstr))),
                      n_divisions=n_divisions)
