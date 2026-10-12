"""Genomics and ndarray aggregation expression constructors."""
from __future__ import annotations

from .expressions import as_expr, if_else
from .types import tarray, tbool, tcall, tfloat32, tfloat64, tint32, tint64, tndarray, tstr, tstruct


def call_stats(call, alleles):
    """Compute allele counts, frequencies, allele number, and hom count."""
    call = as_expr(call)
    alleles = as_expr(alleles)
    if call.dtype != tcall:
        raise TypeError("call_stats requires a Call expression")
    if alleles.dtype != tint32 and (not isinstance(alleles.dtype, tarray) or alleles.dtype.element_type != tstr):
        raise TypeError("call_stats alleles must be an int32 count or array<str>")
    result = tstruct(AC=tarray(tint32), AF=tarray(tfloat64), AN=tint32,
                     homozygote_count=tarray(tint32))
    packed = as_expr({"__vail_genomic_call": call, "__vail_genomic_alleles": alleles})
    n_alleles = alleles if alleles.dtype == tint32 else alleles.length()
    return _aggregate("call_stats", packed, result, init=[("n_alleles", n_alleles)])


def hardy_weinberg_test(expr, one_sided=False):
    """Aggregate calls into HWE heterozygote frequency and exact-test p-value."""
    call = as_expr(expr)
    if call.dtype != tcall:
        raise TypeError("hardy_weinberg_test requires a Call expression")
    result_type = tstruct(het_freq_hwe=tfloat64, p_value=tfloat64)
    def test(mode):
        return _aggregate("hardy_weinberg_test", call, result_type,
                          options={"one_sided": mode})
    if isinstance(one_sided, bool):
        return test(one_sided)
    mode = as_expr(one_sided)
    if mode.dtype != tbool:
        raise TypeError("one_sided must be a bool expression")
    # Hail applies the mode after counting calls. Keep it in the result
    # projection so globals and aggregate-valued modes remain lazy.
    return if_else(mode, test(True), test(False))


def inbreeding(expr, prior):
    """Aggregate calls and expected heterozygote frequencies into F estimates."""
    call = as_expr(expr)
    prior = as_expr(prior)
    if call.dtype != tcall:
        raise TypeError("inbreeding requires a Call expression")
    if prior.dtype != tfloat64:
        raise TypeError("inbreeding prior must be float64")
    packed = as_expr({"__vail_genomic_call": call, "__vail_genomic_prior": prior})
    return _aggregate("inbreeding", packed,
                      tstruct(f_stat=tfloat64, n_called=tint64,
                              expected_homs=tfloat64, observed_homs=tint64))


def info_score(gp):
    """Aggregate genotype probability vectors into an imputation info score."""
    probabilities = as_expr(gp)
    if not isinstance(probabilities.dtype, tarray) or probabilities.dtype.element_type not in (tfloat32, tfloat64):
        raise TypeError("info_score requires an array<float> expression")
    return _aggregate("info_score", probabilities,
                      tstruct(score=tfloat64, n_included=tint32))


def ndarray_sum(expr):
    """Sum same-shaped ndarrays elementwise across rows."""
    value = as_expr(expr)
    if not isinstance(value.dtype, tndarray):
        raise TypeError("ndarray_sum requires an ndarray expression")
    return _aggregate("ndarray_sum", value, value.dtype)


def _aggregate(function, value, result_type, *, options=None, init=None):
    from .agg import _aggregate as aggregate
    return aggregate(function, value, result_type, options=options, init=init)
