"""Genomics and ndarray aggregation expression constructors."""
from __future__ import annotations

from .expressions import struct, as_expr, case, cast, if_else, is_defined, literal, missing
from .types import tarray, tbool, tcall, tfloat32, tfloat64, tint32, tndarray, tstr, tstruct


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
    packed = struct(**{"__vail_genomic_call": call, "__vail_genomic_alleles": alleles})
    n_alleles = alleles if alleles.dtype == tint32 else alleles.length()
    return _aggregate("call_stats", packed, result, init=[("n_alleles", n_alleles)])


def hardy_weinberg_test(expr, one_sided=False):
    """Aggregate calls into HWE heterozygote frequency and exact-test p-value."""
    call = as_expr(expr)
    if call.dtype != tcall:
        raise TypeError("hardy_weinberg_test requires a Call expression")
    if not isinstance(one_sided, bool):
        one_sided = as_expr(one_sided)
        if one_sided.dtype != tbool:
            raise TypeError("one_sided must be a bool expression")
    from . import agg
    # HWE classifies diploid calls by the number of non-reference alleles.
    # The exact-test kernel is scalar, so the three native count reducers can
    # share the same grouped/scan execution as other aggregates.
    diploid = call.ploidy == 2
    counts = [agg.count_where(diploid & (call.n_alt_alleles() == n))
              for n in range(3)]
    from .qc import hardy_weinberg_test as exact_test
    return exact_test(*counts, one_sided=one_sided)


def inbreeding(expr, prior):
    """Aggregate calls and expected heterozygote frequencies into F estimates."""
    call = as_expr(expr)
    prior = as_expr(prior)
    if call.dtype != tcall:
        raise TypeError("inbreeding requires a Call expression")
    if prior.dtype != tfloat64:
        raise TypeError("inbreeding prior must be float64")
    from . import agg
    paired = is_defined(call) & is_defined(prior)
    biallelic_diploid = if_else(
        paired, (call.ploidy == 2) & (call[0] <= 1) & (call[1] <= 1), True)
    allowed = (~paired) | biallelic_diploid
    valid = (case(missing_false=True)
             .when(allowed, True)
             .or_error("'inbreeding' does not support non-diploid or multiallelic genotypes"))
    n_called = agg.count_where(paired & valid)
    is_hom = if_else(paired, call[0] == call[1], False)
    observed_homs = agg.count_where(paired & is_hom & valid)
    expected_per_call = 1. - 2. * prior * (1. - prior)
    expected_homs = agg.sum(if_else(paired & valid, expected_per_call, missing(tfloat64)))
    f_stat = (cast(observed_homs, tfloat64) - expected_homs) / (cast(n_called, tfloat64) - expected_homs)
    return struct(**{"f_stat": f_stat, "n_called": n_called,
                    "expected_homs": expected_homs, "observed_homs": observed_homs})


def info_score(gp):
    """Aggregate genotype probability vectors into an imputation info score."""
    probabilities = as_expr(gp)
    if not isinstance(probabilities.dtype, tarray) or probabilities.dtype.element_type not in (tfloat32, tfloat64):
        raise TypeError("info_score requires an array<float> expression")
    from . import agg
    present = is_defined(probabilities)
    allowed = (~present) | ((probabilities.length() == 3)
                            & is_defined(probabilities[0])
                            & is_defined(probabilities[1])
                            & is_defined(probabilities[2]))
    valid = (case(missing_false=True)
             .when(allowed, True)
             .or_error("'info_score': expected three defined genotype probabilities"))
    g0, g1, g2 = (cast(probabilities[i], tfloat64) for i in range(3))
    mean = g1 + 2. * g2
    include = present & valid
    n = agg.count_where(include)
    mean_sum = agg.sum(if_else(include, mean, missing(tfloat64)))
    variance_sum = agg.sum(if_else(include, g1 + 4. * g2 - mean * mean, missing(tfloat64)))
    probability_sum = agg.sum(if_else(include, g0 + g1 + g2, missing(tfloat64)))
    theta = if_else(probability_sum != 0., mean_sum / probability_sum, literal(float("nan"), tfloat64))
    score = if_else(n == 0, missing(tfloat64),
                    if_else((theta == 0.) | (theta == 1.), 1.,
                            1. - (variance_sum / cast(n, tfloat64)) /
                            (2. * theta * (1. - theta))))
    n32 = (case()
           .when(n <= 2**31 - 1, cast(n, tint32))
           .or_error("info_score count overflow"))
    return struct(**{"score": score, "n_included": n32})


def ndarray_sum(expr):
    """Sum same-shaped ndarrays elementwise across rows."""
    value = as_expr(expr)
    if not isinstance(value.dtype, tndarray):
        raise TypeError("ndarray_sum requires an ndarray expression")
    return _aggregate("ndarray_sum", value, value.dtype)


def _aggregate(function, value, result_type, *, options=None, init=None):
    from .agg import _aggregate as aggregate
    return aggregate(function, value, result_type, options=options, init=init)
