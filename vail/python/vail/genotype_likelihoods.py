"""Genotype likelihood expressions composed from DataFusion array arithmetic."""
from .collections import _function
from .expressions import as_expr, bind, coalesce, if_else, is_missing, literal, missing
from .types import tarray, tint32, tfloat64


def gq_from_pl(pl):
    """Difference between the two least defined PLs, seeded with 99 twice."""
    from . import sorted as sort_values
    pl = as_expr(pl)
    if pl.dtype != tarray(tint32):
        raise TypeError('gq_from_pl requires an array of int32 likelihoods')
    ordered = sort_values(pl.filter(lambda value: ~is_missing(value)).extend([99, 99]))
    return if_else(is_missing(pl), missing(tint32), ordered[1] - ordered[0])


def unphased_diploid_gt_index_call(gt_index):
    """Invert the triangular VCF diploid genotype ordering."""
    from .variant import _kernel
    from .types import tcall
    index = as_expr(gt_index)
    if index.dtype != tint32:
        raise TypeError('genotype index must be int32')
    return _kernel('unphased_diploid_gt_index_call', tcall, index)


def gp_dosage(gp):
    """Expected alternate-allele count for three genotype probabilities."""
    from . import case, float64
    gp = as_expr(gp)
    if not isinstance(gp.dtype, tarray) or gp.dtype.element_type not in (tint32, tfloat64):
        raise TypeError('gp_dosage requires an array of numeric probabilities')
    gp = gp.map(float64) if gp.dtype.element_type == tint32 else gp
    return bind(lambda values: if_else(is_missing(values), missing(tfloat64),
        case().when(values.length() == 3, values[1] + 2.0 * values[2])
        .or_error('gp_dosage requires exactly three probabilities')), gp)


def _pl_probabilities(pl, _cache_size):
    """Construct unnormalized probabilities with Hail's cache-index rules."""
    from . import case, float64

    try:
        pl = as_expr(pl)
    except ValueError:
        # The argument type supplies the missing element type for [] / None.
        pl = literal(pl, tarray(tint32))
    if pl.dtype != tarray(tint32):
        raise TypeError("pl_to_gp requires array<int32>")
    if type(_cache_size) is not int:
        raise TypeError("_cache_size must be int")

    def probability(value):
        # Hail indexes its cached probability array with negative PLs. Match
        # that wraparound analytically, without broadcasting a lookup table.
        if _cache_size > 0:
            index = case().when(value >= -_cache_size,
                if_else(value < 0, value + _cache_size, value)
            ).or_error("PL probability cache index out of bounds")
        else:
            index = case().when(value >= _cache_size, value).or_error(
                "PL probability cache index out of bounds")
        # The checked index and numeric operators already propagate nulls.
        return 10.0 ** (-float64(index) / 10.0)

    return pl.map(probability)


def pl_to_gp(pl, _cache_size=2048):
    """Normalize Phred-scaled int32 likelihoods using a uniform prior."""
    return bind(lambda probabilities: probabilities / coalesce(
        _function("array_sum", [probabilities], tfloat64), 0.0,
    ), _pl_probabilities(pl, _cache_size))


def pl_dosage(pl):
    """Return expected alternate-allele count for three genotype likelihoods."""
    from . import case

    def dosage(probabilities):
        # A dosage only needs one weighted reduction, not a normalized list.
        # Strict scalar addition also propagates a missing probability at any
        # of the three positions without a separate list missingness scan.
        value = bind(lambda p0, p1, p2: (p1 + 2.0 * p2) / (p0 + p1 + p2),
                     probabilities[0], probabilities[1], probabilities[2])
        return case().when(probabilities.length() == 3, value).or_error(
            "pl_dosage requires exactly three likelihoods")

    return bind(dosage, _pl_probabilities(pl, 2048))
