"""Balding-Nichols simulation, adapted from Hail's Python implementation.

Source: hail-is/hail, methods/statgen.py, e10cc355bc8cd0de9c5f29d5182d31527898bae1.
Copyright (c) 2015-2023 Hail Authors. MIT license: third_party/HAIL_LICENSE.
Vail adaptations use partition counts, explicit row keys, source validation,
and native call construction. All random draws remain lazy expressions.
"""
from __future__ import annotations

import math
from numbers import Real
from logging import info
from typing import List, Optional, Union
import vail as hl
from .expressions import Expression, ExpressionException
from .matrixtable import MatrixTable
from .types import tfloat64


def balding_nichols_model(
    n_populations: int,
    n_samples: int,
    n_variants: int,
    n_partitions: Optional[int] = None,
    pop_dist: Optional[List[int]] = None,
    fst: Optional[List[Union[float, int]]] = None,
    af_dist: Optional[Expression] = None,
    reference_genome: str = 'default',
    mixture: bool = False,
    *,
    phased: bool = False,
) -> MatrixTable:
    r"""Generate a matrix table of variants, samples, and genotypes using the
    Balding-Nichols or Pritchard-Stephens-Donnelly model.

    Examples
    --------
    Generate a matrix table of genotypes with 1000 variants and 100 samples
    across 3 populations:

    >>> hl.reset_global_randomness()
    >>> bn_ds = hl.balding_nichols_model(3, 100, 1000)
    >>> bn_ds.show(n_rows=5, n_cols=5)
    +---------------+------------+------+------+------+------+------+
    | locus         | alleles    | 0.GT | 1.GT | 2.GT | 3.GT | 4.GT |
    +---------------+------------+------+------+------+------+------+
    | locus<GRCh37> | array<str> | call | call | call | call | call |
    +---------------+------------+------+------+------+------+------+
    | 1:1           | ["A","C"]  | 0/1  | 0/0  | 0/1  | 0/0  | 0/0  |
    | 1:2           | ["A","C"]  | 1/1  | 1/1  | 1/1  | 1/1  | 0/1  |
    | 1:3           | ["A","C"]  | 0/1  | 0/1  | 1/1  | 0/1  | 1/1  |
    | 1:4           | ["A","C"]  | 0/1  | 0/0  | 0/1  | 0/0  | 0/1  |
    | 1:5           | ["A","C"]  | 0/1  | 0/1  | 0/1  | 0/0  | 0/0  |
    +---------------+------------+------+------+------+------+------+
    showing top 5 rows
    showing the first 5 of 100 columns

    Generate a dataset as above but with phased genotypes:

    >>> hl.reset_global_randomness()
    >>> bn_ds = hl.balding_nichols_model(3, 100, 1000, phased=True)
    >>> bn_ds.show(n_rows=5, n_cols=5)
    +---------------+------------+------+------+------+------+------+
    | locus         | alleles    | 0.GT | 1.GT | 2.GT | 3.GT | 4.GT |
    +---------------+------------+------+------+------+------+------+
    | locus<GRCh37> | array<str> | call | call | call | call | call |
    +---------------+------------+------+------+------+------+------+
    | 1:1           | ["A","C"]  | 0|0  | 0|0  | 0|0  | 0|0  | 1|0  |
    | 1:2           | ["A","C"]  | 1|1  | 1|1  | 1|1  | 1|1  | 1|1  |
    | 1:3           | ["A","C"]  | 1|1  | 1|1  | 0|1  | 1|1  | 1|1  |
    | 1:4           | ["A","C"]  | 0|0  | 1|0  | 0|0  | 1|0  | 0|0  |
    | 1:5           | ["A","C"]  | 0|0  | 0|1  | 0|0  | 0|0  | 0|0  |
    +---------------+------------+------+------+------+------+------+
    showing top 5 rows
    showing the first 5 of 100 columns

    Generate a matrix table using 4 populations, 40 samples, 150 variants, 3
    partitions, population distribution ``[0.1, 0.2, 0.3, 0.4]``,
    :math:`F_{ST}` values ``[.02, .06, .04, .12]``, ancestral allele
    frequencies drawn from a truncated beta distribution with ``a = 0.01`` and
    ``b = 0.05`` over the interval ``[0.05, 1]``, and random seed 1:

    >>> hl.reset_global_randomness()
    >>> bn_ds = hl.balding_nichols_model(4, 40, 150, 3,
    ...          pop_dist=[0.1, 0.2, 0.3, 0.4],
    ...          fst=[.02, .06, .04, .12],
    ...          af_dist=hl.rand_beta(a=0.01, b=2.0, lower=0.05, upper=1.0))

    To guarantee reproducibility, we set the Hail global seed with
    :func:`.set_global_seed` immediately prior to generating the dataset.

    Notes
    -----
    This method simulates a matrix table of variants, samples, and genotypes
    using the Balding-Nichols model, which we now define.

    - :math:`K` populations are labeled by integers :math:`0, 1, \dots, K - 1`.
    - :math:`N` samples are labeled by strings :math:`0, 1, \dots, N - 1`.
    - :math:`M` variants are defined as ``1:1:A:C``, ``1:2:A:C``, ...,
      ``1:M:A:C``.
    - The default distribution for population assignment :math:`\pi` is uniform.
    - The default ancestral frequency distribution :math:`P_0` is uniform on
      :math:`[0.1, 0.9]`.
      All three classes are located in ``hail.stats``.
    - The default :math:`F_{ST}` values are all :math:`0.1`.

    The Balding-Nichols model models genotypes of individuals from a structured
    population comprising :math:`K` homogeneous modern populations that have
    each diverged from a single ancestral population (a `star phylogeny`). Each
    sample is assigned a population by sampling from the categorical
    distribution :math:`\pi`. Note that the actual size of each population is
    random.

    Variants are modeled as biallelic and unlinked. Ancestral allele
    frequencies are drawn independently for each variant from a frequency
    spectrum :math:`P_0`. The extent of genetic drift of each modern population
    from the ancestral population is defined by the corresponding :math:`F_{ST}`
    parameter :math:`F_k` (here and below, lowercase indices run over a range
    bounded by the corresponding uppercase parameter, e.g. :math:`k = 1, \ldots,
    K`). For each variant and population, allele frequencies are drawn from a
    `beta distribution <https://en.wikipedia.org/wiki/Beta_distribution>`__
    whose parameters are determined by the ancestral allele frequency and
    :math:`F_{ST}` parameter. The beta distribution gives a continuous
    approximation of the effect of genetic drift. We denote sample population
    assignments by :math:`k_n`, ancestral allele frequencies by :math:`p_m`,
    population allele frequencies by :math:`p_{k, m}`, and diploid, unphased
    genotype calls by :math:`g_{n, m}` (0, 1, and 2 correspond to homozygous
    reference, heterozygous, and homozygous variant, respectively).

    The generative model is then given by:

    .. math::
        \begin{aligned}
            k_n \,&\sim\, \pi \\
            p_m \,&\sim\, P_0 \\
            p_{k,m} \mid p_m\,&\sim\, \mathrm{Beta}(\mu = p_m,\, \sigma^2 = F_k p_m (1 - p_m)) \\
            g_{n,m} \mid k_n, p_{k, m} \,&\sim\, \mathrm{Binomial}(2, p_{k_n, m})
        \end{aligned}

    The beta distribution by its mean and variance above; the usual parameters
    are :math:`a = p \frac{1 - F}{F}` and :math:`b = (1 - p) \frac{1 - F}{F}` with
    :math:`F = F_k` and :math:`p = p_m`.

    The resulting dataset has the following fields.

    Global fields:

    - `bn.n_populations` (:py:data:`.tint32`) -- Number of populations.
    - `bn.n_samples` (:py:data:`.tint32`) -- Number of samples.
    - `bn.n_variants` (:py:data:`.tint32`) -- Number of variants.
    - `bn.n_partitions` (:py:data:`.tint32`) -- Number of partitions.
    - `bn.pop_dist` (:class:`.tarray` of :py:data:`.tfloat64`) -- Population distribution indexed by
      population.
    - `bn.fst` (:class:`.tarray` of :py:data:`.tfloat64`) -- :math:`F_{ST}` values indexed by
      population.
    - `bn.seed` (:py:data:`.tint32`) -- Random seed.
    - `bn.mixture` (:py:data:`.tbool`) -- Value of `mixture` parameter.

    Row fields:

    - `locus` (:class:`.tlocus`) -- Variant locus (key field).
    - `alleles` (:class:`.tarray` of :py:data:`.tstr`) -- Variant alleles (key field).
    - `ancestral_af` (:py:data:`.tfloat64`) -- Ancestral allele frequency.
    - `af` (:class:`.tarray` of :py:data:`.tfloat64`) -- Modern allele frequencies indexed by
      population.

    Column fields:

    - `sample_idx` (:py:data:`.tint32`) - Sample index (key field).
    - `pop` (:py:data:`.tint32`) -- Population of sample.

    Entry fields:

    - `GT` (:py:data:`.tcall`) -- Genotype call (diploid, unphased).

    For the `Pritchard-Stephens-Donnelly model <http://www.genetics.org/content/155/2/945.long>`__,
    set the `mixture` to true to treat `pop_dist` as the parameters of the
    Dirichlet distribution describing admixture between the modern populations.
    In this case, the type of `pop` is :class:`.tarray` of
    :py:data:`.tfloat64` and the value is the mixture proportions.

    Parameters
    ----------
    n_populations : :obj:`int`
        Number of modern populations.
    n_samples : :obj:`int`
        Total number of samples.
    n_variants : :obj:`int`
        Number of variants.
    n_partitions : :obj:`int`, optional
        Number of partitions.
        Default is one partition per 128 * 1024 * 1024 entries or eight,
        whichever is larger, capped at the number of variants.
    pop_dist : :obj:`list` of :obj:`float`, optional
        Unnormalized population distribution, a list of length
        `n_populations` with finite non-negative values and at least one positive value.
        Default is ``[1, ..., 1]``.
    fst : :obj:`list` of :obj:`float`, optional
        :math:`F_{ST}` values, a list of length `n_populations` with values
        in (0, 1), with finite concentration ``(1 - fst) / fst``.
        Default is ``[0.1, ..., 0.1]``.
    af_dist : :class:`.Float64Expression`, optional
        Representing a random function.  Ancestral allele frequency
        distribution. Realized values must be defined and finite in [0, 1];
        endpoints produce deterministic fixation. Default is :func:`.rand_unif` over the range
        `[0.1, 0.9]` with seed 0.
    reference_genome : :class:`str` or :class:`.ReferenceGenome`
        Reference genome to use.
    mixture : :obj:`bool`
        Treat `pop_dist` as the parameters of a Dirichlet distribution,
        as in the Prichard-Stevens-Donnelly model.
    phased : :obj:`bool`
        Generate phased genotypes.

    Returns
    -------
    :class:`.MatrixTable`
        Simulated matrix table of variants, samples, and genotypes.

    """
    # Native range and array positions are int32. Check counts before using
    # them for defaults, partition metadata, or population-sized allocations.
    def count(value, name):
        if type(value) is not int:
            raise TypeError(f'{name} must be an integer')
        if not 1 <= value <= 2**31 - 1:
            raise ValueError(f'{name} must be positive and fit int32')
        return value

    for name, value in [('n_populations', n_populations), ('n_samples', n_samples),
                        ('n_variants', n_variants)]:
        count(value, name)
    if type(mixture) is not bool or type(phased) is not bool:
        raise TypeError('mixture and phased must be bool')
    if n_partitions is None:
        n_partitions = min(n_variants, max(8, n_samples * n_variants // (128 * 1024 * 1024)))
    else:
        count(n_partitions, 'n_partitions')
        n_partitions = min(n_partitions, n_variants)
    if pop_dist is None:
        pop_dist = [1] * n_populations
    if fst is None:
        fst = [0.1] * n_populations

    def parameters(values, name):
        values = list(values)
        if len(values) != n_populations:
            raise ValueError(f'{name} must be of length n_populations={n_populations}')
        if any(isinstance(value, bool) or not isinstance(value, Real) for value in values):
            raise TypeError(f'{name} values must be numeric')
        try:
            values = [float(value) for value in values]
        except OverflowError as error:
            raise ValueError(f'{name} values must be finite') from error
        if any(not math.isfinite(value) for value in values):
            raise ValueError(f'{name} values must be finite')
        return values

    pop_dist = parameters(pop_dist, 'pop_dist')
    fst = parameters(fst, 'fst')
    if any(value < 0 for value in pop_dist) or not any(value > 0 for value in pop_dist):
        raise ValueError('pop_dist must be nonnegative with at least one positive weight')
    if any(not 0 < value < 1 for value in fst):
        raise ValueError('elements of fst must satisfy 0 < x < 1')
    concentrations = [(1 - value) / value for value in fst]
    if any(not math.isfinite(value) for value in concentrations):
        raise ValueError('fst produces an unsupported nonfinite beta concentration')
    if af_dist is None:
        af_dist = hl.rand_unif(0.1, 0.9, seed=0)

    # verify af_dist
    af_dist = hl.as_expr(af_dist)
    if af_dist.source is not None:
        raise ExpressionException(
            'balding_nichols_model expects af_dist to '
            + 'have scalar arguments: found expression '
            + 'from source {}'.format(af_dist.source)
        )

    if af_dist.dtype != tfloat64:
        raise ValueError("af_dist must be a hail function with return type tfloat64.")

    af_dist = hl.bind(lambda value: hl.case(missing_false=True).when(
        (value >= 0) & (value <= 1), value).or_error(
        'ancestral allele frequency must be defined, finite and in [0,1]'), af_dist)

    info(
        "balding_nichols_model: generating genotypes for {} populations, {} samples, and {} variants...".format(
            n_populations, n_samples, n_variants
        )
    )

    # The original generator nested context and row array callbacks. Retain
    # their exact UID words while streaming rows from a bounded range source.
    # floor partition bounds invert to ((idx + 1) * partitions - 1) // variants.
    def generated_row(source):
        idx = source.idx
        partition = ((hl.int64(idx) + 1) * n_partitions - 1) // n_variants
        local = hl.int64(idx) - partition * n_variants // n_partitions
        row = hl.bind(
            lambda ancestral: hl.struct(
                locus=hl.locus_from_global_position(idx, reference_genome),
                alleles=['A', 'C'],
                ancestral_af=ancestral,
                af=hl.literal(concentrations).map(lambda x: hl.if_else(
                    ancestral * x == 0, 0., hl.if_else(
                        (1 - ancestral) * x == 0, 1.,
                        hl.rand_beta(ancestral * x, (1 - ancestral) * x)))),
                entries=hl.repeat(hl.struct(), n_samples),
            ), af_dist)
        return row, [partition, local]

    pop_f = hl.rand_dirichlet if mixture else hl.rand_cat
    # Preserve ordinary categorical rounding and seeds. Only overflowed totals
    # need rescaling; finite weights always have a finite positive maximum.
    sampling_weights = pop_dist
    if not mixture and not math.isfinite(sum(pop_dist)):
        largest = max(pop_dist)
        sampling_weights = [weight / largest for weight in pop_dist]
    bn = hl.Table._generate_rows(
        n=n_variants, partitions=n_partitions,
        # Bound placeholder entries to roughly 64K per produced batch. A row
        # remains indivisible and therefore requires O(n_samples) storage.
        batch_size=max(1, min(1024, 65536 // n_samples)),
        globals=dict(
            bn=hl.struct(
                n_populations=n_populations, n_samples=n_samples,
                n_variants=n_variants, n_partitions=n_partitions,
                pop_dist=pop_dist, fst=fst, mixture=mixture),
            cols=hl.range(n_samples).map(lambda idx: hl.struct(sample_idx=idx, pop=pop_f(sampling_weights)))),
        rowfn=generated_row,
    )

    bn = bn._with_keys('locus', 'alleles')._unlocalize_entries('entries', 'cols', ['sample_idx'], _aligned=True)

    # entry info
    p = hl.sum(bn.pop * bn.af) if mixture else bn.af[bn.pop]
    q = 1 - p

    if phased:
        bn = bn.annotate_entries(bn_mom=hl.rand_bool(p), bn_dad=hl.rand_bool(p))
        return bn.select_entries(GT=hl.call(hl.int32(bn.bn_mom), hl.int32(bn.bn_dad), phased=True))

    # Retain the draw as an entry field so call construction does not duplicate
    # the categorical expression and its column metadata throughout the plan.
    bn = bn.annotate_entries(bn_gt_index=hl.rand_cat([q**2, 2 * p * q, p**2]))
    idx = bn.bn_gt_index
    return bn.select_entries(GT=hl.if_else(
        idx == 0, hl.call(0, 0), hl.if_else(idx == 1, hl.call(0, 1), hl.call(1, 1))
    ))
