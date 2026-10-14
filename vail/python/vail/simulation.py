"""Balding-Nichols simulation, adapted from Hail's Python implementation.

Source: hail-is/hail, methods/statgen.py, e10cc355bc8cd0de9c5f29d5182d31527898bae1.
Copyright (c) 2015-2023 Hail Authors. MIT license: third_party/HAIL_LICENSE.
Vail adaptations use partition counts, explicit row keys, source validation,
and native call construction. All random draws remain lazy expressions.
"""
from __future__ import annotations

import itertools
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
    are :math:`a = (1 - p) \frac{1 - F}{F}` and :math:`b = p \frac{1 - F}{F}` with
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
        Default is 1 partition per million entries or 8, whichever is larger.
    pop_dist : :obj:`list` of :obj:`float`, optional
        Unnormalized population distribution, a list of length
        `n_populations` with non-negative values.
        Default is ``[1, ..., 1]``.
    fst : :obj:`list` of :obj:`float`, optional
        :math:`F_{ST}` values, a list of length `n_populations` with values
        in (0, 1). Default is ``[0.1, ..., 0.1]``.
    af_dist : :class:`.Float64Expression`, optional
        Representing a random function.  Ancestral allele frequency
        distribution.  Default is :func:`.rand_unif` over the range
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
    if pop_dist is None:
        pop_dist = [1 for _ in range(n_populations)]

    if fst is None:
        fst = [0.1 for _ in range(n_populations)]

    if af_dist is None:
        af_dist = hl.rand_unif(0.1, 0.9, seed=0)

    if n_partitions is None:
        n_partitions = max(8, int(n_samples * n_variants / (128 * 1024 * 1024)))

    # verify args
    for name, var in {
        "populations": n_populations,
        "samples": n_samples,
        "variants": n_variants,
        "partitions": n_partitions,
    }.items():
        if var < 1:
            raise ValueError("n_{} must be positive, got {}".format(name, var))

    for name, var in {"pop_dist": pop_dist, "fst": fst}.items():
        if len(var) != n_populations:
            raise ValueError(
                "{} must be of length n_populations={}, got length {}".format(name, n_populations, len(var))
            )

    if any(x < 0 for x in pop_dist):
        raise ValueError("pop_dist must be non-negative, got {}".format(pop_dist))

    if any(x <= 0 or x >= 1 for x in fst):
        raise ValueError("elements of fst must satisfy 0 < x < 1, got {}".format(fst))

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

    info(
        "balding_nichols_model: generating genotypes for {} populations, {} samples, and {} variants...".format(
            n_populations, n_samples, n_variants
        )
    )

    # generate matrix table
    from numpy import linspace

    n_partitions = min(n_partitions, n_variants)
    start_idxs = [int(x) for x in linspace(0, n_variants, n_partitions + 1)]
    idx_bounds = list(itertools.pairwise(start_idxs))

    pop_f = hl.rand_dirichlet if mixture else hl.rand_cat

    bn = hl.Table._generate(
        contexts=idx_bounds,
        globals=hl.struct(
            bn=hl.struct(
                n_populations=n_populations,
                n_samples=n_samples,
                n_variants=n_variants,
                n_partitions=n_partitions,
                pop_dist=pop_dist,
                fst=fst,
                mixture=mixture,
            ),
            cols=hl.range(n_samples).map(lambda idx: hl.struct(sample_idx=idx, pop=pop_f(pop_dist))),
        ),
        partitions=n_partitions,
        rowfn=lambda idx_range, _: hl.range(idx_range[0], idx_range[1]).map(
            lambda idx: hl.bind(
                lambda ancestral: hl.struct(
                    locus=hl.locus_from_global_position(idx, reference_genome),
                    alleles=['A', 'C'],
                    ancestral_af=ancestral,
                    af=hl.array([(1 - x) / x for x in fst]).map(
                        lambda x: hl.rand_beta(ancestral * x, (1 - ancestral) * x)
                    ),
                    entries=hl.repeat(hl.struct(), n_samples),
                ),
                af_dist,
            )
        ),
    )

    bn = bn._with_keys('locus', 'alleles')._unlocalize_entries('entries', 'cols', ['sample_idx'])

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
