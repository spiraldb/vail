"""Variant quality control, ported from Hail methods/qc.py (MIT; see licenses/HAIL.txt)."""

def variant_qc(mt, name='variant_qc'):
    """Compute common variant statistics (quality control metrics).

    .. include:: ../_templates/req_tvariant.rst

    Examples
    --------

    >>> dataset_result = hl.variant_qc(dataset)

    Notes
    -----
    This method computes variant statistics from the genotype data, returning
    a new struct field `name` with the following metrics based on the fields
    present in the entry schema.

    If `mt` contains an entry field `DP` of type :py:data:`.tint32`, then the
    field `dp_stats` is computed. If `mt` contains an entry field `GQ` of type
    :py:data:`.tint32`, then the field `gq_stats` is computed. Both `dp_stats`
    and `gq_stats` are structs with with four fields:

    - `mean` (``float64``) -- Mean value.
    - `stdev` (``float64``) -- Standard deviation (zero degrees of freedom).
    - `min` (``int32``) -- Minimum value.
    - `max` (``int32``) -- Maximum value.

    If the dataset does not contain an entry field `GT` of type
    :py:data:`.tcall`, then an error is raised. The following fields are always
    computed from `GT`:

    - `AF` (``array<float64>``) -- Calculated allele frequency, one element
      per allele, including the reference. Sums to one. Equivalent to
      `AC` / `AN`.
    - `AC` (``array<int32>``) -- Calculated allele count, one element per
      allele, including the reference. Sums to `AN`.
    - `AN` (``int32``) -- Total number of called alleles.
    - `homozygote_count` (``array<int32>``) -- Number of homozygotes per
      allele. One element per allele, including the reference.
    - `call_rate` (``float64``) -- Fraction of calls neither missing nor filtered.
      Equivalent to `n_called` / :meth:`.count_cols`.
    - `n_called` (``int64``) -- Number of samples with a defined `GT`.
    - `n_not_called` (``int64``) -- Number of samples with a missing `GT`.
    - `n_filtered` (``int64``) -- Number of filtered entries.
    - `n_het` (``int64``) -- Number of heterozygous samples.
    - `n_non_ref` (``int64``) -- Number of samples with at least one called
      non-reference allele.
    - `het_freq_hwe` (``float64``) -- Expected frequency of heterozygous
      samples under Hardy-Weinberg equilibrium. See
      :func:`.functions.hardy_weinberg_test` for details.
    - `p_value_hwe` (``float64``) -- p-value from two-sided test of Hardy-Weinberg
      equilibrium. See :func:`.functions.hardy_weinberg_test` for details.
    - `p_value_excess_het` (``float64``) -- p-value from one-sided test of
      Hardy-Weinberg equilibrium for excess heterozygosity.
      See :func:`.functions.hardy_weinberg_test` for details.

    Warning
    -------
    `het_freq_hwe` and `p_value_hwe` are calculated as in
    :func:`.functions.hardy_weinberg_test`, with non-diploid calls
    (``ploidy != 2``) ignored in the counts. As this test is only
    statistically rigorous in the biallelic setting, :func:`.variant_qc`
    sets both fields to missing for multiallelic variants. Consider using
    :func:`~hail.methods.split_multi` to split multi-allelic variants beforehand.

    Parameters
    ----------
    mt : :class:`.MatrixTable`
        Dataset.
    name : :class:`str`
        Name for resulting field.

    Returns
    -------
    :class:`.MatrixTable`
    """
    import vail as hl

    if not isinstance(mt, hl.MatrixTable):
        raise TypeError("variant_qc requires a MatrixTable")
    if 'alleles' not in mt.row.dtype or mt.alleles.dtype != hl.tarray(hl.tstr):
        raise ValueError("Method 'variant_qc' requires a field 'alleles' (type 'array<str>')")

    bound_exprs = {}
    gq_dp_exprs = {}

    def has_field_of_type(name, dtype):
        return name in mt.entry and mt[name].dtype == dtype

    if has_field_of_type('DP', hl.tint32):
        gq_dp_exprs['dp_stats'] = hl.agg.stats(mt.DP).select('mean', 'stdev', 'min', 'max')

    if has_field_of_type('GQ', hl.tint32):
        gq_dp_exprs['gq_stats'] = hl.agg.stats(mt.GQ).select('mean', 'stdev', 'min', 'max')

    if not has_field_of_type('GT', hl.tcall):
        raise ValueError("'variant_qc': expect an entry field 'GT' of type 'call'")

    bound_exprs['n_called'] = hl.agg.count_where(hl.is_defined(mt['GT']))
    bound_exprs['n_not_called'] = hl.agg.count_where(hl.is_missing(mt['GT']))
    n_cols_ref = mt._num_cols()
    bound_exprs['n_filtered'] = hl.int64(n_cols_ref) - hl.agg.count()
    bound_exprs['call_stats'] = hl.agg.call_stats(mt.GT, mt.alleles)
    bound_exprs['n_het'] = hl.agg.count_where(mt.GT.is_het())
    bound_exprs['n_non_ref'] = hl.agg.count_where(mt.GT.is_non_ref())
    # HWE counts exactly 0/0, 0/1 and 1/1, irrespective of phasing.
    # Call equality compares fixed storage fields without an allele-list copy.
    for label, a, b in [('hom_ref', 0, 0), ('het', 0, 1), ('hom_var', 1, 1)]:
        unphased = mt.GT == hl.call(a, b)
        phased = mt.GT == hl.call(a, b, phased=True)
        if a != b:
            phased = phased | (mt.GT == hl.call(b, a, phased=True))
        bound_exprs['hwe_' + label] = hl.agg.count_where(unphased | phased)

    result = hl.rbind(
        hl.struct(**bound_exprs),
        lambda e1: hl.rbind(
            hl.case()
            .when(
                hl.len(mt.alleles) == 2,
                _hwe_both(e1.hwe_hom_ref, e1.hwe_het, e1.hwe_hom_var),
            )
            .or_missing(),
            lambda hwe: hl.struct(**{
                **gq_dp_exprs,
                **e1.call_stats,
                'call_rate': hl.float(e1.n_called) / (e1.n_called + e1.n_not_called + e1.n_filtered),
                'n_called': e1.n_called,
                'n_not_called': e1.n_not_called,
                'n_filtered': e1.n_filtered,
                'n_het': e1.n_het,
                'n_non_ref': e1.n_non_ref,
                'het_freq_hwe': hwe.het_freq_hwe,
                'p_value_hwe': hwe.p_value,
                'p_value_excess_het': hwe.p_value_excess_het,
            }),
        ),
    )

    return mt.annotate_rows(**{name: result})



def hardy_weinberg_test(n_hom_ref, n_het, n_hom_var, one_sided=False):
    """Exact Hardy-Weinberg mid-p test from three genotype counts."""
    import vail as hl
    from .expressions import Expression, _merge_sources

    args = [hl.int64(n_hom_ref), hl.int64(n_het), hl.int64(n_hom_var), hl.bool(one_sided)]
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    result = Expression(hl.tarray(hl.tfloat64), source, {
        'op': 'scalar_function', 'function': 'hardy_weinberg_counts',
        'args': [arg.to_ir() for arg in args],
    })
    return hl.bind(lambda x: hl.or_missing(hl.is_defined(x), hl.struct(het_freq_hwe=x[0], p_value=x[1])), result)


def _hwe_both(n_hom_ref, n_het, n_hom_var):
    """Share the exact-test distribution when QC needs both tail modes."""
    import vail as hl
    from .expressions import Expression, _merge_sources
    args = [hl.int64(n) for n in (n_hom_ref, n_het, n_hom_var)]
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    return Expression(hl.tstruct(het_freq_hwe=hl.tfloat64, p_value=hl.tfloat64,
                                 p_value_excess_het=hl.tfloat64), source, {
        'op': 'scalar_function', 'function': 'hwe_both_counts',
        'args': [arg.to_ir() for arg in args]})
