# Adapted from Hail under MIT; see ../third_party/HAIL_LICENSE.
from __future__ import annotations
import vail as hl
import logging
from vail import Table, MatrixTable
from .variant_dataset import VariantDataset


def _densify(n, entries):
    entries = hl.or_else(
        entries, hl.range(n).map(lambda _: hl.missing(entries.dtype.element_type))
    )
    # Ordered exclusive scan: one previous entry per sample, not all previous rows.
    return hl.scan.array_agg(lambda entry: hl.scan.prev_nonnull(entry), entries)


def to_dense_mt(vds: "VariantDataset") -> "MatrixTable":
    """Creates a single, dense :class:`.MatrixTable` from the split
    :class:`.VariantDataset` representation.

    Parameters
    ----------
    vds : :class:`.VariantDataset`
        Dataset in VariantDataset representation.

    Returns
    -------
    :class:`.MatrixTable`
        Dataset in dense MatrixTable representation.
    """
    ref = VariantDataset._add_len(vds.reference_data)
    ref = ref.annotate_rows(_locus_global_pos=ref.locus.global_position())
    ref = ref.transmute_entries(_END_GLOBAL=ref["_locus_global_pos"] + ref.LEN - 1)
    to_drop = ("alleles", "rsid", "ref_allele", "_locus_global_pos")
    ref = ref.drop(*(x for x in to_drop if x in ref.row))
    var = vds.variant_data
    refl = ref.localize_entries("_ref_entries")
    varl = var.localize_entries("_var_entries", "_var_cols")
    varl = varl.annotate(_variant_defined=True)
    joined = varl.key_by("locus").join(refl, how="outer")
    dr = joined.annotate(
        dense_ref=hl.or_missing(
            joined["_variant_defined"],
            _densify(hl.len(joined["_var_cols"]), joined["_ref_entries"]),
        )
    )
    dr = dr.filter(dr["_variant_defined"])

    def coalesce_join(ref, var):
        reference_defined = hl.is_defined(ref)
        call_field = "GT" if "GT" in var else "LGT"
        assert call_field in var, var.dtype
        if call_field not in ref:
            ref_call_field = "GT" if "GT" in ref else "LGT"
            if ref_call_field not in ref:
                ref = hl.or_missing(
                    reference_defined, ref.annotate(**{call_field: hl.call(0, 0)})
                )
            else:
                ref = hl.or_missing(
                    reference_defined, ref.annotate(**{call_field: ref[ref_call_field]})
                )
        ref_set, var_set = (set(ref.dtype), set(var.dtype))
        shared_fields = [f for f in var.dtype if f in ref_set]
        var_fields = [f for f in var.dtype if f not in ref_set]
        return hl.if_else(
            hl.is_defined(var),
            var.select(*shared_fields, *var_fields),
            ref.select(
                *shared_fields, **{f: hl.missing(var[f].dtype) for f in var_fields}
            ),
        )

    dr = dr.annotate(
        _dense=hl.rbind(
            dr["_ref_entries"],
            lambda refs_at_this_row: hl.enumerate(
                hl.zip(dr["_var_entries"], dr.dense_ref)
            ).map(
                lambda tup: coalesce_join(
                    hl.coalesce(
                        refs_at_this_row[tup[0]],
                        hl.or_missing(
                            tup[1][1]["_END_GLOBAL"] >= dr.locus.global_position(),
                            tup[1][1],
                        ),
                    ),
                    tup[1][0],
                )
            ),
        )
    )
    dr = dr.key_by("locus", "alleles")
    fields_to_drop = ["_var_entries", "_ref_entries", "dense_ref", "_variant_defined"]
    if hl.vds.VariantDataset.ref_block_max_length_field in dr.globals:
        fields_to_drop.append(hl.vds.VariantDataset.ref_block_max_length_field)
    if "ref_allele" in dr.row:
        fields_to_drop.append("ref_allele")
    dr = dr.drop(*fields_to_drop)
    return dr._unlocalize_entries("_dense", "_var_cols", list(var.col_key))


def to_merged_sparse_mt(
    vds: "VariantDataset", *, ref_allele_function=None
) -> "MatrixTable":
    """Creates a single, merged sparse :class:`.MatrixTable` from the split
    :class:`.VariantDataset` representation.

    Parameters
    ----------
    vds : :class:`.VariantDataset`
        Dataset in VariantDataset representation.

    Returns
    -------
    :class:`.MatrixTable`
        Dataset in the merged sparse MatrixTable representation.
    """
    rht = vds.reference_data.localize_entries("_ref_entries", "_ref_cols")
    vht = vds.variant_data.localize_entries("_var_entries", "_var_cols")
    vht = vht.key_by("locus")
    merged_schema = {}
    for e in vds.reference_data.entry:
        merged_schema[e] = vds.reference_data[e].dtype
    for e in vds.variant_data.entry:
        if e in merged_schema:
            if not merged_schema[e] == vds.variant_data[e].dtype:
                raise TypeError(
                    f"cannot unify field {e!r}: {merged_schema[e]}, {vds.variant_data[e].dtype}"
                )
        else:
            merged_schema[e] = vds.variant_data[e].dtype
    ht = vht.join(rht, how="outer").drop("_ref_cols")

    def merge_arrays(r_array, v_array):

        def rewrite_ref(r):
            ref_block_selector = {}
            for k, t in merged_schema.items():
                if k == "LA":
                    ref_block_selector[k] = hl.literal([0])
                elif k in ("LGT", "GT") and k not in r:
                    ref_block_selector[k] = hl.call(0, 0)
                else:
                    ref_block_selector[k] = r[k] if k in r else hl.missing(t)
            return r.select(**ref_block_selector)

        def rewrite_var(v):
            return v.select(
                **{
                    k: v[k] if k in v else hl.missing(t)
                    for k, t in merged_schema.items()
                }
            )

        return (
            hl.case()
            .when(hl.is_missing(r_array), v_array.map(rewrite_var))
            .when(hl.is_missing(v_array), r_array.map(rewrite_ref))
            .default(
                hl.zip(r_array, v_array).map(
                    lambda t: hl.coalesce(rewrite_var(t[1]), rewrite_ref(t[0]))
                )
            )
        )

    if ref_allele_function is None:
        rg = ht.locus.dtype.reference_genome
        if "ref_allele" in ht.row:

            def ref_allele_function(ht):
                return ht.ref_allele
        elif rg.has_sequence():

            def ref_allele_function(ht):
                return ht.locus.sequence_context()

            logging.getLogger(__name__).info(
                "to_merged_sparse_mt: using locus sequence context to fill in reference alleles at monomorphic loci."
            )
        else:
            raise ValueError(
                "to_merged_sparse_mt: in order to construct a ref allele for reference-only sites, either pass a function to fill in reference alleles (e.g. ref_allele_function=lambda locus: hl.missing('str')) or add a sequence file with 'hl.get_reference(RG_NAME).add_sequence(FASTA_PATH)'."
            )
    ht = ht.select(
        alleles=hl.coalesce(ht["alleles"], hl.array([ref_allele_function(ht)])),
        **{k: ht[k] for k in vds.variant_data.row_value if k != "alleles"},
        _entries=merge_arrays(ht["_ref_entries"], ht["_var_entries"]),
    )
    ht = ht.key_by("locus", "alleles")
    return ht._unlocalize_entries(
        "_entries", "_var_cols", list(vds.variant_data.col_key)
    )


def filter_samples(
    vds: "VariantDataset",
    samples,
    *,
    keep: bool = True,
    remove_dead_alleles: bool = False,
) -> "VariantDataset":
    """Filter samples in a :class:`.VariantDataset`.

    Parameters
    ----------
    vds : :class:`.VariantDataset`
        Dataset in VariantDataset representation.
    samples : :class:`.Table` or list of str
        Samples to keep or remove.
    keep : :obj:`bool`
        Whether to keep (default), or filter out the samples from `samples_table`.
    remove_dead_alleles : :obj:`bool`
        If true, remove alleles observed in no samples. Alleles with AC == 0 will be
        removed, and LA values recalculated.

    Returns
    -------
    :class:`.VariantDataset`
    """
    if not isinstance(samples, hl.Table):
        samples = hl.Table.parallelize(
            hl.array(samples).map(lambda s: hl.struct(s=s)), key="s"
        )
    if not list((samples[x].dtype for x in samples.key)) == [hl.tstr]:
        raise TypeError(f"invalid key: {samples.key.dtype}")
    reference_data = (
        vds.reference_data.semi_join_cols(samples)
        if keep
        else vds.reference_data.anti_join_cols(samples)
    )
    reference_data = reference_data.filter_rows(hl.agg.count() > 0)
    variant_data = (
        vds.variant_data.semi_join_cols(samples)
        if keep
        else vds.variant_data.anti_join_cols(samples)
    )
    if remove_dead_alleles:
        vd = variant_data
        vd = vd.annotate_rows(
            __allele_counts=hl.agg.explode(lambda x: hl.agg.counter(x), vd.LA),
            __n=hl.agg.count(),
        )
        vd = vd.filter_rows(vd["__n"] > 0)
        vd = vd.drop("__n")
        vd = vd.annotate_rows(
            __kept_indices=hl.dict(
                hl.enumerate(
                    hl.range(hl.len(vd.alleles)).filter(
                        lambda idx: (idx == 0) | (vd["__allele_counts"].get(idx, 0) > 0)
                    ),
                    index_first=False,
                )
            )
        )
        vd = vd.annotate_rows(
            __old_to_new_LA=hl.range(hl.len(vd.alleles)).map(
                lambda idx: vd["__kept_indices"].get(idx, -1)
            )
        )

        def new_la_index(old_idx):
            raw_idx = vd["__old_to_new_LA"][old_idx]
            return (
                hl.case()
                .when(raw_idx >= 0, raw_idx)
                .or_error(
                    "'filter_samples': unexpected local allele: old index="
                    + hl.str(old_idx)
                )
            )

        vd = vd.annotate_entries(LA=vd.LA.map(lambda la: new_la_index(la)))
        vd = vd.key_rows_by("locus")
        vd = vd.annotate_rows(
            alleles=vd["__kept_indices"].keys().map(lambda i: vd.alleles[i])
        )
        vd = vd.key_rows_by("locus", "alleles")
        vd = vd.drop("__allele_counts", "__kept_indices", "__old_to_new_LA")
        return VariantDataset(reference_data, vd)
    variant_data = variant_data.filter_rows(hl.agg.count() > 0)
    return VariantDataset(reference_data, variant_data)


def filter_variants(
    vds: "VariantDataset", variants_table: "Table", *, keep: bool = True
) -> "VariantDataset":
    """Filter variants in a :class:`.VariantDataset`, without removing reference
    data.

    Parameters
    ----------
    vds : :class:`.VariantDataset`
        Dataset in VariantDataset representation.
    variants_table : :class:`.Table`
        Variants to filter on.
    keep: :obj:`bool`
        Whether to keep (default), or filter out the variants from `variants_table`.

    Returns
    -------
    :class:`.VariantDataset`.
    """
    if keep:
        variant_data = vds.variant_data.semi_join_rows(variants_table)
    else:
        variant_data = vds.variant_data.anti_join_rows(variants_table)
    return VariantDataset(vds.reference_data, variant_data)


def filter_chromosomes(vds, *, keep=None, remove=None, keep_autosomes=False):
    if (keep is not None) + (remove is not None) + bool(keep_autosomes) != 1:
        raise ValueError(
            "filter_chromosomes requires exactly one of keep, remove, keep_autosomes"
        )
    rg = vds.reference_genome
    if keep is not None:
        contigs = [keep] if isinstance(keep, str) else list(keep)
    else:
        excluded = (
            set(rg.x_contigs + rg.y_contigs + rg.mt_contigs)
            if keep_autosomes
            else set([remove] if isinstance(remove, str) else remove)
        )
        contigs = [c for c in rg.contigs if c not in excluded]
    for c in contigs:
        rg.contig_length(c)
    intervals = [hl.parse_locus_interval(c, reference_genome=rg) for c in contigs]
    return VariantDataset(
        hl.filter_intervals(vds.reference_data, intervals),
        hl.filter_intervals(vds.variant_data, intervals),
    )
