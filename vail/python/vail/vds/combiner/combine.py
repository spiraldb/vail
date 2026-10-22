# Adapted from Hail under MIT; see ../../third_party/HAIL_LICENSE.
from __future__ import annotations
import vail as hl
from vail import MatrixTable
from typing import Set, List, Collection, Optional, Union
from vail.allele_functions import numeric_allele_type
from ..variant_dataset import VariantDataset


class AlleleType:
    SNP, MNP, INSERTION, DELETION, COMPLEX = (1, 2, 3, 4, 5)


def get_lgt(gt, n_alleles, has_non_ref, row):
    index = hl.call(gt[0], gt[1]).unphased_diploid_gt_index()
    n_no_nonref = n_alleles - hl.int(has_non_ref)
    triangle_without_nonref = n_no_nonref * (n_no_nonref + 1) // 2
    return (
        hl.case()
        .when(gt.is_haploid(), hl.or_missing(gt[0] < n_no_nonref, gt))
        .when(index < triangle_without_nonref, gt)
        .when(index < n_alleles * (n_alleles + 1) // 2, hl.missing("call"))
        .or_error("invalid call " + hl.str(gt) + " at site " + hl.str(row.locus))
    )


def make_var_entry_struct(e, info_to_keep, alleles_len, has_non_ref, save_filters, row):
    handled_fields = dict()
    handled_names = {
        "LA",
        "gvcf_info",
        "LAD",
        "AD",
        "LGT",
        "GT",
        "LPL",
        "PL",
        "LPGT",
        "PGT",
    }
    if "GT" not in e:
        raise hl.utils.FatalError(
            "the Hail VDS combiner expects input GVCFs to have a 'GT' field in FORMAT."
        )
    handled_fields["LA"] = hl.range(0, alleles_len - hl.if_else(has_non_ref, 1, 0))
    handled_fields["LGT"] = get_lgt(e.GT, alleles_len, has_non_ref, row)
    if "AD" in e:
        handled_fields["LAD"] = hl.if_else(has_non_ref, e.AD[:-1], e.AD)
    if "PGT" in e:
        handled_fields["LPGT"] = (
            e.PGT
            if e.PGT.dtype != hl.tcall
            else get_lgt(e.PGT, alleles_len, has_non_ref, row)
        )
    if "PL" in e:
        handled_fields["LPL"] = hl.if_else(
            has_non_ref,
            hl.if_else(
                alleles_len > 2,
                hl.if_else(e.GT.is_haploid(), e.PL[:-1], e.PL[:-alleles_len]),
                hl.missing(e.PL.dtype),
            ),
            hl.if_else(alleles_len > 1, e.PL, hl.missing(e.PL.dtype)),
        )
        handled_fields["RGQ"] = hl.if_else(
            has_non_ref,
            hl.if_else(
                e.GT.is_haploid(),
                e.PL[alleles_len - 1],
                e.PL[hl.call(0, alleles_len - 1).unphased_diploid_gt_index()],
            ),
            hl.missing(e.PL.dtype.element_type),
        )
    handled_fields["gvcf_info"] = (
        hl.case()
        .when(
            hl.is_missing(row.info.END),
            parse_allele_specific_fields(row.info.select(*info_to_keep), has_non_ref),
        )
        .or_missing()
    )
    pass_through_fields = {k: v for k, v in e.items() if k not in handled_names}
    filters = {"gvcf_filters": row.filters} if save_filters else {}
    return hl.struct(**handled_fields, **pass_through_fields, **filters)


def make_ref_entry_struct(e, entry_to_keep, save_filters, row):
    handled_fields = {}
    handled_names = {"AD", "GT", "PGT", "PL"}
    if "GT" in entry_to_keep:
        handled_fields["LGT"] = e["GT"]
    if "PGT" in entry_to_keep:
        handled_fields["LPGT"] = e["PGT"]
    if "AD" in entry_to_keep:
        handled_fields["LAD"] = e["AD"][:1]
    if "PL" in entry_to_keep:
        handled_fields["LPL"] = e["PL"][:1]
    reference_fields = {
        k: v for k, v in e.items() if k in entry_to_keep and k not in handled_names
    }
    filters = {"gvcf_filters": row.filters} if save_filters else {}
    return (
        hl.case()
        .when(
            hl.coalesce(e.GT.is_hom_ref(), True),
            hl.struct(
                **reference_fields,
                **handled_fields,
                LEN=row.info.END - row.locus.position + 1,
                **filters,
            ),
        )
        .or_error(
            "found reference block with non reference-genotype at" + hl.str(row.locus)
        )
    )


def defined_entry_fields(mt: MatrixTable, sample=None) -> Set[str]:
    if sample is not None:
        mt = mt.head(sample)
    used = mt.aggregate_entries(
        hl.struct(**{k: hl.agg.any(hl.is_defined(v)) for k, v in mt.entry.items()})
    )
    return set((k for k in mt.entry if used[k]))


def parse_allele_specific_ints(string, has_non_ref):
    ints = string.split("\\|")
    ints = hl.if_else(has_non_ref, ints[:-1], ints)
    return ints.map(
        lambda i: hl.if_else(
            (hl.len(i) == 0) | (i == "."), hl.missing(hl.tint32), hl.int32(i)
        )
    )


def parse_allele_specific_doubles(string, has_non_ref):
    ints = string.split("\\|")
    ints = hl.if_else(has_non_ref, ints[:-1], ints)
    return ints.map(
        lambda i: hl.if_else(
            (hl.len(i) == 0) | (i == "."), hl.missing(hl.tfloat64), hl.float64(i)
        )
    )


def parse_allele_specific_sb_table(string, has_non_ref):
    ints = string.split("\\|")
    ints = hl.if_else(has_non_ref, ints[:-1], ints)
    return ints.map(lambda xs: xs.split(",").map(hl.int32))


def parse_allele_specific_ranksum(string, has_non_ref):
    typ = hl.ttuple(hl.tfloat64, hl.tint32)
    items = string.split("\\|")
    items = hl.if_else(has_non_ref, items[:-1], items)
    return items.map(
        lambda s: hl.if_else(
            (hl.len(s) == 0) | (s == "."),
            hl.missing(typ),
            hl.rbind(
                s.split(","),
                lambda ss: hl.if_else(
                    hl.len(ss) != 2,
                    hl.missing(hl.ttuple(hl.tfloat64, hl.tint32)),
                    hl.tuple([hl.float64(ss[0]), hl.int32(ss[1])]),
                ),
            ),
        )
    )


def parse_allele_specific_fields(
    info: hl.Expression, has_non_ref: Union[bool, hl.Expression]
) -> hl.Expression:

    def parse_field(field: str) -> hl.Expression:
        if parse := _allele_specific_field_parsers.get(field):
            return parse(info[field], has_non_ref)
        return info[field]

    return hl.struct(**{f: parse_field(f) for f in info})


def merge_alleles(alleles):
    return hl.rbind(
        alleles.map(lambda a: hl.or_else(a[0], "")).fold(
            lambda s, t: hl.if_else(hl.len(s) > hl.len(t), s, t), ""
        ),
        lambda ref: hl.rbind(
            alleles.map(
                lambda al: hl.rbind(
                    al[0],
                    lambda r: hl.array([ref]).extend(
                        al[1:].map(
                            lambda a: hl.rbind(
                                numeric_allele_type(r, a),
                                lambda at: hl.if_else(
                                    (at == AlleleType.SNP)
                                    | (at == AlleleType.INSERTION)
                                    | (at == AlleleType.DELETION)
                                    | (at == AlleleType.MNP)
                                    | (at == AlleleType.COMPLEX),
                                    a + ref[hl.len(r) :],
                                    a,
                                ),
                            )
                        )
                    ),
                )
            ),
            lambda lal: hl.struct(
                globl=hl.array([ref]).extend(
                    hl.array(hl.set(hl.flatten(lal)).remove(ref))
                ),
                local=lal,
            ),
        ),
    )


def combine_references(mts: List[MatrixTable]) -> MatrixTable:
    fd = hl.vds.VariantDataset.ref_block_max_length_field
    n_with_ref_max_len = len([mt for mt in mts if fd in mt.globals])
    any_ref_max = n_with_ref_max_len > 0
    all_ref_max = n_with_ref_max_len == len(mts)
    if any_ref_max and (not all_ref_max):
        mts = [mt.drop(fd) if fd in mt.globals else mt for mt in mts]
    mts = [mt.drop("ref_allele") if "ref_allele" in mt.row else mt for mt in mts]
    ts = hl.Table.multi_way_zip_join([localize(mt) for mt in mts], "data", "g")
    combined = combine_r(ts, fd if all_ref_max else None)
    return unlocalize(combined)


def combine_variant_datasets(vdss: List[VariantDataset]) -> VariantDataset:
    reference = combine_references([vds.reference_data for vds in vdss])
    no_variant_key = [vds.variant_data.key_rows_by("locus") for vds in vdss]
    variants = combine_gvcfs(no_variant_key)
    return VariantDataset(reference, variants.key_rows_by("locus", "alleles"))


def transform_gvcf(
    mt: MatrixTable,
    reference_entry_fields_to_keep: Collection[str],
    info_to_keep: Optional[Collection[str]] = None,
    save_filters: bool = False,
) -> VariantDataset:
    """Transforms a GVCF into a single sample VariantDataSet

    The input to this should be some result of :func:`.import_vcf`
    ``array_elements_required=False``.

    There is an assumption that this function will be called on a matrix table
    with one column (or a localized table version of the same).

    Parameters
    ----------
    mt : :class:`.MatrixTable`
        The GVCF being transformed.
    reference_entry_fields_to_keep : :class:`list` of :class:`str`
        Genotype fields to keep in the reference table.
    info_to_keep : :class:`list` of :class:`str`
        Any ``INFO`` fields in the GVCF that are to be kept and put in the ``gvcf_info`` entry
        field. By default, all ``INFO`` fields except ``END`` and ``DP`` are kept.
    save_filters : :obj:`bool`
        preserve the ``FILTER`` field of the gvcf as ``gvcf_filters`` on its entries.

    Returns
    -------
    :obj:`.VariantDataset`
        A single sample variant dataset

    Notes
    -----
    This function will parse the following allele specific annotations from
    pipe delimited strings into proper values. ::

        AS_QUALapprox
        AS_RAW_MQ
        AS_RAW_MQRankSum
        AS_RAW_ReadPosRankSum
        AS_SB_TABLE
        AS_VarDP

    """
    ref_mt = make_reference_matrix_table(
        mt, reference_entry_fields_to_keep, save_filters
    )
    var_mt = make_variants_matrix_table(mt, info_to_keep, save_filters)
    return VariantDataset(ref_mt, var_mt.key_rows_by("locus", "alleles"))


_allele_specific_field_parsers = {
    "AS_QUALapprox": parse_allele_specific_ints,
    "AS_RAW_MQ": parse_allele_specific_doubles,
    "AS_RAW_MQRankSum": parse_allele_specific_ranksum,
    "AS_RAW_ReadPosRankSum": parse_allele_specific_ranksum,
    "AS_SB_TABLE": parse_allele_specific_sb_table,
    "AS_VarDP": parse_allele_specific_ints,
}


def _variant_row(row, info_to_keep, save_filters):
    return hl.rbind(
        hl.len(row.alleles),
        "<NON_REF>" == row.alleles[-1],
        lambda alleles_len, has_non_ref: hl.struct(
            locus=row.locus,
            alleles=hl.if_else(has_non_ref, row.alleles[:-1], row.alleles),
            **{"rsid": row.rsid} if "rsid" in row else {},
            __entries=row["__entries"].map(
                lambda e: make_var_entry_struct(
                    e, info_to_keep, alleles_len, has_non_ref, save_filters, row
                )
            ),
        ),
    )


def _reference_row(row, entry_to_keep, save_filters):
    return hl.struct(
        locus=row.locus,
        __entries=row["__entries"].map(
            lambda e: make_ref_entry_struct(e, entry_to_keep, save_filters, row)
        ),
    )


def combine_reference_row(row, gbl):
    return hl.struct(
        locus=row.locus,
        __entries=hl.range(0, hl.len(row.data)).flatmap(
            lambda i: hl.if_else(
                hl.is_missing(row.data[i]),
                hl.range(0, hl.len(gbl.g[i]["__cols"])).map(
                    lambda _: hl.missing(row.data[i]["__entries"].dtype.element_type)
                ),
                row.data[i]["__entries"],
            )
        ),
    )


def combine_variant_rows(row, gbl):

    def renumber_entry(entry, old_to_new) -> hl.Expression:
        return hl.or_missing(
            hl.is_defined(entry),
            entry.annotate(LA=entry.LA.map(lambda lak: old_to_new[lak])),
        )

    return hl.rbind(
        merge_alleles(row.data.map(lambda d: d.alleles)),
        lambda alleles: hl.struct(
            locus=row.locus,
            alleles=alleles.globl,
            **{"rsid": hl.find(hl.is_defined, row.data.map(lambda d: d.rsid))}
            if "rsid" in row.data.dtype.element_type
            else {},
            __entries=hl.bind(
                lambda combined_allele_index: hl.range(0, hl.len(row.data)).flatmap(
                    lambda i: hl.if_else(
                        hl.is_missing(row.data[i]["__entries"]),
                        hl.range(0, hl.len(gbl.g[i]["__cols"])).map(
                            lambda _: hl.missing(
                                row.data[i]["__entries"].dtype.element_type
                            )
                        ),
                        hl.bind(
                            lambda old_to_new: row.data[i]["__entries"].map(
                                lambda e: renumber_entry(e, old_to_new)
                            ),
                            hl.range(0, hl.len(alleles.local[i])).map(
                                lambda j: combined_allele_index[alleles.local[i][j]]
                            ),
                        ),
                    )
                ),
                hl.dict(
                    hl.range(0, hl.len(alleles.globl)).map(
                        lambda j: hl.tuple([alleles.globl[j], j])
                    )
                ),
            ),
        ),
    )


def localize(mt):
    return (
        mt.localize_entries("__entries", "__cols")
        if isinstance(mt, MatrixTable)
        else mt
    )


def unlocalize(table):
    return table._unlocalize_entries("__entries", "__cols", ["s"])


def make_variants_matrix_table(mt, info_to_keep=None, save_filters=False):
    if not info_to_keep:
        info_to_keep = [
            name for name in mt.info.dtype if name not in ("END", "LEN", "DP")
        ]
    mt = localize(mt)
    mt = mt.filter(hl.is_missing(mt.info.END))
    row = _variant_row(mt.row, info_to_keep, save_filters)
    return unlocalize(_select_row(mt, row))


def make_reference_matrix_table(mt, entry_to_keep, save_filters=False):
    mt = localize(mt).key_by("locus")
    mt = mt.filter(hl.is_defined(mt.info.END))
    return unlocalize(
        _select_row(mt, _reference_row(mt.row, entry_to_keep, save_filters))
    )


def combine_r(ts, ref_block_max_len_field):
    ts = _select_row(ts, combine_reference_row(ts.row, ts.index_globals()))
    fields = {"__cols": hl.flatten(ts.g.map(lambda g: g["__cols"]))}
    if ref_block_max_len_field is not None:
        fields[ref_block_max_len_field] = hl.max(
            ts.g.map(lambda g: g[ref_block_max_len_field])
        )
    return ts.transmute_globals(**fields)


def combine(ts):
    ts = _select_row(ts, combine_variant_rows(ts.row, ts.index_globals()))
    return ts.transmute_globals(__cols=hl.flatten(ts.g.map(lambda g: g["__cols"])))


def combine_gvcfs(mts):
    return unlocalize(
        combine(hl.Table.multi_way_zip_join([localize(mt) for mt in mts], "data", "g"))
    )


def _select_row(table, row):
    keys = list(table.key)
    field = "__vds_transformed_row"
    while field in table.row:
        field += "_"
    table = table.annotate(**{field: row}).key_by()
    return table.select(**table[field]).key_by(*keys)
