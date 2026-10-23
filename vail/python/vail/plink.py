"""PLINK SNP-major BED reader.

Specifications read before implementation:
https://www.cog-genomics.org/plink/1.9/formats#bed (#bim and #fam)
https://hail.is/docs/0.2/methods/impex.html#hail.methods.import_plink
Genotype rows decode in a bounded native scan; FAM is column metadata.
"""

from .vcf import _local_path


def import_plink(
    bed,
    bim,
    fam,
    min_partitions=None,
    delimiter=r"\\s+",
    missing="NA",
    quant_pheno=False,
    a2_reference=True,
    reference_genome="default",
    contig_recoding=None,
    skip_invalid_loci=False,
    n_partitions=None,
    block_size=None,
):
    from . import Table, _validate_n
    from .fam import _import_fam
    from .types import tstruct, tarray, tstr, tint32, tint64, tfloat64, tcall, tlocus
    from .reference_genome import resolve_reference
    from .matrixtable import MatrixTable, MatrixIR, _CID, _COL, _ROW, _CELLS
    from .text import _read
    from .utils import FatalError

    for name, value in [
        ("min_partitions", min_partitions),
        ("n_partitions", n_partitions),
        ("block_size", block_size),
    ]:
        if value is not None:
            _validate_n(value, name, positive=True)
    if type(a2_reference) is not bool or type(skip_invalid_loci) is not bool:
        raise TypeError("a2_reference and skip_invalid_loci must be bool")
    reference = (
        None if reference_genome is None else resolve_reference(reference_genome)
    )
    recoding = contig_recoding
    if recoding is None:
        recoding = {}
        if reference is not None and reference.name == "GRCh37":
            recoding = {"23": "X", "24": "Y", "25": "X", "26": "MT"}
        elif reference is not None and reference.name == "GRCh38":
            recoding = {str(i): "chr" + str(i) for i in range(1, 23)}
            recoding.update({"23": "chrX", "24": "chrY", "25": "chrX", "26": "chrM"})
    sample_table = _import_fam(fam, quant_pheno, delimiter, missing)
    samples = sample_table.collect()
    ids = [s.id for s in samples]
    if len(set(ids)) != len(ids):
        raise FatalError("PLINK individual IDs must be unique")
    bim = _local_path(bim)
    with open(bim) as f:
        if not f.read(1):
            raise FatalError("BIM file does not contain any variants")
    locus_type = (
        tstruct(contig=tstr, position=tint32)
        if reference is None
        else tlocus(reference.name)
    )
    row = tstruct(
        locus=locus_type, alleles=tarray(tstr), rsid=tstr, cm_position=tfloat64
    )
    entry = tstruct(GT=tcall)
    schema = tstruct(
        **{_ROW: row, _CELLS: tarray(tstruct(**{"value": entry}))}
    )
    options = dict(
        bim=bim,
        n_samples=len(samples),
        a2_reference=a2_reference,
        contig_recoding=recoding,
        lengths=None if reference is None else reference.lengths,
        skip_invalid_loci=skip_invalid_loci,
        contig_ordinals=None
        if reference is None
        else {c: i for i, c in enumerate(reference.contigs)},
    )
    core = _read([_local_path(bed)], schema, "plink", options)
    col_fields = dict(sample_table._schema.items())
    col_fields.pop("id")
    col = tstruct(s=tstr, **col_fields)
    cols = Table.parallelize(
        [
            {_CID: i, _COL: dict(s=s.id, **{k: s[k] for k in col_fields})}
            for i, s in enumerate(samples)
        ],
        schema=tstruct(**{_CID: tint64, _COL: col}),
    ).key_by()
    mt = MatrixTable(
        MatrixIR(
            core, cols, row_key=("locus", "alleles"), col_key=("s",), _aligned=True
        )
    )
    mt = mt._copy(rows=mt._sort_rows(mt._mir.core, mt._mir.row_key))
    partitions = n_partitions or min_partitions
    if partitions is not None:
        mt = mt.repartition(partitions)
    return mt


def export_plink(
    dataset,
    output,
    call=None,
    fam_id=None,
    ind_id=None,
    pat_id=None,
    mat_id=None,
    is_female=None,
    pheno=None,
    varid=None,
    cm_position=None,
):
    """Stream typed Arrow calls directly to native BED/BIM output."""
    import re
    from . import eval as evaluate
    from . import literal, or_else, str as string, if_else, delimit, float64
    from .expressions import Expression

    def rebind(e, mt):
        return Expression(e.dtype, mt, e.to_ir()) if e.source is not None else e

    from .types import tbool, tcall, tstr, tarray, tstruct, tint32, tlocus
    from . import MatrixTable, missing, case, len as length

    if not isinstance(dataset, MatrixTable):
        raise TypeError("export_plink requires a MatrixTable")
    if (
        "locus" not in dataset.row.dtype
        or dataset.row.dtype.get("alleles") != tarray(tstr)
        or not (
            isinstance(dataset.locus.dtype, tlocus)
            or dataset.locus.dtype == tstruct(contig=tstr, position=tint32)
        )
    ):
        raise TypeError("export_plink requires locus and alleles row fields")
    if ind_id is None and (
        len(dataset.col_key) != 1 or next(iter(dataset.col_key.dtype.values())) != tstr
    ):
        raise TypeError("export_plink requires one string column key")
    output = _local_path(output)

    def expression(value):
        from .expressions import Expression

        return value if isinstance(value, Expression) else literal(value)

    def text(value, default):
        return (
            literal(default)
            if value is None
            else or_else(string(expression(value)), default)
        )

    gt = (
        (dataset.GT if dataset.entry.dtype.get("GT") == tcall else missing(tcall))
        if call is None
        else expression(call)
    )
    if gt.dtype != tcall:
        raise TypeError("PLINK call must have type call")
    iid = dataset[next(iter(dataset.col_key))] if ind_id is None else expression(ind_id)
    sex = (
        literal("0")
        if is_female is None
        else or_else(if_else(expression(is_female), "2", "1"), "0")
    )
    phenotype = (
        literal("NA")
        if pheno is None
        else (
            or_else(if_else(expression(pheno), "2", "1"), "NA")
            if expression(pheno).dtype == tbool
            else expression(pheno)
        )
    )
    cols = dataset.select_cols(
        _plink_fid=text(fam_id, "0"),
        _plink_iid=string(iid),
        _plink_pat=text(pat_id, "0"),
        _plink_mat=text(mat_id, "0"),
        _plink_sex=sex,
        _plink_pheno=phenotype,
    )
    # Localization guarantees matching physical order of FAM metadata and BED calls.
    mt = cols.select_entries(_plink_gt=rebind(gt, cols))
    default_id = delimit(
        [
            dataset.locus.contig,
            string(dataset.locus.position),
            dataset.alleles[0],
            dataset.alleles[1],
        ],
        ":",
    )
    row_values = dict(
        _plink_contig=dataset.locus.contig,
        _plink_pos=dataset.locus.position,
        _plink_alleles=case()
        .when(length(dataset.alleles) == 2, dataset.alleles)
        .or_error("PLINK requires biallelic variants"),
        _plink_varid=default_id if varid is None else text(varid, ""),
        _plink_cm=literal(0.0)
        if cm_position is None
        else or_else(float64(expression(cm_position)), 0.0),
    )
    mt = mt.select_rows(**{k: rebind(v, mt) for k, v in row_values.items()})
    localized = mt.localize_entries("_plink_entries", "_plink_columns")
    columns = evaluate(localized.index_globals()._plink_columns)
    with open(output + ".fam", "w") as f:
        for c in columns:
            fields = [
                c[k]
                for k in (
                    "_plink_fid",
                    "_plink_iid",
                    "_plink_pat",
                    "_plink_mat",
                    "_plink_sex",
                    "_plink_pheno",
                )
            ]
            fields = [
                str(x) if x is not None else ("NA" if i == 5 else None)
                for i, x in enumerate(fields)
            ]
            if any(x is None or re.search(r"\s", x) for x in fields):
                raise TypeError("has spaces in the following values: " + repr(fields))
            f.write("\t".join(fields) + "\n")
    t = localized.key_by()
    t = t.select(
        contig=t._plink_contig,
        varid=t._plink_varid,
        cm=t._plink_cm,
        position=t._plink_pos,
        a1=t._plink_alleles[1],
        a2=t._plink_alleles[0],
        entries=t._plink_entries,
    )
    from .utils import FatalError

    try:
        t._native.export_plink(output, len(columns))
    except ValueError as error:
        raise FatalError(str(error)) from error
