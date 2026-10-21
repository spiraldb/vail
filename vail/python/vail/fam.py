"""PLINK FAM import expressed as a lazy native text scan.

Contract: https://www.cog-genomics.org/plink/1.9/formats#fam and Hail's
LoadPlink.parseFam (pinned upstream) define the six fields and missing codes.
"""
from .expressions import case, float64, missing as missing_expr, or_missing
from .text import import_lines, _split_line
from .types import tbool, tfloat64


_NUMERIC = r'^-?(?:[0-9]+|[0-9]*\.[0-9]+)(?:[eE]-?[0-9]+)?$'


def import_fam(path, quant_pheno=False, delimiter=r'\\s+', missing='NA'):
    return _import_fam(path, quant_pheno, delimiter, missing, _key=True)


def _import_fam(path, quant_pheno=False, delimiter=r'\\s+', missing='NA', *, _key=False):
    """Import six-column PLINK sample records, keyed by sample ``id``.

    Case/control phenotypes use 1/2; quantitative phenotypes preserve -9
    unless it is explicitly selected as the missing marker.
    """
    if type(quant_pheno) is not bool:
        raise TypeError('quant_pheno must be bool')
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError('delimiter must be a nonempty string')
    if not isinstance(missing, str):
        raise TypeError('missing must be a string')
    # Hail accepts Java-escaped delimiters (the public default is \\s+).
    delimiter = delimiter.replace('\\\\', '\\').replace(r'\t', '\t')
    table = import_lines(path)
    # Match Hail's empty-input error using only a bounded native probe.
    # The records themselves remain in the native scan for lazy decoding.
    import pyarrow as pa
    if pa.table(table._native.sample_arrow(1)).num_rows == 0:
        from .utils import FatalError
        raise FatalError('Empty FAM file')
    table = table.annotate(_fam_fields=_split_line(table.text, delimiter, None, [], 6))
    fields = table._fam_fields
    sex = fields[4]
    female = (case().when((sex == missing) | (sex == '-9') | (sex == '0'), missing_expr(tbool))
              .when(sex == '1', False).when(sex == '2', True)
              .or_error('Invalid sex: ' + sex))
    pheno = fields[5]
    if quant_pheno:
        phenotype = (case().when(pheno == missing, missing_expr(tfloat64))
                     .when(pheno.matches(_NUMERIC), float64(pheno))
                     .or_error('Invalid quantitative phenotype: ' + pheno))
        phenotype_name = 'quant_pheno'
    else:
        phenotype = (case().when((pheno == missing) | (pheno == '0') | (pheno == '-9'), missing_expr(tbool))
                     .when(pheno == '1', False).when(pheno == '2', True)
                     .when(~pheno.matches(_NUMERIC), missing_expr(tbool))
                     .or_error('Invalid case-control phenotype: ' + pheno))
        phenotype_name = 'is_case'
    result = table.select(id=fields[1], fam_id=or_missing(fields[0] != '0', fields[0]),
                        pat_id=or_missing(fields[2] != '0', fields[2]),
                        mat_id=or_missing(fields[3] != '0', fields[3]),
                        is_female=female, **{phenotype_name: phenotype})
    return result.key_by('id') if _key else result

