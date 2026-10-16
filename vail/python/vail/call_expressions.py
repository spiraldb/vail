"""Call predicates lowered to DataFusion arithmetic on the packed call word."""
from .expressions import bind, float64, if_else, int32, int64


def call_property(word, name):
    # The word accessor validates the Call. Bind it once so repeated arithmetic
    # operands do not decode the same genotype again for every occurrence.
    return bind(lambda value: _call_property(value, name), word)


def _call_property(word, name):
    ploidy = int32((int64(word) // 2) % 4)
    if name == "ploidy":
        return ploidy
    if name == "is_haploid":
        return ploidy == 1
    if name == "is_diploid":
        return ploidy == 2
    unsigned = if_else(word < 0, int64(word) + 2**32, int64(word))
    representation = unsigned // 8
    if name == "is_hom_ref":
        return (ploidy > 0) & (representation == 0)
    if name == "is_non_ref":
        return (ploidy > 0) & (representation > 0)
    # Invert the triangular genotype index using the same arithmetic as the
    # native Call getters. All intermediate integers are exact in float64.
    k = int64(((float64(8 * representation + 1) ** 0.5) - 1) / 2)
    first = representation - k * (k + 1) // 2
    second = if_else(unsigned % 2 == 1, k - first, k)
    heterozygous = (ploidy == 2) & (first != second)
    if name == "is_het":
        return heterozygous
    if name == "is_het_ref":
        return heterozygous & ((first == 0) | (second == 0))
    if name == "is_het_non_ref":
        return heterozygous & (first > 0) & (second > 0)
    if name == "is_hom_var":
        return ((ploidy == 1) & (representation > 0)) | (
            (ploidy == 2) & (representation > 0) & (first == second)
        )
    if name == "n_alt_alleles":
        return if_else(ploidy == 0, 0, if_else(
            ploidy == 1, int32(representation > 0), int32(first > 0) + int32(second > 0),
        ))
    raise ValueError(f"unknown Call property {name!r}")
