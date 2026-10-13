"""Allele classification using existing expression and string kernels.

Classification follows Hail's Python numeric_allele_type implementation,
Copyright (c) 2015-2023 Hail Authors, under the MIT license (see
third_party/HAIL_LICENSE). No general Unicode substring semantics are assumed:
substring comparisons below execute only for validated ASCII DNA alleles.
"""
from .expressions import array_sum, case, cast, if_else, literal
from .strings import _call, _text
from .types import tint32, tint64

_NAMES = ['Unknown', 'SNP', 'MNP', 'Insertion', 'Deletion', 'Complex', 'Star', 'Symbolic']


def _dna(value):
    return _call('regexp_like', value, literal(r'\A[ACGTNM]+\z'))


def _char(value, index):
    return _call('substr', value, cast(index + 1, tint64), literal(1, tint64))


def _tail(value):
    return _call('substr', value, literal(2, tint64), cast(value.length(), tint64))


def _mismatches(ref, alt):
    from . import range
    return array_sum(range(ref.length()).map(lambda i: cast(_char(ref, i) != _char(alt, i), tint32)))


def numeric_allele_type(ref, alt):
    """Classify an allele pair as Unknown=0, SNP=1, ..., Symbolic=7."""
    ref, alt = _text(ref), _text(alt)
    equal_length = if_else(
        ref.length() == 1,
        if_else(ref != alt, 1, 0),
        if_else(_mismatches(ref, alt) == 1, 1, 2),
    )
    dna_type = (case().when(ref.length() == alt.length(), equal_length)
                .when((ref.length() < alt.length()) & (_char(ref, literal(0)) == _char(alt, literal(0)))
                      & _call('ends_with', alt, _tail(ref)), 3)
                .when((_char(ref, literal(0)) == _char(alt, literal(0)))
                      & _call('ends_with', ref, _tail(alt)), 4)
                .default(5))
    alt_type = (case().when(_dna(alt), dna_type).when(alt == '*', 6)
                .when(_call('regexp_like', alt, literal(r'(^\.)|(\.$)|(^<)|(>$)|(\[)|(\])')), 7)
                .default(0))
    return if_else(_dna(ref), alt_type, 0)


def allele_type(ref, alt):
    """Return SNP, MNP, Insertion, Deletion, Complex, Star, Symbolic or Unknown."""
    return literal(_NAMES)[numeric_allele_type(ref, alt)]


def is_snp(ref, alt):
    """Whether exactly one nucleotide differs in equal-length DNA alleles."""
    return numeric_allele_type(ref, alt) == 1


def is_mnp(ref, alt):
    """Whether the pair is classified as a multi-nucleotide polymorphism."""
    return numeric_allele_type(ref, alt) == 2


def is_insertion(ref, alt):
    return numeric_allele_type(ref, alt) == 3


def is_deletion(ref, alt):
    return numeric_allele_type(ref, alt) == 4


def is_indel(ref, alt):
    code = numeric_allele_type(ref, alt)
    return (code == 3) | (code == 4)


def is_complex(ref, alt):
    return numeric_allele_type(ref, alt) == 5


def is_star(ref, alt):
    return numeric_allele_type(ref, alt) == 6


def is_symbolic(ref, alt):
    return numeric_allele_type(ref, alt) == 7


def _transition(ref, alt):
    from . import range
    def pair(i):
        r, a = _char(ref, i), _char(alt, i)
        return ((r == 'A') & (a == 'G')) | ((r == 'G') & (a == 'A')) | ((r == 'C') & (a == 'T')) | ((r == 'T') & (a == 'C'))
    return array_sum(range(ref.length()).map(lambda i: cast(pair(i), tint32))) > 0


def is_transition(ref, alt):
    """Whether the SNP changes A/G or C/T, including padded SNP alleles."""
    ref, alt = _text(ref), _text(alt)
    return if_else(is_snp(ref, alt), _transition(ref, alt), False)


def is_transversion(ref, alt):
    ref, alt = _text(ref), _text(alt)
    return if_else(is_snp(ref, alt), ~_transition(ref, alt), False)


def is_strand_ambiguous(ref, alt):
    """Whether the pair is A/T, T/A, C/G or G/C."""
    ref, alt = _text(ref), _text(alt)
    result = ((ref == 'A') & (alt == 'T')) | ((ref == 'T') & (alt == 'A')) | ((ref == 'C') & (alt == 'G')) | ((ref == 'G') & (alt == 'C'))
    return if_else(result, True, False, missing_false=True)
