import pytest

import vail as hl


@pytest.mark.parametrize('ref,alt,kind,transition,transversion', [
    ('A', 'G', 'SNP', True, False),
    ('C', 'T', 'SNP', True, False),
    ('AA', 'AG', 'SNP', True, False),
    ('ACA', 'AGA', 'SNP', False, True),
    ('AA', 'AT', 'SNP', False, True),
    ('AN', 'AM', 'SNP', False, True),
    ('AC', 'GT', 'MNP', False, False),
    ('AA', 'AA', 'MNP', False, False),
    ('A', 'A', 'Unknown', False, False),
    ('ATT', 'ATGCTT', 'Insertion', False, False),
    ('GTGTA', 'GTA', 'Deletion', False, False),
    ('A', 'TATGC', 'Complex', False, False),
    ('A', '*', 'Star', False, False),
    ('C', '<DEL>', 'Symbolic', False, False),
    ('A', '[chr1:3[A', 'Symbolic', False, False),
    ('A', 'T.', 'Symbolic', False, False),
    ('A', 'H', 'Unknown', False, False),
    ('', 'C', 'Unknown', False, False),
    ('a', 'T', 'Unknown', False, False),
    ('🙂', '*', 'Unknown', False, False),
])
def test_allele_classification(ref, alt, kind, transition, transversion):
    result = hl.eval(hl.struct(
        kind=hl.allele_type(ref, alt), transition=hl.is_transition(ref, alt),
        transversion=hl.is_transversion(ref, alt), indel=hl.is_indel(ref, alt),
        symbolic=hl.is_symbolic(ref, alt),
    ))
    assert result.kind == kind
    assert result.transition == transition
    assert result.transversion == transversion
    assert result.indel == (kind in ('Insertion', 'Deletion'))
    assert result.symbolic == (kind == 'Symbolic')


def test_allele_functions_missingness_row_execution_and_validation():
    table = hl.Table.parallelize([
        {'ref': 'A', 'alt': 'G'}, {'ref': 'ATT', 'alt': 'ATGCTT'},
        {'ref': None, 'alt': 'T'}, {'ref': 'A', 'alt': None},
        {'ref': '', 'alt': None},
    ], schema=hl.tstruct(ref=hl.tstr, alt=hl.tstr))
    assert hl.allele_type(table.ref, table.alt).collect() == ['SNP', 'Insertion', None, None, 'Unknown']
    assert hl.is_transition(table.ref, table.alt).collect() == [True, False, None, None, False]
    assert hl.is_strand_ambiguous(table.ref, table.alt).collect() == [False] * 5
    assert hl.eval(hl.is_strand_ambiguous('A', 'T')) is True
    with pytest.raises(TypeError, match='str arguments'):
        hl.is_snp(1, 'T')
    other = hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.is_snp(table.ref, hl.str(other.idx))
