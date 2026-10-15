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


def test_all_dna_snp_pairs_padded_and_long_alleles():
    transitions = {('A','G'),('G','A'),('C','T'),('T','C')}
    pairs = [(ref, alt) for ref in 'ACGTNM' for alt in 'ACGTNM' if ref != alt]
    cases = [(prefix + ref + suffix, prefix + alt + suffix, (ref, alt) in transitions)
             for prefix, suffix in [('', ''), ('NM', 'MN'), ('A' * 4096, 'T' * 4096)]
             for ref,alt in pairs]
    table = hl.Table.parallelize([{'ref':ref,'alt':alt} for ref,alt,_ in cases])
    rows = table.select(kind=hl.allele_type(table.ref,table.alt),
                        snp=hl.is_snp(table.ref,table.alt),
                        transition=hl.is_transition(table.ref,table.alt),
                        transversion=hl.is_transversion(table.ref,table.alt)).collect()
    assert [(r.kind,r.snp,r.transition,r.transversion) for r in rows] == [
        ('SNP',True,transition,not transition) for _,_,transition in cases]


def test_all_classification_predicates_and_half_missing_inputs():
    cases = [('A','G',1),('AC','GT',2),('A','AT',3),('AT','A',4),
             ('AC','GTA',5),('A','*',6),('A','<DEL>',7),('A','H',0),
             (None,'G',None),('A',None,None),('',None,0)]
    table = hl.Table.parallelize([{'ref':ref,'alt':alt} for ref,alt,_ in cases],
                                 schema=hl.tstruct(ref=hl.tstr,alt=hl.tstr))
    functions = [hl.is_snp,hl.is_mnp,hl.is_insertion,hl.is_deletion,hl.is_complex,hl.is_star,hl.is_symbolic]
    result = table.select(code=hl.numeric_allele_type(table.ref,table.alt),
                          **{f'p{i}':fn(table.ref,table.alt) for i,fn in enumerate(functions,1)}).collect()
    for row,(_,_,expected) in zip(result,cases):
        assert row.code == expected
        assert [row[f'p{i}'] for i in range(1,8)] == [None if expected is None else expected==i for i in range(1,8)]
    empty = hl.Table.parallelize([],schema=hl.tstruct(ref=hl.tstr,alt=hl.tstr))
    assert empty.select(code=hl.numeric_allele_type(empty.ref,empty.alt),
                        transition=hl.is_transition(empty.ref,empty.alt)).collect()==[]
