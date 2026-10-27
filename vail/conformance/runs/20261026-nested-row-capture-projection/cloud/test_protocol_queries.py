"""Tiny public API/output proof; no benchmark or private execution hook."""
import vail as h
from queries import construct
from oracle import independent_reference

def test_queries_and_independent_row_reference_on_missing_filtered_entries():
    m=h.range_matrix_table(2,3)
    m=m.annotate_rows(locus=h.locus('20',m.row_idx+1),alleles=h.array(['A','C']))
    m=m.annotate_entries(GQ=h.or_missing((m.row_idx!=0)|(m.col_idx!=1),m.row_idx*10+m.col_idx))
    m=m.filter_entries((m.row_idx!=1)|(m.col_idx!=2))
    expected={'projected':dict(total=37,present=4),'scalar':dict(total=37,present=4),
              'whole_full':dict(total=37,present=4,payload=105,small=17)}
    actual,entries,rows=independent_reference(m)
    assert actual==expected and entries==5 and rows==2
    for case in expected:assert construct(m,case)()==expected[case]
