"""Aligned regression retains native QR math for shared and entry predictors."""
import pytest
import vail as hl


@pytest.mark.parametrize('partitions',[1,3])
def test_coordinate_linreg_shared_entry_dependent_predictors_and_filter_masks(partitions):
    t=hl.utils.range_table(40,n_partitions=partitions)
    fits=t.aggregate(hl.agg.array_agg(lambda i:hl.struct(
        shared=hl.agg.linreg(4.+2.5*t.idx+i,[1.,t.idx]),
        entry=hl.agg.linreg(4.+2.5*(t.idx+i),[1.,t.idx+i]),
        masked=hl.agg.filter((t.idx+i)%3!=0,hl.agg.linreg(4.+2.5*(t.idx+i),[1.,t.idx+i])),
        missing=hl.agg.linreg(4.+2.5*t.idx+i,[1.,hl.or_missing((t.idx+i)%7!=0,t.idx)]),
        empty=hl.agg.filter(False,hl.agg.linreg(t.idx,[1.,t.idx]))),hl.range(3)))
    for i,fit in enumerate(fits):
        assert fit.shared.n==fit.entry.n==40
        assert fit.shared.beta==pytest.approx([4.+i,2.5],abs=1e-8)
        assert fit.entry.beta==pytest.approx([4.,2.5],abs=1e-8)
        assert fit.masked.n==sum((j+i)%3!=0 for j in range(40))
        assert fit.masked.beta==pytest.approx([4.,2.5],abs=1e-8)
        assert fit.missing.n==sum((j+i)%7!=0 for j in range(40))
        assert fit.missing.beta==pytest.approx([4.+i,2.5],abs=1e-8)
        assert fit.empty.n==0 and fit.empty.beta is None


@pytest.mark.parametrize('partitions',[1,3])
def test_matrix_coordinate_linreg_sparse_entry_predictors(partitions):
    m=hl.utils.range_matrix_table(40,3,n_partitions=partitions)
    m=m.annotate_entries(x=hl.float64(m.row_idx+m.col_idx))
    m=m.filter_entries((m.row_idx+m.col_idx)%3!=0)
    m=m.annotate_cols(fit=hl.agg.linreg(4.+2.5*m.x,[1.,m.x]))
    for column,fit in enumerate(m.fit.collect()):
        assert fit.n==sum((row+column)%3!=0 for row in range(40))
        assert fit.beta==pytest.approx([4.,2.5],abs=1e-8)
