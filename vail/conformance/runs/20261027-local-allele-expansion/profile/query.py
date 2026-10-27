"""One consumed Number=R expansion, including only visible entry owners."""
def construct(h,m):
    expanded=h.vds.local_to_global(m.LAD,m.LA,m.n_alleles,0,'R')
    weighted=h.sum(h.range(h.len(expanded)).map(lambda i:h.int64(i+1)*h.int64(expanded[i])))
    q=h.struct(defined_arrays=h.agg.count_where(h.is_defined(expanded)),
               elements=h.agg.sum(h.len(expanded)),
               defined_elements=h.agg.sum(h.sum(expanded.map(lambda x:h.int64(h.is_defined(x))))),
               total=h.agg.sum(h.sum(expanded.map(h.int64))),weighted=h.agg.sum(weighted),
               present_owners=h.agg.count())
    def action():
        result=dict(m.aggregate_entries(q))
        present=result.pop('present_owners')
        if present!=483328:raise RuntimeError('Present owner domain differs')
        # aggregate_entries never visits absent owners. Defined arrays equal present
        # ONLY because source/setup separately proves every defined LA/LAD/n defined.
        result['missing_entries']=1658880-present
        return result
    return action
