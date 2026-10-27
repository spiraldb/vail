"""Same public computations for matched timing and candidate attribution."""
import vail as h
from common import EXPECTED

def construct(m, case):
    if case=='scalar':
        m=m.annotate_rows(__h012_scalar=h.int32(m.locus.position%7+h.len(m.alleles)))
        m=m.select_entries(score=m.GQ+m['__h012_scalar'])
    else:
        m=m.annotate_rows(__h012_capture=h.struct(
            small=h.array([h.int32(m.locus.position%7),h.len(m.alleles)]),
            unused=h.str(m.locus)+':'+'x'*EXPECTED['padding_bytes']))
        if case=='whole_full':
            m=m.select_entries(score=m.GQ+h.sum(m['__h012_capture'].small),saved=m['__h012_capture'])
        else:m=m.select_entries(score=m.GQ+h.sum(m['__h012_capture'].small))
    result={'total':h.agg.sum(m.score),'present':h.agg.count_where(h.is_defined(m.score))}
    if case=='whole_full':
        result.update(payload=h.agg.sum(h.len(m.saved.unused)),small=h.agg.sum(h.sum(m.saved.small)))
    expression=h.struct(**result)
    return lambda:dict(m.aggregate_entries(expression))
