import json
import vail as h
observed={}
def record(name,fn,expected=None,check=False):
 try:
  value=fn();observed[name]={'value':value}
  if check:assert value==expected,(name,value,expected)
 except Exception as e:
  observed[name]={'error_type':type(e).__name__,'error':str(e)}
  if check:raise
 finally:
  print(json.dumps({name:observed[name]},allow_nan=False),flush=True)
record('literal_R_duplicate',lambda:h.eval(h.vds.local_to_global([10,20,30],[0,1,1],3,7,'R')),[10,20,7],True)
record('literal_A_slot0_nonzero_duplicate',lambda:h.eval(h.vds.local_to_global([20,30],[2,1,1],3,7,'A')),[20,7],True)
record('R_present_null_vs_absent',lambda:h.eval(h.vds.local_to_global(h.literal([10,None],h.tarray(h.tint32)),[0,2],4,7,'R')),[10,7,None,7],True)
record('A_present_null_vs_absent',lambda:h.eval(h.vds.local_to_global(h.literal([None],h.tarray(h.tint32)),[2,1],4,7,'A')),[None,7,7],True)
record('R_missing_fill',lambda:h.eval(h.vds.local_to_global([10,20],[0,2],4,h.missing(h.tint32),'R')),[10,None,20,None],True)
t=h.utils.range_table(2,n_partitions=1)
t=t.annotate(values=h.array([10+t.idx*0,20,30]),mapping=h.array([0,1+t.idx*0,1]))
t=t.annotate(expanded=h.vds.local_to_global(t.values,t.mapping,3,7,'R'))
record('dynamic_R_duplicate',lambda:[r.expanded for r in t.select('expanded').collect()],[[10,20,7],[10,20,7]],True)
u=h.utils.range_table(2,n_partitions=1)
u=u.annotate(values=h.array([20+u.idx*0,30]),mapping=h.array([2,1+u.idx*0,1]))
u=u.annotate(expanded=h.vds.local_to_global(u.values,u.mapping,3,7,'A'))
record('dynamic_A_slot0_nonzero_duplicate',lambda:[r.expanded for r in u.select('expanded').collect()],[[20,7],[20,7]],True)
# Observations only: not semantic permission to hoist, suppress errors or RNG.
fill=h.case().when(False,0).or_error('H085 fill observation')
record('error_fill_all_present_observation',lambda:h.eval(h.vds.local_to_global([10,20],[0,1],2,fill,'R')))
record('error_fill_gap_observation',lambda:h.eval(h.vds.local_to_global([10,20],[0,1],3,fill,'R')))
record('seeded_rng_fill_observation',lambda:h.eval(h.vds.local_to_global(h.literal([10.0]),[0],4,h.rand_unif(seed=731),'R')))
print(json.dumps({'status':'literal_and_dynamic_oracles_PASS','effect_observations_not_general_contract':True}))
