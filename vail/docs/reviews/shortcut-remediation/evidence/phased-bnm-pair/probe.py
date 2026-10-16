import json,hashlib,inspect,pathlib,os
import vail as hl
expected=[[0,0],[0,0],[0,0],[0,0],[0,0],[1,1],[1,1],[1,1],[1,0],[1,1],[1,1],[0,1],[1,0],[1,0],[0,1],[0,0],[0,0],[0,0],[0,0],[1,0],[1,1],[1,1],[0,1],[1,1],[1,1]]
def run():
    hl.reset_global_randomness()
    model=hl.balding_nichols_model(1,5,5,phased=True)
    phase=model.aggregate_entries(hl.agg.all(model.GT.phased))
    return phase,[c.alleles for c in model.GT.collect()]
a=run();b=run()
f=pathlib.Path(hl._native.__file__)
print(json.dumps(dict(label=os.environ['PROBE_LABEL'],native_path=str(f),native_sha256=hashlib.sha256(f.read_bytes()).hexdigest(),python_path=str(pathlib.Path(hl.__file__).resolve()),global_seed=0,reset_before_each=True,arguments=dict(n_populations=1,n_samples=5,n_variants=5,phased=True),all_phased=a[0],actual=a[1],expected=expected,matches_hail_golden=a[1]==expected,replay_equal=a==b)))
