import time
import vail as hl
m=hl.MatrixTable.read_legacy('/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt')
for _ in range(4):assert m.aggregate_entries(hl.agg.sum(m.DP))==353709503
print('READY',flush=True)
end=time.monotonic()+6
while time.monotonic()<end:assert m.aggregate_entries(hl.agg.sum(m.DP))==353709503
