import os,time
import vail as hl
p="/private/tmp/vail-legacy-scale-20261023/matrix_100000x10000.mt"
m=hl.MatrixTable.read_legacy(p)
assert m._force_count_rows()==100000
print("READY",os.getpid(),flush=True)
for _ in range(2):
 assert hl.MatrixTable.read_legacy(p)._force_count_rows()==100000
