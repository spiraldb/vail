import sys,time,resource,json
from pathlib import Path
import vail as hl
n=int(sys.argv[1]); cols=20
mt=hl.range_matrix_table(n,cols)
mt=mt.annotate_rows(locus=hl.locus("1",mt.row_idx+1),alleles=["A","C"])
mt=mt.key_rows_by()
mt=mt.key_cols_by(s=hl.str(mt.col_idx)).annotate_entries(GT=hl.call(0,1))
output=Path(f"/private/tmp/vail-vcf-native-profile-{n}.vcf")
start=time.perf_counter();hl.export_vcf(mt,str(output))
print(json.dumps(dict(rows=n,cols=cols,elapsed_s=time.perf_counter()-start,peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,output_bytes=output.stat().st_size)),flush=True)
