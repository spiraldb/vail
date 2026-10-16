import sys,time,resource,json
from pathlib import Path
import vail as hl
n=int(sys.argv[1]); cols=20
source=Path(f"/private/tmp/vail-vcf-profile-{n}.vcf")
with source.open("w") as f:
 f.write('##fileformat=VCFv4.2\n##FORMAT=<ID=GT,Number=1,Type=String,Description="call">\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t'+"\t".join(f"s{i}" for i in range(cols))+"\n")
 for i in range(n): f.write(f"1\t{i+1}\t.\tA\tC\t.\tPASS\t.\tGT\t"+"\t".join(["0/1"]*cols)+"\n")
start=time.perf_counter(); mt=hl.import_vcf(str(source)); hl.export_vcf(mt,str(source)+".out")
print(json.dumps(dict(rows=n,cols=cols,elapsed_s=time.perf_counter()-start,peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,output_bytes=Path(str(source)+".out").stat().st_size)),flush=True)
