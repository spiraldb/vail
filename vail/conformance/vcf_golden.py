"""Compare Vail decoding with the original Hail golden MatrixTables.

Run --generate with the pinned Hail interpreter, then --compare with Vail.
Golden extraction is a test fixture adapter, not Hail-format runtime support.
"""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import time

CASES = [('ex.vcf','GRCh37'),('sample.vcf','GRCh37'),('gvcfs/HG00096.g.vcf.gz','GRCh38')]


def plain(hl, value):
    if isinstance(value,hl.Locus): return dict(contig=value.contig,position=value.position)
    if isinstance(value,hl.Call): return dict(alleles=list(value.alleles),phased=value.phased)
    if isinstance(value,(dict,hl.Struct)): return {k:plain(hl,v) for k,v in value.items()}
    if isinstance(value,(list,tuple)): return [plain(hl,v) for v in value]
    if isinstance(value,set): return sorted(plain(hl,v) for v in value)
    if isinstance(value,float) and math.isnan(value): return 'NaN'
    return value


def same(left,right):
    if isinstance(left,float) and isinstance(right,(float,int)):
        # Hail _same defaults to relative tolerance 1e-6 with a normal floor.
        return left==right or abs(left-right)<=max(1e-6*max(abs(left),abs(right)),2**-1022)
    if isinstance(left,dict) and isinstance(right,dict):
        return left.keys()==right.keys() and all(same(left[k],right[k]) for k in left)
    if isinstance(left,list) and isinstance(right,list):
        return len(left)==len(right) and all(same(a,b) for a,b in zip(left,right))
    return left==right


def generate(resources,output):
    import hail as hl
    hl.init(master='local[2]',quiet=True,log=str(output/'hail.log'))
    try:
        for name,reference in CASES:
            matrix=hl.read_matrix_table(str(resources/name)+'.mt')
            rows=plain(hl,matrix.localize_entries('entries','cols').collect())
            record=dict(rows=rows,cols=plain(hl,matrix.cols().collect()),row_type=str(matrix.row.dtype),entry_type=str(matrix.entry.dtype))
            with gzip.open(output/(Path(name).name+'.json.gz'),'wt') as file:
                json.dump(record,file,sort_keys=True)
            print(name,len(rows),flush=True)
    finally:
        hl.stop()


def compare(resources,output):
    import vail as hl
    reports=[]
    for name,reference in CASES:
        started=time.perf_counter()
        fixture=output/(Path(name).name+'.json.gz')
        # Also accept an existing uncompressed oracle extraction.
        if fixture.exists():
            with gzip.open(fixture,'rt') as file: expected=json.load(file)
        else:
            fixture=output/(Path(name).name+'.json')
            expected=json.loads(fixture.read_text())
        matrix=hl.import_vcf(str(resources/name),reference_genome=reference,array_elements_required=False,force_bgz=True)
        assert matrix.row.dtype==hl.dtype(expected['row_type'])
        assert matrix.entry.dtype==hl.dtype(expected['entry_type'])
        assert plain(hl,matrix.cols().collect())==expected['cols']
        actual=plain(hl,matrix.localize_entries('entries','cols').collect())
        assert same(actual,expected['rows']), name
        reports.append(dict(file=name,status='passed',rows=len(actual),samples=len(expected['cols']),seconds=time.perf_counter()-started,source_sha256=hashlib.sha256((resources/name).read_bytes()).hexdigest(),oracle_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest()))
        print(name,'passed',len(actual),flush=True)
    return reports


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--generate',action='store_true')
    parser.add_argument('--resources',type=Path,required=True)
    parser.add_argument('--fixtures',type=Path,required=True)
    parser.add_argument('--report',type=Path)
    args=parser.parse_args()
    args.fixtures.mkdir(parents=True,exist_ok=True)
    if args.generate: generate(args.resources,args.fixtures)
    else:
        results=compare(args.resources,args.fixtures)
        if args.report: args.report.write_text(json.dumps(results,indent=2)+'\n')
