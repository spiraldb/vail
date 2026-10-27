"""Independent Python integer reference from exact original real cells."""
import gzip,json
from common import HERE,EXPECTED,digest
def source():
    path=HERE/'source.json.gz'
    if digest(path)!=EXPECTED['source_sha256']:raise RuntimeError('Original Hail extraction hash differs')
    data=json.loads(gzip.decompress(path.read_bytes()))
    if len(data['rows'])!=81 or len(data['cols'])!=5:raise RuntimeError('Original domain differs')
    records=[];outputs=[];present=0;absolute_total=0;absolute_weighted=0
    totals=dict(defined_arrays=0,elements=0,defined_elements=0,total=0,weighted=0,missing_entries=0)
    for row in data['rows']:
        n=len(row['alleles']);entries=[];expanded=[]
        for entry in row['entries']:
            if entry is None:
                entries.append(None);expanded.append(None);totals['missing_entries']+=1;continue
            la,lad=entry['LA'],entry['LAD']
            if la is None or lad is None or len(la)!=len(lad):raise RuntimeError('Defined source owner lacks matching LA/LAD')
            if any(type(i)!=int or i<0 or i>=n for i in la):raise RuntimeError('Source allele bounds')
            if any(v is not None and (type(v)!=int or not -(2**31)<=v<2**31) for v in lad):raise RuntimeError('LAD Int32 premise')
            lookup={}
            for index,value in zip(la,lad):
                if index not in lookup:lookup[index]=value
            values=[lookup.get(index,0) for index in range(n)]
            entries.append(dict(LA=la,LAD=lad));expanded.append(values);present+=1
            totals['defined_arrays']+=1;totals['elements']+=len(values)
            for index,value in enumerate(values):
                if value is not None:
                    totals['defined_elements']+=1;totals['total']+=value;totals['weighted']+=(index+1)*value
                    absolute_total+=abs(value);absolute_weighted+=(index+1)*abs(value)
        records.append(dict(n_alleles=n,entries=entries,source_contig=row['locus']['contig'],source_position=row['locus']['position'],source_alleles=row['alleles']));outputs.append(expanded)
    if present!=118:raise RuntimeError('Original defined owner distribution differs')
    multiplied={k:v*EXPECTED['replications'] for k,v in totals.items()}
    # Absolute-sum bounds cover every partial reduction, not just the final total.
    if any(abs(v)>=2**63 for v in multiplied.values()) or max(absolute_total,absolute_weighted)*EXPECTED['replications']>=2**63:raise RuntimeError('Int64 partial/final aggregate premise')
    return dict(records=records,source_outputs=outputs,one_replica=totals,expected=multiplied,
                samples=[c['s'] for c in data['cols']],present=present*EXPECTED['replications'])
