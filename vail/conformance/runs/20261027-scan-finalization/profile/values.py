"""Lossless observed values and complete comparison, no native math code."""
from collections.abc import Mapping
import math
def encode(value):
    if value is None:return {'null':True}
    if isinstance(value,float):
        if math.isnan(value):return {'float':'NaN'}
        if math.isinf(value):return {'float':'+Inf' if value>0 else '-Inf'}
        return {'float':value.hex()}
    if isinstance(value,(str,int,bool)):return value
    if isinstance(value,Mapping):return {str(k):encode(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [encode(v) for v in value]
    raise TypeError(f'Unsupported output:{type(value)}')
def compatible(a,b):
    if isinstance(a,dict) and set(a)=={'float'} and isinstance(b,dict) and set(b)=={'float'}:
        x,y=a['float'],b['float']
        if x in ('NaN','+Inf','-Inf') or y in ('NaN','+Inf','-Inf'):return x==y
        return math.isclose(float.fromhex(x),float.fromhex(y),rel_tol=1e-12,abs_tol=1e-12)
    if type(a)!=type(b):return False
    if isinstance(a,dict):return a.keys()==b.keys() and all(compatible(a[k],b[k]) for k in a)
    if isinstance(a,list):return len(a)==len(b) and all(compatible(x,y) for x,y in zip(a,b))
    return a==b
