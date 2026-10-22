import gzip,json,collections,sys
from pathlib import Path
root=Path(__file__).resolve().parent
name=sys.argv[1] if len(sys.argv)>1 else "before"
p=json.load(gzip.open(root/f'profile-{name}-symbolicated.json.gz','rt'))
selfc=collections.Counter();inc=collections.Counter();paths=collections.Counter();total=0
for t in p['threads']:
    ft=t['frameTable'];fns=t['funcTable'];st=t['stackTable'];strings=t['stringArray']; cache={None:()}
    def path(i):
        if i in cache:return cache[i]
        chain=[];n=i
        while n not in cache:chain.append(n);n=st['prefix'][n]
        v=cache[n]
        for n in reversed(chain):
            f=ft['func'][st['frame'][n]];v=v+(strings[fns['name'][f]],);cache[n]=v
        return v
    for i,s in enumerate(t['samples']['stack']):
        if s is None:continue
        w=(t['samples']['threadCPUDelta'][i] or 0)/1000000
        if w <= 0:continue
        a=path(s);total+=w;selfc[a[-1]]+=w
        for n in set(a):inc[n]+=w
        paths[a]+=w
print('TOTAL',total)
for label,c in [('SELF',selfc),('INCLUSIVE',inc)]:
    print('\n'+label)
    for n,w in c.most_common(35):print(f'{100*w/total:6.2f}% {w:8.0f} {n}')
json.dump({'total':total,'self':selfc,'inclusive':inc},open(root/f'profile-{name}-hotspots.json','w'),indent=2)
with open(root/f'profile-{name}-stacks.folded','w') as f:
 for s,w in paths.items():f.write(';'.join(s)+' '+str(w)+'\n')
