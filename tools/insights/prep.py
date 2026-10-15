import json,os,re,math,collections,time
import numpy as np,pandas as pd,networkx as nx,jieba
T0=time.time()
def log(*a): print(f'[{time.time()-T0:.0f}s]',*a,flush=True)
EXP=os.environ.get('EXP','/workspace/ins/exp'); OUT='stage'; os.makedirs(OUT,exist_ok=True)
GHOST='456226577798135808'
m=pd.read_parquet(os.environ.get('ALL','/workspace/prof/all.parquet')).merge(pd.read_parquet('toks.parquet'),on='id',how='left'); m['tok']=m.tok.fillna('')
m['k']=m.id.astype('int64'); m['ts']=pd.Series(pd.to_datetime((m.k.to_numpy()>>22)+1420070400000,unit='ms',utc=True).tz_convert('Asia/Shanghai'),index=m.index)
m['ym']=m.ts.dt.strftime('%Y-%m'); m['yr']=m.ts.dt.year; m['dayl']=m.ts.dt.strftime('%Y-%m-%d')
U=pd.read_csv(f'{EXP}/users.csv',dtype=str)
NAME={r.user_id:(r.nickname if isinstance(r.nickname,str) and r.nickname else r.username) for r in U.itertuples()}
BOT=set(U[U.is_bot.isin(['1','True','true'])].user_id)
name=lambda u:NAME.get(str(u),str(u))
def real(u): u=str(u); n=str(name(u)); return u!=GHOST and u not in BOT and not n.lower().startswith('deleted_user') and n!='Deleted User'
th=pd.read_csv(f'{EXP}/threads.csv',dtype=str).set_index('thread_id')['name'].to_dict()
m['reacts']=m.id.map(pd.read_csv(f'{EXP}/reacts.csv',dtype={'message_id':str}).set_index('message_id')['n']).fillna(0).astype(int)
N=len(m); df=collections.Counter()
for s in m.tok: df.update(s.split())
jieba.initialize(); JF=jieba.dt.FREQ; JT=jieba.dt.total
EMO=re.compile(r'^[0-9a-z]{1,3}_[0-9a-z_]+$|^[a-z]+[0-9]+[a-z0-9_]*$',re.I)
def dist(toks,k=12,minc=4):
    c=collections.Counter(w for s in toks for w in s.split()); n=max(1,len(toks))
    sc={w:(v/n)*math.log((v/n)/((df[w]+1)/N)) for w,v in c.items() if v>=minc and not EMO.match(w)}
    return [w for w,_ in sorted(sc.items(),key=lambda x:-x[1])[:k]]
TT=lambda s,n=6:[{'thread_id':t,'title':th.get(t,''),'msgs':int(c)} for t,c in s.value_counts().head(n).items()]
SENT=lambda g:{k:round(float(v),3) for k,v in g.label.value_counts(normalize=True).items()}|{'mean':round(float(g.score.mean()),3)}
log('loaded',N)
# network
Eym=[]
for f in ('replies_ym.csv','mentions_ym.csv'):
    e=pd.read_csv(f'{EXP}/{f}',dtype={'src':str,'dst':str}); Eym.append(e[e.src.map(real)&e.dst.map(real)])
Eym=pd.concat(Eym); E=collections.Counter()
for r in Eym.groupby(['src','dst']).n.sum().reset_index().itertuples(): E[tuple(sorted((r.src,r.dst)))]+=r.n
G=nx.Graph(); G.add_weighted_edges_from((a,b,w) for (a,b),w in E.items() if w>=8)
comms=sorted([c for c in nx.community.louvain_communities(G,weight='weight',seed=1) if len(c)>=5],key=lambda c:-G.subgraph(c).size(weight='weight'))
cid={n:i for i,c in enumerate(comms) for n in c}; msgc=m.author_id.value_counts(); circles=[]
for i,c in enumerate(comms):
    s=G.subgraph(c); core=sorted(c,key=lambda n:-s.degree(n,weight='weight')); cm=m[m.author_id.isin(c)]
    circles.append({'id':i,'size':len(c),'weight':int(s.size(weight='weight')),'core':[{'user_id':u,'name':name(u),'msgs':int(msgc.get(u,0))} for u in core[:10]],
      'members':core,'top_threads':TT(cm.thread_id,8),'words':dist(cm.tok.sample(min(30000,len(cm)),random_state=1).tolist(),15,10)})
H=G.subgraph(sorted(G.nodes,key=lambda n:-G.degree(n,weight='weight'))[:450]).copy()
for u,v,d in H.edges(data=True): d['lw']=math.log1p(d['weight'])
pos=nx.spring_layout(H,weight='lw',seed=2,iterations=300)
json.dump({'nodes':[{'id':n,'name':name(n),'c':cid.get(n,-1),'x':round(float(pos[n][0]),4),'y':round(float(pos[n][1]),4),'w':int(G.degree(n,weight='weight'))} for n in H.nodes],
  'edges':sorted(([u,v,int(d['weight'])] for u,v,d in H.edges(data=True)),key=lambda e:-e[2])[:2500],'circles':circles,
  'graph':{'nodes':G.number_of_nodes(),'edges':G.number_of_edges()}},open(f'{OUT}/network.json','w'),ensure_ascii=False)
log('network',G.number_of_nodes(),len(comms))
# memes
cand=sorted(((w,c,math.log((c/N)/((JF.get(w,0)+1)/JT))) for w,c in df.items() if c>=80 and not EMO.match(w) and not re.fullmatch(r'[a-zA-Z]{1,2}',w)),key=lambda x:-x[2])[:1500]
ems=[w for w,c in df.most_common(3000) if EMO.match(w)][:50]
keep={w for w,_,_ in cand}|set(ems)
ex=m[['id','k','author_id','thread_id','ym','dayl','text','reacts','tok']].copy(); ex['tok']=ex.tok.str.split(); ex=ex.explode('tok'); ex=ex[ex.tok.isin(keep)]
gb=dict(tuple(ex.groupby('tok')))
def trace(w,g,kind):
    g=g.sort_values('k'); f=g.iloc[0]; mon=g.groupby('ym').size(); seen=set(); cum={}
    for ym,a in g.groupby('ym').author_id: seen|=set(a); cum[ym]=len(seen)
    return {'term':w,'kind':kind,'total':int(len(g)),'authors':int(g.author_id.nunique()),
     'first':{'id':f.id,'day':f.dayl,'author_id':f.author_id,'author':name(f.author_id),'thread_id':f.thread_id,'thread':th.get(f.thread_id,''),'text':f.text[:200]},
     'early':[{'day':r.dayl,'author':name(r.author_id),'thread':th.get(r.thread_id,''),'text':r.text[:150]} for r in g.head(5).itertuples()],
     'top_users':[{'name':name(u),'n':int(n)} for u,n in g.author_id.value_counts().head(6).items() if real(u)][:5],
     'peak_ym':mon.idxmax(),'burst':round(float(mon.max()/max(1,np.median(mon.values))),1),'monthly':{k:int(v) for k,v in mon.items()},'cum_authors':cum,
     'examples':[t[:150] for t in g.nlargest(4,'reacts').text]+[t[:150] for t in g.sample(min(5,len(g)),random_state=3).text]}
memes=[]
for w,c,s in cand:
    g=gb.get(w)
    if g is None or g.author_id.nunique()<25: continue
    memes.append(trace(w,g,'term')|{'distinct':round(s,2)})
    if len(memes)>=200: break
emo=[trace(w,gb[w],'emoji') for w in ems if w in gb]
cp=m[m.text.str.len().between(4,30)].groupby('text').agg(n=('id','size'),a=('author_id','nunique')).query('a>=30 and n>=60').sort_values('n',ascending=False).head(60)
phr=[trace(t,m[m.text==t],'phrase') for t in cp.index]
json.dump({'terms':memes,'emoji':emo,'phrases':phr},open(f'{OUT}/memes.json','w'),ensure_ascii=False); log('memes',len(memes),len(emo),len(phr))
# reports
Eym['yr']=Eym.ym.str[:4].astype(int); bud=collections.defaultdict(collections.Counter)
for r in Eym.itertuples(): bud[(r.src,r.yr)][r.dst]+=r.n; bud[(r.dst,r.yr)][r.src]+=r.n
my=[]
for (u,y),g in m.groupby(['author_id','yr']):
    if len(g)<50 or not real(u): continue
    d=g.groupby('dayl').agg(n=('id','size'),s=('score','mean')); d2=d[d.n>=15]; b=g.loc[g.reacts.idxmax()]
    my.append({'user_id':u,'year':int(y),'msgs':int(len(g)),'active_days':int(len(d)),'phrases':dist(g.tok.sample(min(4000,len(g)),random_state=1).tolist(),8,4),
     'buddies':[{'user_id':x,'name':name(x),'n':int(n)} for x,n in bud[(u,y)].most_common(5)],'busiest_day':{'day':d.n.idxmax(),'msgs':int(d.n.max())},
     'happiest_day':{'day':d2.s.idxmax(),'mean':round(float(d2.s.max()),3)} if len(d2) else None,'gloomiest_day':{'day':d2.s.idxmin(),'mean':round(float(d2.s.min()),3)} if len(d2) else None,
     'top_threads':TT(g.thread_id,5),'sentiment':SENT(g),'best_msg':{'text':b.text[:200],'reacts':int(b.reacts),'thread':th.get(b.thread_id,'')} if b.reacts>0 else None})
r=pd.DataFrame(my).groupby('year').msgs.rank(ascending=False,method='min')
for x,k in zip(my,r): x['rank']=int(k)
json.dump(my,open(f'{OUT}/member_year.json','w'),ensure_ascii=False); log('member_year',len(my))
firsts=m.groupby('author_id').ym.min(); mt=collections.defaultdict(dict)
for t in memes:
    for ym,n in t['monthly'].items(): mt[ym][t['term']]=n
yms=sorted(m.ym.unique()); sm=[]
for i,ym in enumerate(yms):
    g=m[m.ym==ym]; pv=yms[max(0,i-3):i]; b=g.loc[g.reacts.idxmax()]
    ris=sorted(((w,n,n/(1+sum(mt[p].get(w,0) for p in pv)/max(1,len(pv)))) for w,n in mt[ym].items() if n>=30),key=lambda x:-x[2])[:6]
    sm.append({'ym':ym,'msgs':int(len(g)),'authors':int(g.author_id.nunique()),'new_authors':int((firsts==ym).sum()),'top_threads':TT(g.thread_id),
     'top_posters':[{'user_id':u,'name':name(u),'msgs':int(n)} for u,n in g.author_id.value_counts().head(12).items() if real(u)][:5],
     'rising_terms':[{'term':w,'n':int(n),'x':round(x,1)} for w,n,x in ris],'sentiment':SENT(g),
     'best_msg':{'text':b.text[:200],'reacts':int(b.reacts),'author':name(b.author_id),'thread':th.get(b.thread_id,'')}})
json.dump(sm,open(f'{OUT}/server_month.json','w'),ensure_ascii=False); log('months',len(sm))
# corpus
BAD=re.compile(r'操|艹|屌|鸡|逼|屄|妈的|你妈|尼玛|傻|sb|嫖|奸|性|做爱|叫床|射|撸|裸|乳|胸|屁|阴|精|前列腺|约炮|母狗|婊|贱|死全家|杀|死|🔪|跳楼|黑鬼|支那|蛆|龙鸣|鬼子|共匪|习|毛泽东|台独|政府|共产党|微信|vx|qq|电话|地址|身份证|住在|@|\d{6,}',re.I)
c=m[(m.reacts>=12)&m.text.str.len().between(6,140)]; c=c[~c.text.str.contains(BAD)&c.author_id.map(real)]
c=c.sort_values('reacts',ascending=False).drop_duplicates('text').groupby('author_id').head(12).head(800)
json.dump([{'id':r.id,'text':r.text,'reacts':int(r.reacts),'thread':th.get(r.thread_id,''),'day':r.dayl,'sentiment':r.label} for r in c.itertuples()],open(f'{OUT}/corpus.json','w'),ensure_ascii=False)
# dossier inputs
top=[u for u in msgc.index if real(u) and msgc[u]>=int(os.environ.get('MINMSG','300'))]
orig=collections.defaultdict(list)
for t in memes+phr: orig[t['first']['author_id']].append(t['term'])
rp=Eym.groupby(['src','dst']).n.sum().reset_index(); dos=[]
for rk,u in enumerate(top,1):
    g=m[m.author_id==u]
    pa=pd.concat([rp[rp.src==u].rename(columns={'dst':'o'}),rp[rp.dst==u].rename(columns={'src':'o'})]).groupby('o').n.sum().sort_values(ascending=False).head(8)
    smp=pd.concat([g.nlargest(25,'reacts'),(lambda h:h.sample(min(150,len(h)),random_state=5))(g[g.text.str.len()>=8])]).drop_duplicates('id').sort_values('k')
    dos.append({'user_id':u,'name':name(u),'rank':rk,'msgs':int(len(g)),'first':g.dayl.min(),'last':g.dayl.max(),'active_days':int(g.dayl.nunique()),
     'peak_hours':[int(h) for h in g.ts.dt.hour.value_counts().head(4).index],'yearly':[{'year':int(y),'msgs':int(len(x))} for y,x in g.groupby('yr')],
     'sentiment':SENT(g),'words':dist(g.tok.sample(min(15000,len(g)),random_state=1).tolist(),20,6),
     'repeated':[(k,int(v)) for k,v in g.text[g.text.str.len()<=12].value_counts().head(20).items()],'threads':TT(g.thread_id,10),
     'partners':[{'name':name(x),'n':int(n)} for x,n in pa.items()],'circle':cid.get(u),'originated':orig.get(u,[])[:10],
     'notable':[{'day':r.dayl,'thread':th.get(r.thread_id,''),'reacts':int(r.reacts),'text':r.text[:180]} for r in g.nlargest(10,'reacts').itertuples() if r.reacts>0],
     'samples':[f'[{r.dayl}] {r.text[:150]}' for r in smp.itertuples()]})
json.dump(dos,open(f'{OUT}/dossier_inputs.json','w'),ensure_ascii=False)
json.dump({'messages':N,'authors':int(m.author_id.nunique()),'first':m.dayl.min(),'last':m.dayl.max(),'sentiment':SENT(m),
  'by_year':{int(y):int(n) for y,n in m.yr.value_counts().sort_index().items()},'top_members':[{'name':name(u),'msgs':int(msgc[u])} for u in top[:20]],
  'hot_threads':TT(m.thread_id,25)},open(f'{OUT}/community.json','w'),ensure_ascii=False)
log('DONE')
