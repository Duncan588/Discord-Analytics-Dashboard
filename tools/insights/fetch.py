"""Incremental pull from prod's read-only export API (Bearer INSIGHTS_API_TOKENS) into the box store:
appends cleaned new messages to $ALL (parquet); refreshes exp/{users,threads}.csv; appends reply/mention edges; refreshes recent reactions."""
import gzip,json,os,sys,urllib.request,collections
import pandas as pd
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..'))
from dixo.sentiment import clean_text
BASE=os.environ.get('DIXO_BASE_URL','https://discordanalytics.monster6324.me'); TOK=os.environ['INSIGHTS_TOKEN']; GID=os.environ.get('GUILD','915249444721668096')
ALL=os.environ.get('ALL','/workspace/prof/all.parquet'); EXP=os.environ.get('EXP','/workspace/ins/exp')
def get(kind,**q):
    url=f"{BASE}/api/insights/v1/guilds/{GID}/export/{kind}?"+'&'.join(f'{k}={v}' for k,v in q.items())
    r=urllib.request.urlopen(urllib.request.Request(url,headers={'Authorization':f'Bearer {TOK}','User-Agent':'dixo-insights/1.0'}),timeout=300).read()
    try: r=gzip.decompress(r)
    except OSError: pass
    return json.loads(r)
ym=lambda i:pd.Timestamp((int(i)>>22)+1420070400000,unit='ms',tz='UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m')
users=pd.DataFrame(get('users'),columns=['user_id','username','nickname','is_bot','avatar_url']).astype({'user_id':str}); users.to_csv(f'{EXP}/users.csv',index=False)
pd.DataFrame(get('threads'),columns=['thread_id','name','category_id']).to_csv(f'{EXP}/threads.csv',index=False)
bots=set(users[users.is_bot.astype(str).isin(['1','True'])].user_id)
cur=pd.read_parquet(ALL); lo=after=int(cur.id.astype('int64').max()); auth=dict(zip(cur.id,cur.author_id)); new=[]; edges=collections.Counter()
while True:
    rows=get('messages',after_id=after,limit=100000)
    for mid,a,t,ts,content,rep in rows:
        mid,a=str(mid),str(a); auth[mid]=a
        if rep and str(rep) in auth and auth[str(rep)]!=a: edges[(ym(mid),a,auth[str(rep)])]+=1
        txt=clean_text(content)
        if txt and a not in bots: new.append((mid,a,str(t),str(ts)[:10],txt))
    if rows: after=int(rows[-1][0]); print('fetched to',after,len(new),flush=True)
    if len(rows)<100000: break
if new:
    n=pd.DataFrame(new,columns=['id','author_id','thread_id','day','text']); n['label']=None; n['score']=float('nan')
    pd.concat([cur,n]).to_parquet(ALL,compression='zstd')
    add=pd.DataFrame([(k[0],k[1],k[2],v) for k,v in edges.items()],columns=['ym','src','dst','n'])
    pd.concat([pd.read_csv(f'{EXP}/replies_ym.csv',dtype=str),add.astype(str)]).to_csv(f'{EXP}/replies_ym.csv',index=False)
    mn=pd.DataFrame(get('mentions',after_id=lo),columns=['mid','src','dst'])
    if len(mn):
        mn['ym']=mn.mid.map(ym); mn=mn[mn.src!=mn.dst].groupby(['ym','src','dst']).size().rename('n').reset_index()
        pd.concat([pd.read_csv(f'{EXP}/mentions_ym.csv',dtype=str),mn.astype(str)]).to_csv(f'{EXP}/mentions_ym.csv',index=False)
    rc=pd.DataFrame(get('reactions',after_id=lo-((60*86400000)<<22)),columns=['message_id','n']).astype({'message_id':str})
    old=pd.read_csv(f'{EXP}/reacts.csv',dtype={'message_id':str}); old=old[~old.message_id.isin(rc.message_id)]
    pd.concat([old,rc[rc.n>=3]]).to_csv(f'{EXP}/reacts.csv',index=False)
print('new messages',len(new))
