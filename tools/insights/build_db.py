"""Assemble insights.db from stage/*.json (box CPU stats) + Colab outputs (topics_out.parquet, llm_out.json)."""
import json,os,re,sqlite3,sys,datetime,collections
import pandas as pd
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..'))
from dixo.insights import SCHEMA
S,O,DB=sys.argv[1],sys.argv[2],sys.argv[3]
L=lambda f:json.load(open(os.path.join(S,f)))
llm=json.load(open(os.path.join(O,'llm_out.json'))); out=llm['out']
def js(k):
    t=out.get(k) or ''; m=re.search(r'\{.*\}',t,re.S)
    try: return json.loads(m.group(0)) if m else {}
    except ValueError: return {}
if os.path.exists(DB): os.remove(DB)
c=sqlite3.connect(DB); c.executescript(SCHEMA)
# topics
tp=pd.read_parquet(os.path.join(O,'topics_out.parquet')); tm=pd.read_parquet(os.path.join(S,'thread_month.parquet'))
tm=tm.merge(tp[['thread_id','topic']],on='thread_id'); agg=tm.groupby(['topic','ym']).msgs.sum().reset_index()
yms=sorted(agg.ym.unique()); last3,prev12=yms[-3:],yms[-15:-3]
tot=agg.groupby('ym').msgs.sum()
for t,g in tp.groupby('topic'):
    lab=js(f'topic:{t}'); a=agg[agg.topic==t].set_index('ym').msgs
    s3=a.reindex(last3).fillna(0).sum()/tot.reindex(last3).sum(); s12=a.reindex(prev12).fillna(0).sum()/max(1,tot.reindex(prev12).sum())
    c.execute('INSERT INTO topics VALUES(?,?,?,?,?,?,?)',(int(t),lab.get('name') or f'话题{t}',lab.get('desc',''),len(g),int(g.n.sum()),round(float(s3/s12-1) if s12 else 0,3),
      json.dumps(g.sort_values('n',ascending=False).title.head(6).tolist(),ensure_ascii=False)))
c.executemany('INSERT INTO topic_month VALUES(?,?,?)',[(int(r.topic),r.ym,int(r.msgs)) for r in agg.itertuples()])
c.executemany('INSERT INTO thread_topic VALUES(?,?)',[(r.thread_id,int(r.topic)) for r in tp.itertuples()])
# network
net=L('network.json')
for ci in net['circles']:
    lab=js(f"circle:{ci['id']}")
    c.execute('INSERT INTO circles VALUES(?,?,?,?,?,?,?)',(ci['id'],lab.get('name') or f"圈子{ci['id']+1}",lab.get('desc',''),ci['size'],json.dumps(ci['core'],ensure_ascii=False),json.dumps(ci['top_threads'],ensure_ascii=False),json.dumps(ci['words'],ensure_ascii=False)))
    c.executemany('INSERT OR IGNORE INTO member_circle VALUES(?,?)',[(u,ci['id']) for u in ci['members']])
c.execute("INSERT INTO meta VALUES('network',?)",(json.dumps({'nodes':net['nodes'],'edges':net['edges']},ensure_ascii=False),))
# memes: stage/memes.json is pre-curated by tools/insights/memes.py (in-joke discovery); the LLM only vetoes
# (is_meme false) or adds meaning/origin/usage when it has a label for the term. Emoji codes are never kept.
mm=L('memes.json'); kept=0
for t in mm['terms']+mm['phrases']:
    lab=js(f"meme:{t['kind']}:{t['term']}")
    if lab.get('is_meme') is False: continue
    f=t['first']; kept+=1
    c.execute('INSERT OR REPLACE INTO memes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(t['term'],t['kind'],lab.get('meaning',''),lab.get('origin',''),lab.get('usage',''),t['total'],t['authors'],
      f['day'],f['author'],f['author_id'],f['thread'],f['thread_id'],f['text'],t['peak_ym'],json.dumps(t['monthly']),json.dumps(t['examples'][:8],ensure_ascii=False)))
# reports
c.executemany('INSERT INTO member_year VALUES(?,?,?)',[(x['user_id'],x['year'],json.dumps({k:v for k,v in x.items() if k not in('user_id','year')},ensure_ascii=False)) for x in L('member_year.json')])
c.executemany('INSERT INTO server_month VALUES(?,?,?)',[(x['ym'],json.dumps(x,ensure_ascii=False),(out.get(f"month:{x['ym']}") or '').strip()) for x in L('server_month.json')])
# NaN never reaches the web app (see dixo.insights._j); still strip it at the source
for ym,d in list(c.execute('SELECT ym,data FROM server_month')): c.execute('UPDATE server_month SET data=? WHERE ym=?',(json.dumps(json.loads(d,parse_constant=lambda _:None),ensure_ascii=False),ym))
dos=0
for d in L('dossier_inputs.json'):
    md=(out.get(f"dossier:{d['user_id']}") or '').strip()
    if md: c.execute('INSERT INTO dossiers VALUES(?,?,?)',(d['user_id'],d['name'],md)); dos+=1
from dixo.insights import fix_doc, meme_terms  # replace LLM meme sections with the curated glossary
c.executemany('INSERT INTO docs VALUES(?,?)',[(k,fix_doc(k,(out.get(o) or '').strip(),meme_terms(c))) for k,o in (('overview','overview'),('persona_draft','persona'))])
c.executemany('INSERT INTO corpus VALUES(?,?,?,?,?)',[(x['id'],x['text'],x['reacts'],x['thread'],x['day']) for x in L('corpus.json')])
meta={'built_at':datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),'model':llm.get('model',''),'llm_secs':str(llm.get('secs','')),
      'data_through':L('community.json')['last'],'topics':str(tp.topic.nunique()),'circles':str(len(net['circles'])),'memes':str(kept),'dossiers':str(dos)}
c.executemany('INSERT INTO meta VALUES(?,?)',meta.items()); c.commit(); c.execute('VACUUM'); c.close()
print(meta)
