import os,pandas as pd
m=pd.read_parquet(os.environ.get('ALL','/workspace/prof/all.parquet'),columns=['id','thread_id','text'])
m['k']=m.id.astype('int64'); m['ym']=pd.to_datetime((m.k.to_numpy()>>22)+1420070400000,unit='ms',utc=True).tz_convert('Asia/Shanghai').strftime('%Y-%m')
os.makedirs('stage',exist_ok=True); m.groupby(['thread_id','ym']).size().rename('msgs').reset_index().to_parquet('stage/thread_month.parquet')
m=m.sort_values('k'); f=m.groupby('thread_id').head(3).groupby('thread_id').text.apply(lambda s:' / '.join(t[:100] for t in s))
th=pd.read_csv(os.environ.get('EXP','exp')+'/threads.csv',dtype=str).set_index('thread_id')['name']; n=m.groupby('thread_id').size()
d=pd.DataFrame({'thread_id':n.index,'n':n.values}); d['title']=d.thread_id.map(th).fillna(''); d['doc']=(d.title+'。'+d.thread_id.map(f).fillna('')).str[:320]
d.to_parquet('thread_docs.parquet')
