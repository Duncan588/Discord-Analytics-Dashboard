import os,pandas as pd,jieba,re
from multiprocessing import Pool
STOP=set(open('/workspace/r15/dixo/stopwords/zh.txt').read().split())|set(open('/workspace/r15/wordcloud_stopwords.txt').read().split())
def init(): jieba.setLogLevel(60); jieba.initialize()
def tk(chunk):
    out=[]
    for s in chunk:
        ws=[w for w in jieba.lcut(s) if len(w)>1 and w not in STOP and not w.isdigit() and not w.isspace()]
        out.append(' '.join(dict.fromkeys(w.replace(' ','') for w in ws)))
    return out
if __name__=='__main__':
    m=pd.read_parquet(os.environ.get('ALL','/workspace/prof/all.parquet'),columns=['id','text'])
    T=m.text.tolist(); ch=[T[i:i+20000] for i in range(0,len(T),20000)]
    with Pool(7,initializer=init) as p: r=p.map(tk,ch,chunksize=1)
    m['tok']=[x for c in r for x in c]
    m[['id','tok']].to_parquet('toks.parquet',compression='zstd'); print('ok',len(m))
