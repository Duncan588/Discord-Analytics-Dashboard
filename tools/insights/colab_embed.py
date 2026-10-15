# Runs on Colab T4: embed thread docs, KMeans topics. Writes /content/topics_out.parquet + centroids.npy
import pandas as pd,numpy as np,torch,time
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
t=time.time()
d=pd.read_parquet('/content/thread_docs.parquet')
model=SentenceTransformer('BAAI/bge-base-zh-v1.5',device='cuda'); model.half()
E=model.encode(d.doc.tolist(),batch_size=256,normalize_embeddings=True,show_progress_bar=False,convert_to_numpy=True).astype('float32')
print('embedded',E.shape,round(time.time()-t),'s',flush=True)
big=(d.n>=5).values
K=int(__import__('os').environ.get('K','70'))
km=KMeans(n_clusters=K,n_init=3,random_state=0).fit(E[big],sample_weight=np.log1p(d.n.values[big]))
C=km.cluster_centers_/np.linalg.norm(km.cluster_centers_,axis=1,keepdims=True)
sims=E@C.T
d['topic']=sims.argmax(1); d['sim']=sims.max(1)
d[['thread_id','n','title','topic','sim']].to_parquet('/content/topics_out.parquet')
np.save('/content/centroids.npy',C); np.save('/content/emb.npy',E.astype('float16'))
print('done',round(time.time()-t),'s',flush=True)
