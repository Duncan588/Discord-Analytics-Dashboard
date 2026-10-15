# Colab T4: all LLM writing in one batched pass. In: /content/stage/*.json, topics_out.parquet. Out: /content/llm_out.json
import json,os,re,time,sys,pandas as pd
T0=time.time(); S='/content/stage'; L=lambda f:json.load(open(f'{S}/{f}'))
SYS="你是一个中文娱乐向 Discord 论坛社区的资料编辑。只依据给定材料写作，不编造；推测用“似乎/可能”。不复述粗口、色情、人身攻击，不涉及真实身份或隐私。"
jobs=[]
def add(key,prompt,maxt): jobs.append((key,prompt,maxt))
tp=pd.read_parquet('/content/topics_out.parquet')
for t,g in tp.groupby('topic'):
    titles=g.sort_values('n',ascending=False).title.head(30).tolist()
    add(f'topic:{t}',"以下是同一话题簇里最热门的帖子标题：\n"+"\n".join(titles)+'\n\n输出 JSON：{"name":"不超过8个字的中文话题名","desc":"一句话描述"}，只输出 JSON。',120)
net=L('network.json')
for c in net['circles'][:25]:
    add(f"circle:{c['id']}",f"一个群友圈子：核心成员 {', '.join(x['name'] for x in c['core'][:8])}；常去的帖子 {'; '.join(x['title'] for x in c['top_threads'][:6])}；高频词 {'、'.join(c['words'][:12])}。\n输出 JSON：{{\"name\":\"不超过8个字的有趣圈子名\",\"desc\":\"一句话描述这个圈子聊什么、什么氛围\"}}，只输出 JSON。",150)
mm=L('memes.json')
for t in mm['terms']+mm['emoji']+mm['phrases']:
    ctx={k:t[k] for k in ('term','kind','total','authors','peak_ym')}|{'first':t['first'],'early':t['early'][:4],'examples':t['examples'][:8]}
    add(f"meme:{t['kind']}:{t['term']}","这是社区里的一个高频梗/词（kind=emoji 表示自定义表情名）。材料：\n"+json.dumps(ctx,ensure_ascii=False)+'\n\n输出 JSON：{"meaning":"含义（1-2句）","origin":"起源推测（结合首次出现的帖子与语境，1-2句）","usage":"典型用法（1句）","is_meme":true/false（如果只是普通词或专有名词则为false）}，只输出 JSON。',260)
for s in L('server_month.json'):
    add(f"month:{s['ym']}","服务器月度数据：\n"+json.dumps({k:s[k] for k in s if k!='best_msg'},ensure_ascii=False)+"\n用 2-3 句中文写这个月的月报导语（热门话题、活跃度变化、流行词），轻松口吻，不点评个人。",200)
for d in L('dossier_inputs.json'):
    facts={k:v for k,v in d.items() if k not in('samples','user_id')}
    add(f"dossier:{d['user_id']}","统计数据：\n"+json.dumps(facts,ensure_ascii=False)+"\n\n消息样本：\n"+"\n".join(d['samples'][:120])+
     "\n\n为这位群友写 Markdown 档案（不要写标题行），小节：\n### 一句话印象\n### TA 是谁\n### 常聊话题\n### 说话风格\n### 口头禅与常用表情\n### 名场面\n### 人设小结\n形如 2a_shunfei2 的是自定义表情名。名场面只引用 notable 里的内容并说明是在别人的帖子里的回复还是自己的发言（无法判断就不说）。",900)
corp=L('corpus.json'); comm=L('community.json')
add('overview',"社区数据：\n"+json.dumps(comm,ensure_ascii=False)+"\n圈子："+json.dumps([{k:c[k] for k in('size','words')}|{'core':[x['name'] for x in c['core'][:5]]} for c in net['circles'][:10]],ensure_ascii=False)+
 "\n热梗："+'、'.join(t['term'] for t in mm['terms'][:40])+"\n\n写一份 Markdown 社区概况（不要标题行）：### 社区简介 ### 发展历程（按年份） ### 主要圈子 ### 社区文化与流行梗 ### 氛围与情绪。",1200)
add('persona',"以下是社区高赞发言样本（已过滤）：\n"+"\n".join(f"- {c['text']}" for c in corp[:150])+"\n社区流行词："+'、'.join(t['term'] for t in mm['terms'][:50])+
 "\n\n为这个社区的 AstrBot 聊天机器人写一份中文 system prompt（人格设定）：身份、说话风格（短句、接梗、适度调侃）、可以使用的社区梗与用法、禁区（色情、人身攻击、政治、隐私、引战）、遇到求助/严肃话题时如何切换语气、回复长度规范、示例对话 3 组。直接输出 prompt 正文。",1500)
print('jobs',len(jobs),flush=True)
P='/content/llm_partial.jsonl'
from vllm import LLM,SamplingParams
used=os.environ.get('MODEL','Qwen/Qwen2.5-7B-Instruct-AWQ')
llm=LLM(model=used,quantization='awq',dtype='half',max_model_len=12288,gpu_memory_utilization=0.92)
tok=llm.get_tokenizer(); out={}
if os.path.exists(P):
    for l in open(P,encoding='utf-8',errors='ignore'):
        try: d=json.loads(l); out[d['k']]=d['v']
        except Exception: pass
NJ=len(jobs); jobs=[j for j in jobs if j[0] not in out]; print('resume',len(out),'remain',len(jobs),flush=True)
for i in range(0,len(jobs),25):
    ch=jobs[i:i+25]
    ps=[tok.apply_chat_template([{'role':'system','content':SYS},{'role':'user','content':p}],tokenize=False,add_generation_prompt=True) for _,p,_ in ch]
    rs=llm.generate(ps,[SamplingParams(temperature=0.5,top_p=0.9,max_tokens=mt,repetition_penalty=1.05) for _,_,mt in ch],use_tqdm=False)
    with open(P,'a',encoding='utf-8') as f:
        for (k,_,_),o in zip(ch,rs):
            out[k]=o.outputs[0].text
            f.write(json.dumps({'k':k,'v':out[k]},ensure_ascii=False)+'\n')
            f.flush()
    open('/content/llm_progress.txt','w').write(f'{len(out)}/{NJ}\n')
    # also dump full out periodically for Drive sync
    if len(out)%100<25:
        json.dump({'model':used,'secs':round(time.time()-T0),'out':out},open('/content/llm_out.json','w',encoding='utf-8'),ensure_ascii=False)
    print('done',len(out),'/',NJ,flush=True)
json.dump({'model':used,'secs':round(time.time()-T0),'out':out},open('/content/llm_out.json','w',encoding='utf-8'),ensure_ascii=False)
print('DONE',flush=True)
