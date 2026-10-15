"""Community in-joke extraction (replaces the token-frequency meme list in prep.py).
New-word discovery on Chinese char n-grams (cohesion + boundary entropy) and repeated short catchphrases,
scored by how over-represented they are here vs general Chinese (jieba dict freq), kept only if they recur across
many members and months. Emoji codes, URLs, mentions, numbers and latin junk are stripped before counting.
Usage: python memes.py ALL_PARQUET USERS_CSV THREADS_CSV OUT_JSON   (writes stage/memes.json format)"""
import collections, json, math, re, sys
import pandas as pd, jieba

ALL, USERS, THREADS, OUT = sys.argv[1:5]
SAMPLE = int(__import__("os").environ.get("MEME_SAMPLE", "500000"))
m = pd.read_parquet(ALL, columns=['id', 'author_id', 'thread_id', 'day', 'text'])
U = pd.read_csv(USERS, dtype=str)
NAME = {r.user_id: (r.nickname if isinstance(r.nickname, str) and r.nickname else r.username) for r in U.itertuples()}
BOT = set(U[U.is_bot.isin(['1', 'True', 'true'])].user_id)
m = m[~m.author_id.isin(BOT)]
th = pd.read_csv(THREADS, dtype=str).set_index('thread_id')['name'].to_dict()
JUNK = re.compile(r'https?://\S+|<a?:\w+:\d+>|<[@#!&]+\d+>|:\w+:|@\S+|\d+')
m['clean'] = m.text.fillna('').str.replace(JUNK, ' ', regex=True)
m['ym'] = m.day.str[:7]
jieba.initialize(); JF = jieba.dt.FREQ; JT = jieba.dt.total
STOP = set(open(sys.argv[5]).read().split()) if len(sys.argv) > 5 else set()
HAN = re.compile('[\u4e00-\u9fff]+')

import os
CACHE = OUT + '.cands'
if os.path.exists(CACHE):
    cands = [tuple(x) for x in json.load(open(CACHE))]
else:
    # ---- 1. char n-gram discovery on a sample
    s = m.clean.sample(min(SAMPLE, len(m)), random_state=7)
    cnt = collections.Counter(); L = collections.defaultdict(collections.Counter); R = collections.defaultdict(collections.Counter)
    for t in s:
        for run in HAN.findall(t):
            n = len(run)
            for k in range(1, 6):
                for i in range(n - k + 1):
                    cnt[run[i:i + k]] += 1
    cnt = collections.Counter({w: c for w, c in cnt.items() if c >= 25 or len(w) == 1})
    for t in s:
        for run in HAN.findall(t):
            n = len(run)
            for k in range(2, 6):
                for i in range(n - k + 1):
                    w = run[i:i + k]
                    if w in cnt:
                        L[w][run[i - 1] if i else '^'] += 1; R[w][run[i + k] if i + k < n else '$'] += 1
    TOT = sum(c for w, c in cnt.items() if len(w) == 1)
    ent = lambda c: -sum(v / sum(c.values()) * math.log(v / sum(c.values())) for v in c.values())
    cands = []
    for w, c in cnt.items():
        if len(w) < 2 or c < 40 or len(set(w)) == 1 or (len(w) >= 4 and len(set(w)) <= 2):
            continue
        p = c / TOT
        coh = min(math.log(p / ((cnt[w[:i]] / TOT) * (cnt[w[i:]] / TOT))) for i in range(1, len(w)))
        if coh < 3.5:
            continue
        be = min(ent(L[w]), ent(R[w]))
        if be < 1.2:
            continue
        gen = JF.get(w, 0)
        dist = math.log(p / ((gen + 1) / JT))  # over-representation vs general Chinese
        if gen > 30000 or dist < 4 or w in STOP:
            continue
        cands.append((w, c, dist, coh, be))
    cands.sort(key=lambda x: -x[1] * x[2])
    cands = cands[:1500]
    # drop substrings of a longer candidate with similar count (e.g. "斯卡" inside "派斯卡")
    cs = {w: c for w, c, *_ in cands}
    cands = [x for x in cands if not any(x[0] in o and o != x[0] and cs[o] >= 0.6 * x[1] for o in cs)]

    json.dump(cands, open(CACHE, 'w'), ensure_ascii=False)
print('ngram cands', len(cands), flush=True)

# ---- 2. catchphrases: whole short messages repeated by many people
st = m.clean.str.strip()
short = m[st.str.len().between(3, 16) & st.str.contains(HAN)].assign(p=st)
cp = short.groupby('p').agg(n=('id', 'size'), a=('author_id', 'nunique'), mo=('ym', 'nunique'))
cp = cp[(cp.a >= 25) & (cp.mo >= 4)]
phr = []
for p, r in cp.sort_values('n', ascending=False).head(800).iterrows():
    han = ''.join(HAN.findall(p))
    if len(han) < 3 or len(set(han)) <= 2 or len(han) / max(1, len(p.replace(' ', ''))) < .6:
        continue
    words = [x for x in jieba.lcut(han)]
    # catchphrase must not be ordinary chat ("我也是", "好的谢谢"): needs a rare word or an unusual whole-string
    rare = min((JF.get(x, 0) for x in words), default=0)
    if rare > 50000 and len(words) <= 3:
        continue
    phr.append(p)
print('phrase cands', len(phr), flush=True)

# ---- 4. curation: distinctiveness vs general Chinese, drop generic profanity / brands / duplicates
PROF = re.compile(r'卧槽|我操|操|草泥马|曹尼玛|擦尼玛|尼玛|泥马|你妈|他妈|妈的|妈逼|妈了个|傻逼|骚逼|臭逼|牛逼|牛批|几把|鸡巴|屁眼|死了|死啦|滚|去你|逼|狗日|贱')
FILL = re.compile(r'[我你他她们的了个啊呢吧吗哦呀是这那就太真很好也都在有没不一]')
GENERIC = set("""蜜雪冰城 拼多多 抖音 淘宝 微信 微博 小红书 知乎 贴吧 推特 谷歌 美团 闲鱼 比特币 英伟达 区块链 王者荣耀 英雄联盟 米哈游 原神 姆巴佩 梅西 马思唯
坂井泉水 生日快乐 王者 朋友圈 发朋友圈 送外卖 外卖员 富二代 二次元 网红 主播 博主 评论区 表情包 聊天记录 火钳刘明 越努力越幸运 尊嘟假嘟 偶买噶 哦买噶
阿里嘎多 前情提要 笑点解析 太长不看 史上最 一看就 长得像 长得跟 长啥样 没见过 没看懂 看不懂 真有这么 百块钱 块钱 淘宝买 拼多多买 抖音搜 抖音刷 拉我进群
发个帖 发个包 谈过恋爱 长得丑 真的很难 我来晚了 小哥哥一枚 一群傻逼 你们这群 习近平 习主席 川普 特朗普 新冠 社工库 肉便器 支那猪 支那人 白皮猪 樱花妹
小黄人 猪脚饭 骑电动车 虚拟币 大平层 看黄片 真的假的 绷不住了 臭屌 长得 很难 见过 拉黑 安卓 撸管 刻板印象 千块钱 打得过 在群里 结婚生子 傻逼东西 做题家 舔狗 马勒戈壁 烂梗 打手冲 多少沾点 鸡鸡操你 小吊子 大雄看报纸 多人正在输入 关你屁事 一坨屎 一脚踢死 打个飞机 德莱文 阿卡丽 盖伦 亚索 诺手 豹女 漫威 宝可梦 黑神话 法环 欧冠 打野 团战""".split())
HANS = lambda w: ''.join(HAN.findall(w))
def pgen(w):
    ws = jieba.lcut(w); p = 1.0
    for x in ws:
        p *= (JF.get(x, 0) + 1) / JT
    return max(JF.get(w, 0) / JT, p * 10 ** (len(ws) - 1))  # common words co-occur: crude adjacency prior
def curate(items, N, top=int(os.environ.get('MEME_TOP', '70'))):
    best = {}
    for x in items:
        h = HANS(x['term'])
        if len(h) < 2 or h in GENERIC or x['term'] in GENERIC:
            continue
        core = FILL.sub('', PROF.sub('', h))
        if len(core) < 2 and PROF.search(h):  # pure swearing ("我操你妈", "你妈个逼") is not an in-joke
            continue
        x['r'] = math.log((x['total'] / N) / pgen(h))
        if x['r'] < 11 or x['authors'] < 25:
            continue
        x['rank'] = x['r'] * math.log(x['authors']) * math.log(1 + x['months'])
        k = h  # merge punctuation variants ("唉 共勉吧" / "唉，共勉吧")
        if k not in best or best[k]['total'] < x['total']:
            best[k] = x
    out = sorted(best.values(), key=lambda x: -x['rank'])
    keep = []
    for x in out:  # drop near-duplicates (substring of an already kept entry)
        h = HANS(x['term'])
        if any(h in HANS(y['term']) or HANS(y['term']) in h for y in keep):
            continue
        keep.append(x)
        if len(keep) >= top:
            break
    return keep

if os.environ.get('REFILTER'):
    raw = json.load(open(OUT + '.raw')); out = curate(raw, len(m))
    json.dump({'terms': [x for x in out if x['kind'] == 'term'], 'emoji': [], 'phrases': [x for x in out if x['kind'] == 'phrase']}, open(OUT, 'w'), ensure_ascii=False)
    print('kept', len(out)); sys.exit()

# ---- 3. exact stats on full data
def trace(w, g, kind, score):
    g = g.sort_values('id', key=lambda x: x.astype('int64'))
    f = g.iloc[0]; mon = g.groupby('ym').size(); seen = set(); cum = {}
    for ym, a in g.groupby('ym').author_id:
        seen |= set(a); cum[ym] = len(seen)
    ex = g[g.text.str.len().between(len(w) + 2, 120)]
    ex = ex.drop_duplicates('text')
    exs = ex.sample(min(6, len(ex)), random_state=3).text.str[:150].tolist() if len(ex) else []
    return {'term': w, 'kind': kind, 'total': int(len(g)), 'authors': int(g.author_id.nunique()), 'months': int(mon.size), 'score': round(score, 2),
            'first': {'id': f.id, 'day': f.day, 'author_id': f.author_id, 'author': NAME.get(f.author_id, f.author_id), 'thread_id': f.thread_id,
                      'thread': th.get(f.thread_id, ''), 'text': f.text[:200]},
            'early': [{'day': r.day, 'author': NAME.get(r.author_id, r.author_id), 'thread': th.get(r.thread_id, ''), 'text': r.text[:150]} for r in g.head(5).itertuples()],
            'top_users': [{'name': NAME.get(u, u), 'n': int(n)} for u, n in g.author_id.value_counts().head(5).items()],
            'peak_ym': mon.idxmax(), 'burst': round(float(mon.max() / max(1, mon.median())), 1), 'monthly': {k: int(v) for k, v in mon.items()},
            'cum_authors': cum, 'examples': exs}

out = []
pat = re.compile('|'.join(re.escape(w) for w, *_ in cands))
hit = m[m.clean.str.contains(pat)]
for w, c, dist, coh, be in cands:
    g = hit[hit.clean.str.contains(w, regex=False)]
    a, mo = g.author_id.nunique(), g.ym.nunique()
    if a < 30 or mo < 5 or g.author_id.value_counts(normalize=True).iloc[0] > .35:
        continue
    out.append(trace(w, g, 'term', dist * math.log(a) * math.log(mo)))
for p in phr:
    g = short[short.p == p]
    if g.author_id.value_counts(normalize=True).iloc[0] > .35:
        continue
    out.append(trace(p, g, 'phrase', math.log(len(g)) * math.log(g.author_id.nunique()) * 1.5))
json.dump(out, open(OUT + '.raw', 'w'), ensure_ascii=False)
out = curate(out, len(m))
json.dump({'terms': [x for x in out if x['kind'] == 'term'], 'emoji': [], 'phrases': [x for x in out if x['kind'] == 'phrase']},
          open(OUT, 'w'), ensure_ascii=False)
print('kept', len(out), flush=True)
