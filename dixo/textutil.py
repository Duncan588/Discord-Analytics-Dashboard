"""Word-cloud tokenisation: noise stripping + jieba (zh) + English words + stopwords.

Pipeline (order matters, see KNOWN_ISSUES K20):
1. strip code blocks / inline code
2. strip URLs and bare domains
3. strip Discord markup: <@id> <@!id> <@&id> <#id> <:name:id> <a:name:id> <t:..>, @everyone/@here, :shortcode:
4. strip Unicode emoji / pictographs, numbers and snowflake ids
5. collapse repeated chars (哈哈哈哈 -> 哈哈, loooool -> lool)
6. Chinese: jieba segmentation, keep tokens of >=2 CJK chars not in zh stopwords
   English: [a-z][a-z'-]{2,} lowercased, not in en stopwords
Extra stopwords: <repo>/wordcloud_stopwords.txt (hot-reloaded by mtime).
"""
import collections
import logging
import os
import re
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(_HERE)
EXTRA_STOPWORDS_PATH = os.path.join(REPO_ROOT, "wordcloud_stopwords.txt")

_CODE_BLOCK_RE = re.compile(r"```.*?```|`[^`\n]*`", re.S)
_URL_RE = re.compile(r"(https?://\S+|www\.\S+|\b[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+\b\S*)", re.I)
_DISCORD_RE = re.compile(r"<a?:\w+:\d+>|<[@#][!&]?\d+>|<t:\d+(?::\w)?>|@(?:everyone|here)\b|:[a-z0-9_+-]{2,32}:", re.I)
_EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF\U00002300-\U000023FF"
    "\U00002B00-\U00002BFF\U0000FE0F\U0000200D\U000020E3\U00003030\U0000303D\U00003297\U00003299]+"
)
_NUM_RE = re.compile(r"\d+")
_REPEAT_RE = re.compile(r"(.)\1{2,}")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]+")
_CJK_TOKEN_RE = re.compile(r"^[\u4e00-\u9fff]{2,}$")
_EN_RE = re.compile(r"[a-zA-Z][a-zA-Z'-]*")
_EN_TOKEN_RE = re.compile(r"^[a-z][a-z'-]{2,}$")

_lock = threading.Lock()
_jieba = None
_extra_cache = (None, frozenset())


def _load_list(name):
    path = os.path.join(_HERE, "stopwords", name)
    try:
        with open(path, encoding="utf-8") as fh:
            return frozenset(l.strip().lower() for l in fh if l.strip() and not l.startswith("#"))
    except OSError:
        logging.getLogger(__name__).warning("stopword list missing: %s", path)
        return frozenset()


ZH_STOP = _load_list("zh.txt")
EN_STOP = _load_list("en.txt")


def extra_stopwords():
    """wordcloud_stopwords.txt, reloaded when its mtime changes."""
    global _extra_cache
    try:
        mtime = os.path.getmtime(EXTRA_STOPWORDS_PATH)
    except OSError:
        return frozenset()
    if _extra_cache[0] != mtime:
        with open(EXTRA_STOPWORDS_PATH, encoding="utf-8") as fh:
            words = frozenset(l.strip().lower() for l in fh if l.strip() and not l.lstrip().startswith("#"))
        _extra_cache = (mtime, words)
    return _extra_cache[1]


def _get_jieba():
    global _jieba
    if _jieba is None:
        with _lock:
            if _jieba is None:
                import jieba  # noqa: WPS433 (lazy: ~0.5s dictionary load)
                jieba.setLogLevel(logging.WARNING)
                jieba.initialize()
                _jieba = jieba
    return _jieba


def clean_text(text):
    """Remove everything that should never reach the word cloud."""
    if not text:
        return ""
    text = _CODE_BLOCK_RE.sub(" ", str(text))
    text = _URL_RE.sub(" ", text)
    text = _DISCORD_RE.sub(" ", text)
    text = _EMOJI_RE.sub(" ", text)
    text = _NUM_RE.sub(" ", text)
    text = _REPEAT_RE.sub(r"\1\1", text)
    return text


def tokenize(text, extra=None):
    """Yield filtered tokens for one message."""
    text = clean_text(text)
    if not text:
        return
    extra = extra_stopwords() if extra is None else extra
    jb = None
    for run in _CJK_RE.findall(text):
        if len(run) < 2:
            continue
        if jb is None:
            jb = _get_jieba()
        for tok in jb.lcut(run, HMM=True):
            if _CJK_TOKEN_RE.match(tok) and tok not in ZH_STOP and tok not in extra:
                yield tok
    for raw in _EN_RE.findall(text):
        tok = raw.lower().strip("'-")
        if _EN_TOKEN_RE.match(tok) and tok not in EN_STOP and tok not in extra:
            yield tok


def count_words(texts):
    extra = extra_stopwords()
    counter = collections.Counter()
    for t in texts:
        counter.update(tokenize(t, extra))
    return counter


def format_word_cloud(counter, limit=60):
    """Counter/iterable of (word, n) -> [{"text","weight"}] sorted desc, stopwords re-applied
    (so editing wordcloud_stopwords.txt takes effect without recounting)."""
    extra = extra_stopwords()
    items = counter.most_common() if hasattr(counter, "most_common") else sorted(counter, key=lambda x: -x[1])
    out = []
    for word, n in items:
        if word in extra or word in ZH_STOP or word in EN_STOP:
            continue
        out.append({"text": word, "weight": int(n)})
        if limit and len(out) >= limit:
            break
    return out
