from dixo import textutil


def toks(s):
    return list(textutil.tokenize(s, extra=frozenset()))


def test_strips_urls_mentions_emoji_code_numbers():
    text = ("看 https://v.douyin.com/abc123 www.example.cn <@123456789012345678> <@!1> <#42> <@&7> "
            "<:pepe:111> <a:dance:222> @everyone :smile: 😂🔥 `npm install` ```py\nprint(1)\n``` 12345 数据库")
    out = toks(text)
    joined = " ".join(out)
    for bad in ("https", "douyin", "com", "pepe", "dance", "everyone", "smile", "npm", "print", "12345", "123456789012345678"):
        assert bad not in joined, bad
    assert "数据库" in out


def test_jieba_segments_chinese_and_drops_stopwords():
    out = toks("我觉得这个数据库优化之后快多了")
    assert "数据库" in out and "优化" in out
    assert "这个" not in out and "觉得" not in out


def test_english_lowercase_and_stopwords():
    out = toks("The Dashboard looks GREAT and the dashboard is fast")
    assert out.count("dashboard") == 2
    assert "the" not in out and "and" not in out and "great" not in out


def test_repeated_chars_collapsed():
    assert "哈哈哈哈哈" not in " ".join(toks("哈哈哈哈哈哈 loooooool"))


def test_format_word_cloud_reapplies_extra(monkeypatch):
    monkeypatch.setattr(textutil, "extra_stopwords", lambda: frozenset({"secretword"}))
    out = textutil.format_word_cloud([("secretword", 10), ("数据库", 5)], 10)
    assert [w["text"] for w in out] == ["数据库"]
