"""Incremental sentiment scorer (runs off-box, e.g. the AstrBot LAN host).

Loop: GET  {BASE}/api/sentiment/v1/guilds/{GUILD}/pending   (server picks up after max scored id)
      score with the same model as the historical Colab run
      POST {BASE}/api/sentiment/v1/guilds/{GUILD}/scores   (+cursor so skipped/NA messages advance too)
until `more` is false, then sleep INTERVAL seconds (0 = run once and exit, for cron/systemd timers).

Env: DIXO_BASE_URL (https://discordanalytics.monster6324.me), SENTIMENT_TOKEN, SENTIMENT_GUILDS (comma list),
     INTERVAL (3600), BATCH (256), MODEL (cardiffnlp/twitter-xlm-roberta-base-sentiment), TORCH_THREADS (2)
"""
import json
import logging
import os
import time
import urllib.request

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

BASE = os.environ.get("DIXO_BASE_URL", "https://discordanalytics.monster6324.me").rstrip("/")
TOKEN = os.environ["SENTIMENT_TOKEN"]
GUILDS = [g.strip() for g in os.environ.get("SENTIMENT_GUILDS", "915249444721668096").split(",") if g.strip()]
INTERVAL = int(os.environ.get("INTERVAL", "3600"))
BATCH = int(os.environ.get("BATCH", "256"))
MODEL = os.environ.get("MODEL", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
UA = "dixo-sentiment-scorer/1.0"  # Cloudflare rejects the default Python-urllib UA
log = logging.getLogger("scorer")


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {TOKEN}", "User-Agent": UA, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


class Model:
    def __init__(self):
        torch.set_num_threads(int(os.environ.get("TORCH_THREADS", "2")))
        self.tok = AutoTokenizer.from_pretrained(MODEL)
        self.m = AutoModelForSequenceClassification.from_pretrained(MODEL).eval()
        names = [self.m.config.id2label[i].lower()[:3] for i in range(3)]
        assert names == ["neg", "neu", "pos"], names

    @torch.inference_mode()
    def score(self, texts):
        out = []
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        res = [None] * len(texts)
        for i in range(0, len(order), 32):
            idx = order[i:i + 32]
            enc = self.tok([texts[j] for j in idx], padding=True, truncation=True, max_length=128, return_tensors="pt")
            p = torch.softmax(self.m(**enc).logits, -1).tolist()
            for j, pr in zip(idx, p):
                res[j] = pr
        return res


def run_guild(model, gid):
    total = 0
    while True:
        d = call("GET", f"/api/sentiment/v1/guilds/{gid}/pending?limit={BATCH * 4}")["data"]
        items = [x for x in d["items"] if x.get("text")]
        scores = []
        if items:
            probs = model.score([x["text"] for x in items])
            for x, (pn, pu, pp) in zip(items, probs):
                lab = ("neg", "neu", "pos")[max(range(3), key=lambda k: (pn, pu, pp)[k])]
                scores.append({"id": x["id"], "label": lab, "p_neg": round(pn, 4), "p_neu": round(pu, 4), "p_pos": round(pp, 4)})
        if d["items"]:
            r = call("POST", f"/api/sentiment/v1/guilds/{gid}/scores", {"scores": scores, "cursor": d["cursor"]})
            total += r.get("inserted", 0)
        if not d.get("more"):
            return total


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    model = Model()
    while True:
        for gid in GUILDS:
            try:
                n = run_guild(model, gid)
                log.info("guild=%s scored=%d", gid, n)
            except Exception as e:  # keep the loop alive; next round retries from the server-side cursor
                log.warning("guild=%s error=%s", gid, e)
        if INTERVAL <= 0:
            return
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
