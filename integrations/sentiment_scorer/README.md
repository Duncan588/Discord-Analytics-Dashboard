# Incremental sentiment scorer (off-box)

Prod (1 GB RAM) can't host the model, so this runs on the AstrBot LAN host (4 cores, ~4 GB free) and talks to prod
over HTTPS via the token-gated bridge in `dixo/api.py` (`/api/sentiment/v1`, env `SENTIMENT_API_TOKENS` on prod).

* Same model as the historical Colab run (`cardiffnlp/twitter-xlm-roberta-base-sentiment`, fp32 CPU) → labels are comparable.
* Only scores messages with id > max scored id (server-side cursor in `sentiment.db` meta `max_id`), so it must
  start **after** the historical import; before that `/pending` answers 409.
* Bot messages are filtered server-side; empty / link-only / <2 chars come back with `text=null` and stay NA.
* Re-posting a score is a no-op (INSERT OR IGNORE), so crashes/restarts never double count.

Runs **once a day** (04:00 Asia/Shanghai) via host cron; `INTERVAL=0` makes each run drain the backlog and exit.

```bash
docker build -t dixo-sentiment-scorer integrations/sentiment_scorer
# scorer.env: SENTIMENT_TOKEN=..., SENTIMENT_GUILDS=915249444721668096, INTERVAL=0, TORCH_THREADS=2  (chmod 600)
docker create --name dixo-sentiment --memory 2g --cpus 2 --env-file scorer.env dixo-sentiment-scorer
( crontab -l; echo '0 4 * * * docker start -a dixo-sentiment >> $HOME/dixo-sentiment/cron.log 2>&1' ) | crontab -
```
