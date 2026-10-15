#!/usr/bin/env bash
# Monthly insights rerun, driven from the box (see README in this folder).
# Needs: INSIGHTS_TOKEN (one of prod INSIGHTS_API_TOKENS), authorised Colab CLI, ssh alias `dixo-prod`, $PY venv (pandas, pyarrow, jieba, networkx).
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd); W=${WORK:-/workspace/ins}; PY=${PY:-/workspace/llm/bin/python}; C=${COLAB:-/workspace/colabcli/bin/colab}
GUILD=${GUILD:-915249444721668096}; export ALL=${ALL:-/workspace/prof/all.parquet} EXP=${EXP:-$W/exp}
cd "$W"
$PY "$D/fetch.py"           # incremental pull (read-only HTTPS export API)
$PY "$D/tok.py"             # jieba tokens for all messages (~3 min)
$PY "$D/prep.py"            # network/circles, meme candidates+tracing, yearly/monthly reports, corpus, dossier inputs
rm -f stage/memes.json.cands; $PY "$D/memes.py" "$ALL" "$EXP/users.csv" "$EXP/threads.csv" stage/memes.json "$D/../../wordcloud_stopwords.txt"  # in-joke discovery, overwrites prep's meme list (~20 min)
$PY "$D/docs.py"            # thread docs + thread_month for topics
tar czf stage.tgz stage
S=ins$(date +%m%d); $C new -s "$S" --gpu T4; trap '$C stop -s "$S" || true' EXIT
for f in thread_docs.parquet stage.tgz "$D/colab_embed.py" "$D/colab_llm.py"; do $C upload -s "$S" "$f" "/content/$(basename "$f")"; done
printf 'import subprocess\nr=subprocess.run("cd /content && tar xzf stage.tgz && pip -q install vllm sentence-transformers && python colab_embed.py && python colab_llm.py",shell=True,capture_output=True,text=True)\nprint(r.stdout[-1500:],r.stderr[-1500:])\n' > /tmp/ins_job.py
$C exec -s "$S" -f /tmp/ins_job.py
mkdir -p colab_out; for f in topics_out.parquet llm_out.json; do $C download -s "$S" "/content/$f" "colab_out/$f"; done
$PY "$D/build_db.py" stage colab_out insights.db
P=/root/v2/V20/data/servers/$GUILD
scp -q insights.db "dixo-prod:$P/insights.db.new" && ssh dixo-prod "mv $P/insights.db.new $P/insights.db"
