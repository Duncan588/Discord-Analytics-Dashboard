# AI bot read-only API (`/api/ai/v1`)

Enable: set `AI_API_TOKENS=<token1>,<token2>` in `.env` (empty = disabled, every route returns 404).
Auth: `Authorization: Bearer <token>`. GET only. 120 requests/minute per token.

| Endpoint | Description |
|---|---|
| `GET /guilds` | servers with data |
| `GET /guilds/<gid>/members?limit=&offset=` | members ranked by message count (summary stats + top emoji) |
| `GET /guilds/<gid>/members/<uid>` | user row + profile (stats, daily/hourly series, interactions, keywords) |
| `GET /guilds/<gid>/members/<uid>/messages?limit=&before_rowid=` | newest-first messages (merged accounts included); paginate with `next_before_rowid` |
| `GET /guilds/<gid>/threads/<tid>/messages?limit=&before_rowid=` | messages of one thread |
| `GET /guilds/<gid>/search?q=&author=&limit=&offset=` | FTS5 full-text search (zh via jieba, en words); `indexed=false` until `search.db` is built |
| `GET /guilds/<gid>/members/<uid>/markdown[/history-001.md]` | exported Markdown (run the exporter first) |

Markdown export (incremental, safe to cron):

    python -m dixo.export_md data/servers/<gid>/discord_data.db            # all members
    python -m dixo.export_md data/servers/<gid>/discord_data.db --user <uid> --max-bytes 500000

Output `data/exports/<gid>/members/<uid>/README.md` (YAML front matter: ids, counts, active range,
peak hour, top words, file list) + `history-NNN.md` (grouped by thread → local date; split by size).

Example:

    curl -H "Authorization: Bearer $TOKEN" "https://<host>/api/ai/v1/guilds/<gid>/search?q=数据库&limit=5"
