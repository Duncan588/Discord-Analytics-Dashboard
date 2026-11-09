#!/root/v2/venv/bin/python
"""Incremental backup of discord_data.db (only new/changed/deleted rows).

  db-incr.py install DB             add backup_changelog + AFTER INSERT/UPDATE/DELETE triggers (idempotent)
  db-incr.py export DB OUTDIR [--since ISO]   write OUTDIR/<gid>-<seq>-<ts>.sqlite.zst + pending state
  db-incr.py commit DB              promote pending state after a successful upload; prune old changelog

Change capture: every write on every table (incl. edits, REPLACE, deletes) logs (tbl,rowid,op,pk)
into backup_changelog. Export = current version of every touched row + list of rows that are gone.
First export (no state) additionally copies a conservative window since --since to cover the gap
between the last full baseline and trigger install.
"""
import sqlite3, json, os, sys, time, subprocess

STATE = os.environ.get('DB_INCR_STATE', '/root/.local/state/db-incr')
SKIP = {'sqlite_sequence', 'backup_changelog'}
APPEND = ['messages', 'reactions', 'attachments', 'mentions', 'ingest_tombstones']
BUSY_MS = 5000


def q(n):
    return '"' + n.replace('"', '""') + '"'


def tables(c):
    return [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            if r[0] not in SKIP]


def cols(c, t):
    return [r[1] for r in c.execute(f'PRAGMA table_info({q(t)})')]


def pkcol(c, t):
    pk = [r[1] for r in c.execute(f'PRAGMA table_info({q(t)})') if r[5]]
    return pk[0] if len(pk) == 1 else None


def install(db):
    c = sqlite3.connect(db, timeout=BUSY_MS / 1000, isolation_level=None)
    c.execute(f'PRAGMA busy_timeout={BUSY_MS}')
    ddl = ["CREATE TABLE IF NOT EXISTS backup_changelog(id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT, src_rowid INTEGER,"
           " op TEXT, pk TEXT, at TEXT DEFAULT CURRENT_TIMESTAMP)"]
    for t in tables(c):
        pk = pkcol(c, t)
        npk = f'NEW.{q(pk)}' if pk else 'NULL'
        opk = f'OLD.{q(pk)}' if pk else 'NULL'
        n = 'trg_bkcl_' + t
        ins = "INSERT INTO backup_changelog(tbl,src_rowid,op,pk) VALUES"
        ddl += [
            f"CREATE TRIGGER IF NOT EXISTS {q(n+'_i')} AFTER INSERT ON {q(t)} BEGIN {ins}('{t}',NEW.rowid,'I',{npk}); END",
            f"CREATE TRIGGER IF NOT EXISTS {q(n+'_u')} AFTER UPDATE ON {q(t)} BEGIN {ins}('{t}',NEW.rowid,'U',{npk}); END",
            # key moved by UPDATE: the old key/rowid is gone
            f"CREATE TRIGGER IF NOT EXISTS {q(n+'_uo')} AFTER UPDATE ON {q(t)} WHEN OLD.rowid<>NEW.rowid OR {opk} IS NOT {npk}"
            f" BEGIN {ins}('{t}',OLD.rowid,'D',{opk}); END",
            f"CREATE TRIGGER IF NOT EXISTS {q(n+'_d')} AFTER DELETE ON {q(t)} BEGIN {ins}('{t}',OLD.rowid,'D',{opk}); END",
        ]
    c.execute('BEGIN IMMEDIATE')
    for s in ddl:
        c.execute(s)
    c.execute('COMMIT')
    n = c.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_bkcl_%'").fetchone()[0]
    print(f'install ok {db} triggers={n}')


def gid_of(db):
    return os.path.basename(os.path.dirname(os.path.abspath(db)))


def first_rowid(c, t, pred, lo, hi):
    """smallest rowid in [lo,hi] whose row satisfies pred(row) (assumes ~monotonic); rows fetched by rowid seek."""
    while lo < hi:
        mid = (lo + hi) // 2
        r = c.execute(f'SELECT rowid,* FROM {q(t)} WHERE rowid>=? ORDER BY rowid LIMIT 1', (mid,)).fetchone()
        if r is None or pred(c, r):
            hi = mid
        else:
            lo = mid + 1
    return lo


def export(db, outdir, since=None):
    gid = gid_of(db)
    os.makedirs(STATE, exist_ok=True)
    os.makedirs(outdir, exist_ok=True)
    sf = f'{STATE}/{gid}.json'
    st = json.load(open(sf)) if os.path.exists(sf) else None
    if st is None and not since:
        sys.exit(f'{gid}: no state yet; pass --since')
    s = sqlite3.connect(f'file:{db}?mode=ro', uri=True, timeout=BUSY_MS / 1000, isolation_level=None)
    s.execute(f'PRAGMA busy_timeout={BUSY_MS}')
    s.execute('BEGIN')  # one consistent read snapshot (WAL)
    if not s.execute("SELECT 1 FROM sqlite_master WHERE name='backup_changelog'").fetchone():
        sys.exit(f'{gid}: backup_changelog missing; run install first')
    cl_lo = st['cl_hi'] if st else 0
    cl_hi = s.execute('SELECT coalesce(max(id),0) FROM backup_changelog').fetchone()[0]
    seq = (st['seq'] + 1) if st else 1
    touched = {}  # tbl -> {rowid: pk}
    for tbl, rid, pk in s.execute('SELECT tbl,src_rowid,pk FROM backup_changelog WHERE id>? AND id<=?', (cl_lo, cl_hi)):
        touched.setdefault(tbl, {})[rid] = pk
    window = {}
    if st is None:  # first run: cover baseline -> trigger install gap
        have = set(tables(s))
        mx = lambda t: s.execute(f'SELECT coalesce(max(rowid),0) FROM {q(t)}').fetchone()[0]
        msg_lo = 1
        if 'messages' in have:
            msg_lo = max(1, first_rowid(s, 'messages', lambda c, r: (r[5] or '') >= since, 1, mx('messages') + 1) - 5000)
            window['messages'] = msg_lo

        def by_msg(c, r):
            m = c.execute('SELECT rowid FROM messages WHERE message_id=?', (r[2],)).fetchone()
            return m is None or m[0] >= msg_lo
        for t in ('reactions', 'attachments', 'mentions'):
            if t in have:
                window[t] = max(1, first_rowid(s, t, by_msg, 1, mx(t) + 1) - 2000)
        if 'ingest_tombstones' in have:
            window['ingest_tombstones'] = max(1, first_rowid(s, 'ingest_tombstones', lambda c, r: (r[12] or '') >= since, 1, mx('ingest_tombstones') + 1) - 100)
        if 'threads' in have:
            window['threads'] = max(1, first_rowid(s, 'threads', lambda c, r: (r[4] or '') >= since, 1, mx('threads') + 1) - 100)
    out = f'{outdir}/{gid}-{seq:06d}-{time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())}.sqlite'
    if os.path.exists(out):
        os.unlink(out)
    o = sqlite3.connect(out)
    o.execute('CREATE TABLE _meta(k TEXT PRIMARY KEY, v TEXT)')
    o.execute('CREATE TABLE _deleted(tbl TEXT, src_rowid INTEGER, pk TEXT)')
    stats = {}
    for t in tables(s):
        cs = cols(s, t)
        o.execute(f'CREATE TABLE {q(t)}(_rowid INTEGER PRIMARY KEY, {", ".join(q(x) for x in cs)})')
        ins = f'INSERT OR REPLACE INTO {q(t)} VALUES({",".join("?" * (len(cs) + 1))})'
        n = 0
        sel = f'SELECT rowid,{",".join(q(x) for x in cs)} FROM {q(t)}'
        if st is None:
            if t in window:
                rows = s.execute(sel + ' WHERE rowid>=?', (window[t],)).fetchall()
            else:
                rows = s.execute(sel).fetchall()  # small tables: whole table once
            o.executemany(ins, rows); n += len(rows)
            if t == 'threads' and 'messages' in window:  # threads touched by recent messages
                tids = [r[0] for r in o.execute('SELECT DISTINCT thread_id FROM messages')]
                for i in range(0, len(tids), 500):
                    ch = tids[i:i + 500]
                    rows = s.execute(sel + f' WHERE thread_id IN ({",".join("?" * len(ch))})', ch).fetchall()
                    o.executemany(ins, rows); n += len(rows)
        tc = touched.get(t, {})
        rids = list(tc)
        present = set()
        for i in range(0, len(rids), 500):
            ch = rids[i:i + 500]
            rows = s.execute(sel + f' WHERE rowid IN ({",".join("?" * len(ch))})', ch).fetchall()
            o.executemany(ins, rows); n += len(rows)
            present.update(r[0] for r in rows)
        gone = [(t, r, tc[r]) for r in rids if r not in present]
        o.executemany('INSERT INTO _deleted VALUES(?,?,?)', gone)
        stats[t] = (n, len(gone))
    meta = dict(gid=gid, seq=seq, cl_lo=cl_lo, cl_hi=cl_hi, created=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                first=int(st is None), since=since or '', window=json.dumps(window),
                msg_max_rowid=s.execute('SELECT coalesce(max(rowid),0) FROM messages').fetchone()[0])
    o.executemany('INSERT INTO _meta VALUES(?,?)', [(k, str(v)) for k, v in meta.items()])
    o.commit(); o.execute('VACUUM'); o.close()
    s.execute('COMMIT'); s.close()
    subprocess.run(['zstd', '-q', '-19', '--rm', '-f', out], check=True)
    json.dump(dict(seq=seq, cl_hi=cl_hi, file=os.path.basename(out) + '.zst'), open(sf + '.pending', 'w'))
    nz = {k: v for k, v in stats.items() if v != (0, 0)}
    print(f'export {gid} seq={seq} changelog=({cl_lo},{cl_hi}] size={os.path.getsize(out + ".zst")}B rows/deleted={nz} window={window}')


def commit(db):
    gid = gid_of(db)
    sf = f'{STATE}/{gid}.json'
    p = json.load(open(sf + '.pending'))
    os.replace(sf + '.pending', sf)
    c = sqlite3.connect(db, timeout=BUSY_MS / 1000, isolation_level=None)
    c.execute(f'PRAGMA busy_timeout={BUSY_MS}')
    n = c.execute("DELETE FROM backup_changelog WHERE id<=? AND at<datetime('now','-7 days')", (p['cl_hi'],)).rowcount
    print(f'commit {gid} seq={p["seq"]} cl_hi={p["cl_hi"]} pruned_changelog={n}')


if __name__ == '__main__':
    a = sys.argv[1:]
    if a[0] == 'install':
        install(a[1])
    elif a[0] == 'export':
        export(a[1], a[2], a[4] if len(a) > 4 and a[3] == '--since' else None)
    elif a[0] == 'commit':
        commit(a[1])
    else:
        sys.exit(__doc__)
