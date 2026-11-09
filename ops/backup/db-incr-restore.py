#!/root/v2/venv/bin/python
"""Apply incremental files onto a baseline copy:  db-incr-restore.py TARGET.db INCR1.sqlite[.zst] ...
Files are applied in seq order. Per file: deletes (by primary key, else rowid), then insert-or-replace of
every exported row with its original rowid. Idempotent (re-applying overlapping files is safe)."""
import sqlite3, sys, os, subprocess, tempfile, re

def q(n): return '"' + n.replace('"', '""') + '"'

def seqkey(p):
    m = re.search(r'-(\d{6})-', os.path.basename(p)); return int(m.group(1)) if m else 0

def main(target, files):
    c = sqlite3.connect(target, isolation_level=None)
    c.execute('PRAGMA busy_timeout=5000')
    for f in sorted(files, key=seqkey):
        tmp = None
        if f.endswith('.zst'):
            fd, tmp = tempfile.mkstemp(suffix='.sqlite', dir=os.path.dirname(os.path.abspath(target))); os.close(fd)
            subprocess.run(['zstd', '-q', '-d', '-f', f, '-o', tmp], check=True); src = tmp
        else:
            src = f
        c.execute('ATTACH ? AS inc', (src,))
        c.execute('BEGIN')
        tgt = {r[0] for r in c.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
        inc = [r[0] for r in c.execute("SELECT name FROM inc.sqlite_master WHERE type='table' AND name NOT IN ('_meta','_deleted')")]
        ups = dels = 0
        for tbl, rid, pk in c.execute('SELECT tbl,src_rowid,pk FROM inc._deleted').fetchall():
            if tbl not in tgt: continue
            pkc = [r[1] for r in c.execute(f'PRAGMA main.table_info({q(tbl)})') if r[5]]
            if pk is not None and len(pkc) == 1:
                dels += c.execute(f'DELETE FROM main.{q(tbl)} WHERE {q(pkc[0])}=?', (pk,)).rowcount
            else:
                dels += c.execute(f'DELETE FROM main.{q(tbl)} WHERE rowid=?', (rid,)).rowcount
        for t in inc:
            cs = [r[1] for r in c.execute(f'PRAGMA inc.table_info({q(t)})')][1:]
            if t not in tgt:  # table created after baseline
                ddl = c.execute("SELECT sql FROM inc.sqlite_master WHERE name=?", (t,)).fetchone()
                c.execute(f'CREATE TABLE main.{q(t)}({", ".join(q(x) for x in cs)})'); tgt.add(t)
            cl = ",".join(q(x) for x in cs)
            ups += c.execute(f'INSERT OR REPLACE INTO main.{q(t)}(rowid,{cl}) SELECT _rowid,{cl} FROM inc.{q(t)}').rowcount
        c.execute('COMMIT'); c.execute('DETACH inc')
        if tmp: os.unlink(tmp)
        print(f'applied {os.path.basename(f)}: upserts={ups} deletes={dels}')

if __name__ == '__main__':
    if len(sys.argv) < 3: sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
