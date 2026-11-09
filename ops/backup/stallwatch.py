#!/usr/bin/python3
"""stallwatch.py [-s SECONDS] -- CMD ...
Run CMD; kill it (whole process group) if for SECONDS (default 10) in a row there is no progress:
no change in CPU time (utime+stime) and IO (rchar+wchar, includes network) summed over the process tree.
No total runtime limit. Exit code: CMD's, or 124 on stall."""
import os, sys, time, signal, subprocess

def tree(root):
    kids = {}
    for p in os.listdir('/proc'):
        if p.isdigit():
            try:
                f = open(f'/proc/{p}/stat').read(); ppid = int(f[f.rindex(')') + 2:].split()[1])
                kids.setdefault(ppid, []).append(int(p))
            except Exception:
                pass
    out, st = [], [root]
    while st:
        x = st.pop(); out.append(x); st += kids.get(x, [])
    return out

def progress(pids):
    tot = 0
    for p in pids:
        try:
            f = open(f'/proc/{p}/stat').read().rsplit(')', 1)[1].split(); tot += int(f[11]) + int(f[12])
            for line in open(f'/proc/{p}/io'):
                k, v = line.split(':')
                if k in ('rchar', 'wchar'): tot += int(v)
        except Exception:
            pass
    return tot

a = sys.argv[1:]; lim = 10.0
if a and a[0] == '-s': lim = float(a[1]); a = a[2:]
if a and a[0] == '--': a = a[1:]
p = subprocess.Popen(a, start_new_session=True)
last, since = None, time.monotonic()
while p.poll() is None:
    time.sleep(1)
    cur = progress(tree(p.pid))
    if cur != last:
        last, since = cur, time.monotonic()
    elif time.monotonic() - since >= lim:
        print(f'stallwatch: no CPU/IO progress for {lim:.0f}s, killing: {" ".join(a)[:200]}', file=sys.stderr)
        try: os.killpg(p.pid, signal.SIGKILL)
        except Exception: pass
        p.wait(); sys.exit(124)
sys.exit(p.returncode)
