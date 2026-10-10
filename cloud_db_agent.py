#!/usr/bin/env python3
"""Remote SQLite execution agent for Dixo's cloud-DB bridge.

Deployed on the production host next to ``app.py``. It reads newline
delimited JSON requests on stdin, runs them against SQLite files that live
under ``CLOUD_DB_AGENT_ROOT`` and writes newline delimited JSON responses on
stdout. It speaks nothing else -- no shell, no arbitrary file access.

Why a bridge at all
-------------------
SQLite has no network protocol: a client cannot open ``host:/path/db`` the way
it would with MySQL or Postgres. The only ways to read a remote SQLite file are
a network filesystem (SQLite locking over NFS/SMB is unsafe) or a query proxy
like this one. This proxy keeps SQLite running on the machine that owns the
file and only ships rows, so large scans stay on the server.

Safety
------
* Every path is resolved with ``realpath`` and confined to the agent root.
* **Read-only by default.** Writes require ``CLOUD_DB_AGENT_ALLOW_WRITE=1`` in
  the server environment *and* a per-connection ``write`` flag from the client.
  Read-only connections additionally run with ``PRAGMA query_only=1`` so the
  SQLite engine itself refuses to modify the file.
* Large result sets are capped by ``CLOUD_DB_AGENT_MAX_ROWS`` and flagged as
  truncated instead of exhausting memory.
"""

import base64
import json
import os
import sqlite3
import sys

ROOT = os.path.realpath(os.environ.get("CLOUD_DB_AGENT_ROOT", "/root/v2/V20"))
ALLOW_WRITE = os.environ.get("CLOUD_DB_AGENT_ALLOW_WRITE", "0") == "1"
MAX_ROWS = int(os.environ.get("CLOUD_DB_AGENT_MAX_ROWS", "500000"))

_connections = {}
_next_id = 0


def _encode(value):
    """JSON cannot carry bytes/NaN/Inf; tag those with a dict sentinel.

    SQLite column values are only NULL/INTEGER/REAL/TEXT/BLOB, so any dict in
    the wire format is one of our sentinels and can never collide with data.
    Python ints survive ``json`` exactly (unlike JS), so snowflake IDs stay
    lossless.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__b64__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, float):
        if value != value:
            return {"__num__": "nan"}
        if value == float("inf"):
            return {"__num__": "inf"}
        if value == float("-inf"):
            return {"__num__": "-inf"}
    return value


def _resolve(path):
    real = os.path.realpath(path)
    if real != ROOT and not real.startswith(ROOT + os.sep):
        raise PermissionError("path outside agent root: %s" % real)
    return real


def _open(request):
    global _next_id
    real = _resolve(request["path"])
    write = bool(ALLOW_WRITE and request.get("write"))
    if not os.path.exists(real):
        raise FileNotFoundError(real)
    uri = "file:%s?mode=%s" % (real, "rw" if write else "ro")
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.execute("PRAGMA busy_timeout=60000")
    if write:
        connection.execute("PRAGMA journal_mode=WAL")
    else:
        connection.execute("PRAGMA query_only=1")
    _next_id += 1
    _connections[_next_id] = connection
    return {"conn": _next_id, "write": write, "path": real}


def _cursor_payload(cursor):
    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = []
    truncated = False
    if columns:
        for index, row in enumerate(cursor):
            if index >= MAX_ROWS:
                truncated = True
                break
            rows.append([_encode(value) for value in row])
    return {
        "columns": columns,
        "rows": rows,
        "rowcount": cursor.rowcount,
        "lastrowid": cursor.lastrowid,
        "truncated": truncated,
    }


def _handle(request):
    op = request.get("op")
    if op == "ping":
        return {"pong": True, "root": ROOT, "allow_write": ALLOW_WRITE,
                "sqlite": sqlite3.sqlite_version}
    if op == "stat":
        real = _resolve(request["path"])
        if not os.path.exists(real):
            return {"exists": False, "path": real}
        info = os.stat(real)
        return {"exists": True, "path": real,
                "mtime_ns": info.st_mtime_ns, "size": info.st_size}
    if op == "open":
        return _open(request)

    connection = _connections.get(request.get("conn"))
    if connection is None:
        raise RuntimeError("unknown connection %r" % request.get("conn"))

    if op == "execute":
        cursor = connection.execute(request["sql"], request.get("params") or [])
        payload = _cursor_payload(cursor)
        cursor.close()
        return payload
    if op == "executemany":
        cursor = connection.executemany(request["sql"], request.get("seq") or [])
        payload = {"rowcount": cursor.rowcount, "lastrowid": cursor.lastrowid}
        cursor.close()
        return payload
    if op == "executescript":
        connection.executescript(request["sql"])
        return {"rowcount": -1}
    if op == "commit":
        connection.commit()
        return {}
    if op == "rollback":
        connection.rollback()
        return {}
    if op == "close":
        connection.close()
        _connections.pop(request.get("conn"), None)
        return {}
    raise ValueError("unsupported op %r" % op)


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except ValueError as exc:
            out.write(json.dumps({"id": None, "ok": False,
                                  "error": {"type": "ProtocolError", "message": str(exc)}}) + "\n")
            out.flush()
            continue
        try:
            result = _handle(request)
            response = {"id": request.get("id"), "ok": True, "result": result}
        except Exception as exc:  # noqa: BLE001 - every failure goes back to the client
            response = {"id": request.get("id"), "ok": False,
                        "error": {"type": type(exc).__name__, "message": str(exc)}}
        out.write(json.dumps(response) + "\n")
        out.flush()


if __name__ == "__main__":
    main()
