"""Read production SQLite databases over SSH, without copying them.

SQLite has no network protocol, so a client cannot open ``host:/path/db``.
This module ships a small ``sqlite3``-compatible adapter that forwards
``execute``/``executemany``/``executescript``/``commit``/``rollback`` to
``cloud_db_agent.py`` running on the production host (see that file for the
wire protocol and the safety rules). SQLite itself keeps running next to the
database file, so large scans never cross the wire -- only result rows do.

Design points
-------------
* **One SSH process, many logical connections.** Spawning ``ssh`` costs
  seconds, so a single long-lived transport is shared and each caller gets a
  cheap logical connection id. A lock serialises requests on that transport,
  which is fine for a development instance and keeps the code simple.
* **Read-only unless explicitly told otherwise.** ``CLOUD_DB_READONLY``
  defaults to 1 and the server-side agent has its own write gate, so two
  deliberate switches are needed before a local instance can modify
  production data.
* **Errors surface as real ``sqlite3`` exceptions** so existing ``except
  sqlite3.OperationalError`` handling in the app keeps working.

Enable with::

    CLOUD_DB=1
    CLOUD_DB_SSH_TARGET=root@<host>
    CLOUD_DB_SSH_KEY=C:\\path\\to\\id_key        # optional
    CLOUD_DB_REMOTE_ROOT=/root/v2/V20            # default
    CLOUD_DB_LOCAL_ROOT=<repo dir>               # default: repo root
"""

import base64
import json
import os
import sqlite3
import subprocess
import threading

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_DEFAULT_REMOTE_ROOT = "/root/v2/V20"
_SENTINELS = {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}


class CloudDbError(RuntimeError):
    """Transport or protocol failure talking to the remote SQLite agent."""


def cloud_enabled():
    return os.getenv("CLOUD_DB", "0") == "1"


def _remote_path(local_path):
    """Map a local database path to its path on the production host.

    Returns ``None`` for anything outside ``CLOUD_DB_LOCAL_ROOT`` -- callers
    then fall back to a normal local SQLite file, which keeps unrelated
    databases (caches, scratch files) local.
    """
    local_root = os.path.abspath(os.getenv("CLOUD_DB_LOCAL_ROOT") or BASE_DIR)
    remote_root = (os.getenv("CLOUD_DB_REMOTE_ROOT") or _DEFAULT_REMOTE_ROOT).rstrip("/")
    target = os.path.abspath(os.fspath(local_path))
    if target == local_root:
        relative = ""
    elif target.startswith(local_root + os.sep):
        relative = os.path.relpath(target, local_root)
    else:
        return None
    relative = relative.replace(os.sep, "/")
    return remote_root + "/" + relative if relative else remote_root


def _decode(value):
    if isinstance(value, dict):
        if "__b64__" in value:
            return base64.b64decode(value["__b64__"])
        if "__num__" in value:
            return _SENTINELS[value["__num__"]]
    return value


def _error_class(name):
    return {
        "OperationalError": sqlite3.OperationalError,
        "IntegrityError": sqlite3.IntegrityError,
        "ProgrammingError": sqlite3.ProgrammingError,
        "DatabaseError": sqlite3.DatabaseError,
        "InterfaceError": sqlite3.InterfaceError,
        "DataError": sqlite3.DataError,
        "NotSupportedError": sqlite3.NotSupportedError,
    }.get(name, CloudDbError)


class _Transport:
    """A single ``ssh`` subprocess speaking newline-delimited JSON."""

    def __init__(self):
        self._lock = threading.RLock()
        self._process = None
        self._counter = 0

    def _argv(self):
        target = os.getenv("CLOUD_DB_SSH_TARGET", "").strip()
        if not target:
            raise CloudDbError("CLOUD_DB=1 需要设置 CLOUD_DB_SSH_TARGET（例如 root@1.2.3.4）")
        argv = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                "-o", "ConnectTimeout=15"]
        key = os.getenv("CLOUD_DB_SSH_KEY", "").strip()
        if key:
            argv += ["-i", key]
        port = os.getenv("CLOUD_DB_SSH_PORT", "").strip()
        if port:
            argv += ["-p", port]
        extra = os.getenv("CLOUD_DB_SSH_EXTRA", "").strip()
        if extra:
            argv += extra.split()
        remote_root = os.getenv("CLOUD_DB_REMOTE_ROOT") or _DEFAULT_REMOTE_ROOT
        python = os.getenv("CLOUD_DB_REMOTE_PYTHON") or "python3"
        agent = os.getenv("CLOUD_DB_AGENT") or (remote_root.rstrip("/") + "/cloud_db_agent.py")
        if not python.strip() or not agent.strip():
            raise CloudDbError("CLOUD_DB_REMOTE_PYTHON / CLOUD_DB_AGENT 不能为空")
        # 生产端 agent 自带写开关（默认拒绝）。本地要写，必须显式把
        # CLOUD_DB_AGENT_ALLOW_WRITE=1 传给它，本地再配合 CLOUD_DB_READONLY=0。
        env_prefix = "CLOUD_DB_AGENT_ROOT=%s" % remote_root
        if os.getenv("CLOUD_DB_AGENT_ALLOW_WRITE") == "1":
            env_prefix += " CLOUD_DB_AGENT_ALLOW_WRITE=1"
        remote = "%s exec %s -u %s" % (env_prefix, python, agent)
        argv += [target, remote]
        return argv

    def _spawn(self):
        argv = self._argv()
        try:
            self._process = subprocess.Popen(
                argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, text=True, encoding="utf-8",
                bufsize=1,
            )
        except OSError as exc:
            raise CloudDbError("无法启动 ssh 通道: %s（命令: %s）" % (exc, " ".join(argv)))

    def _terminate(self):
        process = self._process
        self._process = None
        if process is not None:
            try:
                process.kill()
            except OSError:
                pass

    def request(self, payload, retry=True):
        with self._lock:
            for attempt in (0, 1):
                if self._process is None or self._process.poll() is not None:
                    self._terminate()
                    self._spawn()
                self._counter += 1
                payload = dict(payload, id=self._counter)
                try:
                    self._process.stdin.write(json.dumps(payload) + "\n")
                    self._process.stdin.flush()
                    line = self._process.stdout.readline()
                except (OSError, ValueError) as exc:
                    line = ""
                    if attempt == 0 and retry:
                        self._terminate()
                        continue
                    raise CloudDbError("云端数据库通道写入失败: %s" % exc)
                if not line:
                    if attempt == 0 and retry:
                        self._terminate()
                        continue
                    raise CloudDbError(
                        "云端数据库通道没有响应（检查 %s 是否可用，命令: %s）"
                        % (os.getenv("CLOUD_DB_SSH_TARGET", ""), " ".join(self._argv())))
                response = json.loads(line)
                if not response.get("ok"):
                    error = response.get("error") or {}
                    raise _error_class(error.get("type", ""))(
                        error.get("message") or "云端数据库执行失败")
                return response.get("result") or {}
            raise CloudDbError("云端数据库通道不可用")


_transport = None
_transport_lock = threading.Lock()


def _get_transport():
    global _transport
    with _transport_lock:
        if _transport is None:
            _transport = _Transport()
        return _transport


class CloudRow(dict):
    """Same dual access as ``sqlite3.Row``: positional and by (any-case) name."""

    __slots__ = ("_keys", "_values")

    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._keys = list(columns)
        self._values = list(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            lowered = key.lower() if isinstance(key, str) else key
            for existing in self._keys:
                if isinstance(existing, str) and existing.lower() == lowered:
                    return dict.__getitem__(self, existing)
            raise

    def keys(self):
        return list(self._keys)

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def __repr__(self):
        return "<CloudRow %r>" % (dict(zip(self._keys, self._values)),)


class CloudCursor:
    """Mimics the ``sqlite3.Cursor`` subset this project uses."""

    def __init__(self, columns, rows, row_factory, rowcount=None, lastrowid=None,
                 truncated=False, write=False, connection=None):
        self.description = [(name, None, None, None, None, None, None) for name in columns]
        self._columns = list(columns)
        self._rows = rows
        self._row_factory = row_factory
        self._index = 0
        self.rowcount = rowcount if rowcount is not None else len(rows)
        self.lastrowid = lastrowid
        self.truncated = truncated
        self.write = write
        self._connection = connection

    # 真实 sqlite3 的 Cursor 自己也能 execute()，app.py 的 DataEngine 就是这么用的
    # （``cur = conn.cursor(); cur.execute(...)``）。这里把结果并回自身并返回 self，
    # 同时兼容 ``cur.execute(...).fetchone()`` 和先 execute 再 fetch 两种写法。
    def _adopt(self, other):
        self.description = other.description
        self._columns = other._columns
        self._rows = other._rows
        self._index = 0
        self.rowcount = other.rowcount
        self.lastrowid = other.lastrowid
        self.truncated = other.truncated
        self.write = other.write
        return self

    def _need_connection(self):
        if self._connection is None:
            raise sqlite3.ProgrammingError("Cursor 未绑定连接，无法执行语句")
        return self._connection

    def execute(self, sql, params=()):
        return self._adopt(self._need_connection().execute(sql, params))

    def executemany(self, sql, seq_of_params):
        return self._adopt(self._need_connection().executemany(sql, seq_of_params))

    def executescript(self, script):
        return self._adopt(self._need_connection().executescript(script))

    def _shape(self, raw):
        if raw is None:
            return None
        if self._row_factory is False:
            return tuple(raw)
        return CloudRow(self._columns, raw)

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        raw = self._rows[self._index]
        self._index += 1
        return self._shape(raw)

    def fetchall(self):
        out = [self._shape(raw) for raw in self._rows[self._index:]]
        self._index = len(self._rows)
        return out

    def fetchmany(self, size=1):
        chunk = self._rows[self._index:self._index + size]
        self._index += len(chunk)
        return [self._shape(raw) for raw in chunk]

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    def close(self):
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class CloudConnection:
    """``sqlite3.Connection``-compatible handle to a remote database file."""

    def __init__(self, remote_path, row_factory=True, write=False):
        self.remote_path = remote_path
        self.row_factory = row_factory
        self._transport = _get_transport()
        opened = self._transport.request({"op": "open", "path": remote_path, "write": write})
        self._conn = opened["conn"]
        self.read_only = not opened.get("write")
        self._closed = False
        self._writes = 0

    # -------------------------------------------------------------- helpers
    def _request(self, payload):
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
        return self._transport.request(dict(payload, conn=self._conn))

    def _is_write(self, sql):
        head = sql.lstrip().upper()
        return head.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE",
                                "DROP", "ALTER", "VACUUM", "REINDEX"))

    # ------------------------------------------------------------ execution
    def execute(self, sql, params=()):
        result = self._request({"op": "execute", "sql": sql, "params": list(params or [])})
        write = self._is_write(sql)
        if write:
            self._writes += max(result.get("rowcount") or 0, 0)
        return CloudCursor(
            result.get("columns") or [], result.get("rows") or [],
            self.row_factory, rowcount=result.get("rowcount"),
            lastrowid=result.get("lastrowid"),
            truncated=result.get("truncated", False), write=write,
        )

    def executemany(self, sql, seq_of_params):
        items = [list(p) for p in seq_of_params]
        result = self._request({"op": "executemany", "sql": sql, "seq": items})
        self._writes += max(result.get("rowcount") or 0, 0)
        return CloudCursor([], [], self.row_factory, rowcount=result.get("rowcount"),
                           lastrowid=result.get("lastrowid"), write=True)

    def executescript(self, script):
        result = self._request({"op": "executescript", "sql": script})
        return CloudCursor([], [], self.row_factory, rowcount=result.get("rowcount", -1))

    def commit(self):
        self._request({"op": "commit"})

    def rollback(self):
        self._request({"op": "rollback"})

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._transport.request({"op": "close", "conn": self._conn})
        except Exception:  # noqa: BLE001 - closing must never raise
            pass

    def cursor(self):
        return CloudCursor([], [], self.row_factory, connection=self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    @property
    def total_changes(self):
        return self._writes


def maybe_connect(path, row_factory=True, write=None):
    """Return a cloud connection for ``path``, or ``None`` to use the local file.

    ``None`` is the normal answer whenever ``CLOUD_DB`` is off or the path is
    outside ``CLOUD_DB_LOCAL_ROOT``, so call sites can simply fall through.
    """
    if not cloud_enabled():
        return None
    remote = _remote_path(path)
    if remote is None:
        return None
    if write is None:
        # 空值一律按最安全的只读处理，避免手滑写出空字符串就打开了写权限。
        write = (os.getenv("CLOUD_DB_READONLY") or "1") != "1"
    return CloudConnection(remote, row_factory=row_factory, write=write)


def remote_stat(path):
    """Ask the production host whether the mapped database file exists.

    Returns ``None`` when cloud mode is off or the path is not cloud-mapped,
    otherwise a ``{"exists": bool, "path": str}`` dict.
    """
    if not cloud_enabled():
        return None
    remote = _remote_path(path)
    if remote is None:
        return None
    try:
        return _get_transport().request({"op": "stat", "path": remote})
    except CloudDbError:
        return {"exists": False, "path": remote}


def remote_exists(path):
    """True when the mapped remote database file exists on the production host."""
    stat = remote_stat(path)
    return None if stat is None else bool(stat.get("exists"))


def remote_file_stat(path):
    """``(mtime_ns, size)`` for a remote file, or ``None`` when it is absent.

    Cache-invalidation code needs the *origin* file's metadata; a local
    ``os.stat`` on a path that only exists on the production host always fails,
    which would silently pin every cache signature to zero and defeat caching.
    """
    stat = remote_stat(path)
    if not stat or not stat.get("exists"):
        return None
    return int(stat.get("mtime_ns") or 0), int(stat.get("size") or 0)
