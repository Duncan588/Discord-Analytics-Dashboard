"""SQLite 连接与轻量迁移工具。

网站、下载器和机器人并发读写同一批数据库文件，因此连接参数（WAL、
busy_timeout）必须一致，否则会出现随机的 database is locked。
"""
import os
import sqlite3


def connect_sqlite(path, timeout=60, row_factory=True, synchronous="NORMAL", create_parents=False):
    """按项目统一的并发参数打开 SQLite 数据库。"""
    path = os.fspath(path)
    if create_parents:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=timeout)
    if row_factory:
        conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA journal_mode=WAL")
    if synchronous:
        conn.execute(f"PRAGMA synchronous={synchronous}")
    return conn


def add_columns(conn, table, columns):
    """为已存在的表补充缺失列；已经存在的列会被 SQLite 拒绝，直接忽略。"""
    for column, definition in columns.items():
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError:
            pass
