"""SQLite connection and small migration helpers."""

import os
import sqlite3


def connect_sqlite(path, timeout=60, row_factory=True, create_parents=True, synchronous="NORMAL"):
    """Open a consistently configured SQLite connection.

    ``row_factory=False`` keeps the standard tuple rows for the importer,
    while the application and services use mapping-like ``sqlite3.Row`` rows.
    ``synchronous=None`` is accepted for callers that only want to set WAL and
    busy timeout without changing the existing database setting.
    """
    path = os.fspath(path)
    if create_parents:
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=timeout)
    if row_factory is not False:
        conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    # 大库读侧优化：调大 cache、内存临时表。
    # 注意：PRAGMA page_size 只对新建库生效，对现有库无效（现有库仍是 4K 页），
    # 所以这里不能写 PRAGMA page_size=16384，否则 SQLite 会静默忽略。
    conn.execute("PRAGMA cache_size=-200000")        # 200 MB
    conn.execute("PRAGMA temp_store=MEMORY")
    if synchronous is not None:
        conn.execute(f"PRAGMA synchronous={synchronous}")
    return conn


def add_columns(conn, table, columns):
    """Add missing columns, tolerating databases upgraded incrementally."""
    for column, definition in columns.items():
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


def is_missing_table_error(error):
    return isinstance(error, sqlite3.OperationalError) and "no such table" in str(error).lower()

