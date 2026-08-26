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

