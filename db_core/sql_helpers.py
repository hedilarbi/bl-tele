import sqlite3

from .config import DB_FILE


def _table_cols(table: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in c.fetchall()]
    conn.close()
    return set(cols)


def _table_schema(table: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    rows = c.fetchall()
    conn.close()
    return [
        {
            "name": r[1],
            "type": (r[2] or ""),
            "notnull": bool(r[3]),
            "dflt_value": r[4],
            "pk": bool(r[5]),
        }
        for r in rows
    ]


def _default_for_sqlite_type(type_str: str):
    t = (type_str or "").upper()
    if "INT" in t:
        return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return 0.0
    if "BLOB" in t:
        return b""
    return ""
