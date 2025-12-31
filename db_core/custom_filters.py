import sqlite3
import json as _json
from datetime import datetime as _dt

from .config import DB_FILE
from .sql_helpers import _table_cols, _table_schema, _default_for_sqlite_type


def create_custom_filter(
    slug: str,
    name_: str,
    description: str = "",
    params: dict | None = None,
    global_enabled: bool = True,
    rule_kind: str = "generic",
    rule_code: str | None = None,
):
    schema = _table_schema("custom_filters")
    colmap = {c["name"]: c for c in schema}
    if "slug" not in colmap or "name" not in colmap:
        raise RuntimeError("custom_filters table is missing required columns (slug/name)")

    insert_cols, values = [], []

    def add(col: str, val):
        insert_cols.append(col)
        values.append(val)

    for col in schema:
        name = col["name"]
        if name == "id":
            continue
        if name == "slug":
            add("slug", slug)
        elif name == "name":
            add("name", name_.strip())
        elif name == "description":
            add("description", (description or "").strip())
        elif name == "params":
            add("params", _json.dumps(params or {}, ensure_ascii=False))
        elif name == "global_enabled":
            add("global_enabled", 1 if global_enabled else 0)
        elif name == "rule_kind":
            add("rule_kind", (rule_kind or "generic"))
        elif name == "rule_code":
            add("rule_code", rule_code or "")
        elif name == "created_at":
            if col["notnull"] and col["dflt_value"] is None:
                add("created_at", _dt.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                pass
        else:
            if col["notnull"] and col["dflt_value"] is None:
                add(name, _default_for_sqlite_type(col["type"]))
            else:
                pass

    placeholders = ", ".join(["?"] * len(values))
    sql = (
        f"INSERT INTO custom_filters ({', '.join(insert_cols)}) VALUES ({placeholders}) "
        "ON CONFLICT(slug) DO UPDATE SET "
        "name=excluded.name, description=excluded.description"
    )
    if "global_enabled" in colmap:
        sql += ", global_enabled=excluded.global_enabled"
    if "params" in colmap:
        sql += ", params=excluded.params"
    if "rule_kind" in colmap:
        sql += ", rule_kind=excluded.rule_kind"
    if "rule_code" in colmap:
        sql += ", rule_code=excluded.rule_code"

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(sql, values)
    conn.commit()
    conn.close()


def list_all_custom_filters():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    cols = _table_cols("custom_filters")
    sel = "id, slug, name, description"
    if "global_enabled" in cols:
        sel += ", global_enabled"
    if "params" in cols:
        sel += ", params"
    if "rule_kind" in cols:
        sel += ", rule_kind"
    c.execute(f"SELECT {sel} FROM custom_filters ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()

    idx = {k: i for i, k in enumerate(sel.replace(" ", "").split(","))}
    out = []
    for r in rows:
        item = {
            "id": r[idx["id"]],
            "slug": r[idx["slug"]],
            "name": r[idx["name"]],
            "description": r[idx["description"]],
        }
        if "global_enabled" in idx:
            item["global_enabled"] = bool(r[idx["global_enabled"]])
        if "params" in idx:
            raw = r[idx["params"]]
            try:
                item["params"] = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                item["params"] = {}
        if "rule_kind" in idx:
            item["rule_kind"] = r[idx["rule_kind"]] or "generic"
        out.append(item)
    return out


def get_filter_by_slug(slug: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT id, slug, name, description, global_enabled, params FROM custom_filters WHERE slug=?",
        (slug,),
    )
    r = c.fetchone()
    conn.close()
    if not r:
        return None
    return {
        "id": r[0],
        "slug": r[1],
        "name": r[2],
        "description": r[3],
        "global_enabled": bool(r[4]),
        "params": r[5],
    }


def update_custom_filter(slug: str, **fields):
    allowed = {"name", "description", "params", "global_enabled", "rule_kind", "rule_code"}
    cols = _table_cols("custom_filters")
    sets, vals = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k not in cols:
            continue
        if k == "params" and isinstance(v, (dict, list)):
            v = _json.dumps(v, ensure_ascii=False)
        if k == "global_enabled":
            v = 1 if bool(v) else 0
        if k == "rule_kind":
            v = v or "generic"
        sets.append(f"{k}=?")
        vals.append(v)
    if not sets:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"UPDATE custom_filters SET {', '.join(sets)} WHERE slug=?", (*vals, slug))
    conn.commit()
    conn.close()


def assign_custom_filter(bot_id: str, telegram_id: int, slug: str, enabled: bool = True):
    f = get_filter_by_slug(slug)
    if not f:
        raise ValueError("Unknown filter slug")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO user_custom_filters (bot_id, telegram_id, filter_id, enabled) VALUES (?, ?, ?, ?)",
        (bot_id, telegram_id, f["id"], 1 if enabled else 0),
    )
    c.execute(
        "UPDATE user_custom_filters SET enabled=? WHERE bot_id=? AND telegram_id=? AND filter_id=?",
        (1 if enabled else 0, bot_id, telegram_id, f["id"]),
    )
    conn.commit()
    conn.close()


def unassign_custom_filter(bot_id: str, telegram_id: int, slug: str):
    f = get_filter_by_slug(slug)
    if not f:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "DELETE FROM user_custom_filters WHERE bot_id=? AND telegram_id=? AND filter_id=?",
        (bot_id, telegram_id, f["id"]),
    )
    conn.commit()
    conn.close()


def toggle_user_custom_filter(bot_id: str, telegram_id: int, slug: str, enabled: bool):
    assign_custom_filter(bot_id, telegram_id, slug, enabled)


def list_user_custom_filters(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT cf.slug, cf.name, cf.description, cf.global_enabled, ucf.enabled, cf.params
        FROM custom_filters cf
        JOIN user_custom_filters ucf ON ucf.filter_id = cf.id
        WHERE ucf.bot_id = ? AND ucf.telegram_id = ?
        ORDER BY cf.id ASC
    """,
        (bot_id, telegram_id),
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            "slug": r[0],
            "name": r[1],
            "description": r[2],
            "global_enabled": bool(r[3]),
            "user_enabled": bool(r[4]),
            "params": r[5],
        }
        for r in rows
    ]
