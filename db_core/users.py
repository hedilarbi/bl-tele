import sqlite3
import json
from datetime import datetime

from .config import DB_FILE


def upsert_user_from_bot(bot_id: str, user_obj: dict, chat_obj: dict | None = None):
    if not user_obj or "id" not in user_obj:
        return
    uid = int(user_obj["id"])

    first = user_obj.get("first_name") or None
    last = user_obj.get("last_name") or None
    uname = user_obj.get("username") or None
    lang = user_obj.get("language_code") or None
    prem = 1 if user_obj.get("is_premium") else 0

    chat_obj = chat_obj or {}
    chat_type = chat_obj.get("type") or None
    chat_id = chat_obj.get("id") or None
    chat_title = chat_obj.get("title") or None

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute(
        "INSERT OR IGNORE INTO users(bot_id, telegram_id, active) VALUES (?, ?, 1)",
        (bot_id, uid),
    )
    cur.execute(
        """
        UPDATE users
           SET tg_first_name = ?,
               tg_last_name  = ?,
               tg_username   = ?,
               tg_lang       = ?,
               tg_is_premium = ?,
               tg_last_seen  = ?,
               tg_chat_type  = ?,
               tg_chat_id    = ?,
               tg_chat_title = ?,
               tg_first_seen = COALESCE(tg_first_seen, ?)
         WHERE bot_id = ? AND telegram_id = ?
    """,
        (first, last, uname, lang, prem, now, chat_type, chat_id, chat_title, now, bot_id, uid),
    )

    conn.commit()
    conn.close()


def add_user(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT default_timezone FROM bot_instances WHERE bot_id = ?", (bot_id,))
    row = c.fetchone()
    tz = row[0] if row and row[0] else "UTC"
    c.execute(
        "INSERT OR IGNORE INTO users (bot_id, telegram_id, token, filters, timezone, token_status) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (bot_id, telegram_id, None, "{}", tz, "unknown"),
    )
    c.execute(
        "UPDATE users SET timezone = ? WHERE bot_id = ? AND telegram_id = ? "
        "AND (timezone IS NULL OR timezone = '' OR timezone = 'UTC')",
        (tz, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def update_token(
    bot_id: str,
    telegram_id: int,
    token: str,
    headers: dict | None = None,
    auth_meta: dict | None = None,
):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    updates = [
        "token = ?",
        "token_status = 'unknown'",
        "cache_version = COALESCE(cache_version, 0) + 1",
    ]
    params = [token]

    if headers is not None:
        try:
            headers_json = json.dumps(headers, ensure_ascii=True)
        except Exception:
            headers_json = None
        updates.append("mobile_headers = ?")
        params.append(headers_json)

    if auth_meta is not None:
        try:
            auth_json = json.dumps(auth_meta, ensure_ascii=True) if auth_meta else None
        except Exception:
            auth_json = None
        updates.append("mobile_auth_json = ?")
        params.append(auth_json)

    params.extend([bot_id, telegram_id])
    c.execute(
        f"UPDATE users SET {', '.join(updates)} WHERE bot_id = ? AND telegram_id = ?",
        tuple(params),
    )
    conn.commit()
    conn.close()


def set_token_status(bot_id: str, telegram_id: int, status: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET token_status = ? WHERE bot_id = ? AND telegram_id = ?",
        (status, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def update_portal_token(bot_id: str, telegram_id: int, token: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET portal_token = ? WHERE bot_id = ? AND telegram_id = ?",
        (token, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_portal_token(bot_id: str, telegram_id: int) -> str | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT portal_token FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def get_mobile_headers(bot_id: str, telegram_id: int) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT mobile_headers FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        val = json.loads(row[0])
        return val if isinstance(val, dict) else None
    except Exception:
        return None


def get_mobile_auth(bot_id: str, telegram_id: int) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT mobile_auth_json FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        val = json.loads(row[0])
        return val if isinstance(val, dict) else None
    except Exception:
        return None


def get_token_status(bot_id: str, telegram_id: int) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token_status FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else "unknown"


def update_filters(bot_id: str, telegram_id: int, filters_json: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users "
        "SET filters = ?, cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id = ? AND telegram_id = ?",
        (filters_json, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT bot_id, telegram_id, token, filters, active FROM users")
    users = c.fetchall()
    conn.close()
    return users


def get_all_users_with_bot_admin_active():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT
                u.bot_id,
                u.telegram_id,
                u.token,
                u.filters,
                u.active,
                COALESCE(b.admin_active, 0),
                COALESCE(u.cache_version, 0)
            FROM users u
            LEFT JOIN bot_instances b ON b.bot_id = u.bot_id
            WHERE COALESCE(b.role, 'user') != 'admin'
              AND COALESCE(u.active, 0) = 1
              AND COALESCE(b.admin_active, 0) = 1
        """
        )
        users = c.fetchall()
    except sqlite3.OperationalError as e:
        # Backward-compatible fallback for old DBs not yet migrated with users.cache_version
        if "cache_version" not in str(e).lower():
            conn.close()
            raise
        c.execute(
            """
            SELECT
                u.bot_id,
                u.telegram_id,
                u.token,
                u.filters,
                u.active,
                COALESCE(b.admin_active, 0),
                0 AS cache_version
            FROM users u
            LEFT JOIN bot_instances b ON b.bot_id = u.bot_id
            WHERE COALESCE(b.role, 'user') != 'admin'
              AND COALESCE(u.active, 0) = 1
              AND COALESCE(b.admin_active, 0) = 1
        """
        )
        users = c.fetchall()
    conn.close()
    return users


def get_user_row(bot_id: str, telegram_id: int) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT
            token, filters, active, timezone, token_status,
            notify_accepted, notify_not_accepted, notify_rejected,
            bl_email, bl_password, portal_token, bl_uuid,
            tg_first_name, tg_last_name, tg_username, tg_lang, tg_is_premium
        FROM users
        WHERE bot_id = ? AND telegram_id = ?
        LIMIT 1
    """,
        (bot_id, telegram_id),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "token": row[0],
        "filters": row[1],
        "active": bool(row[2]),
        "timezone": row[3],
        "token_status": row[4],
        "notify_accepted": bool(row[5]) if row[5] is not None else True,
        "notify_not_accepted": bool(row[6]) if row[6] is not None else True,
        "notify_rejected": bool(row[7]) if row[7] is not None else True,
        "bl_email": row[8],
        "bl_password": row[9],
        "portal_token": row[10],
        "bl_uuid": row[11],
        "tg_first_name": row[12],
        "tg_last_name": row[13],
        "tg_username": row[14],
        "tg_lang": row[15],
        "tg_is_premium": bool(row[16]) if row[16] is not None else False,
    }


def get_active(bot_id: str, telegram_id: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT active FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def set_active(bot_id: str, telegram_id: int, active: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users "
        "SET active = ?, cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id = ? AND telegram_id = ?",
        (1 if active else 0, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_user_timezone(bot_id: str, telegram_id: int) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT timezone FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else "UTC"


def set_user_timezone(bot_id: str, telegram_id: int, tz: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users "
        "SET timezone = ?, cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id = ? AND telegram_id = ?",
        (tz, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_notifications(bot_id: str, telegram_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT 
            COALESCE(notify_accepted,1),
            COALESCE(notify_not_accepted,1),
            COALESCE(notify_rejected,1)
        FROM users WHERE bot_id = ? AND telegram_id = ?
    """,
        (bot_id, telegram_id),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return {"accepted": True, "not_accepted": True, "rejected": True}
    return {
        "accepted": bool(row[0]),
        "not_accepted": bool(row[1]),
        "rejected": bool(row[2]),
    }


def set_notification(bot_id: str, telegram_id: int, kind: str, enabled: bool):
    colmap = {
        "accepted": "notify_accepted",
        "not_accepted": "notify_not_accepted",
        "rejected": "notify_rejected",
    }
    col = colmap.get(kind)
    if not col:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        f"UPDATE users SET {col} = ? WHERE bot_id = ? AND telegram_id = ?",
        (1 if enabled else 0, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def set_bl_account(bot_id: str, telegram_id: int, email: str, password: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users "
        "SET bl_email=?, bl_password=?, cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id=? AND telegram_id=?",
        (email.strip(), password.strip(), bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_bl_account(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT bl_email, bl_password FROM users WHERE bot_id=? AND telegram_id=?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return {"email": None, "has_password": False}
    return {"email": row[0], "has_password": bool(row[1])}


def get_bl_account_full(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT bl_email, bl_password FROM users WHERE bot_id=? AND telegram_id=?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return None, None
    email, password = row[0], row[1]
    if not email or not password:
        return None, None
    return email, password


def set_bl_uuid(bot_id: str, telegram_id: int, bl_uuid: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users "
        "SET bl_uuid = ?, cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id = ? AND telegram_id = ?",
        (bl_uuid, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_bl_uuid(bot_id: str, telegram_id: int) -> str | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT bl_uuid FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None
