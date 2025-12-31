import sqlite3

from .config import DB_FILE


def add_bot_instance(
    bot_id: str,
    bot_token: str,
    bot_name: str | None = None,
    role: str = "user",
    default_timezone: str | None = None,
    admin_active: bool | None = None,
):
    tz = (default_timezone or "UTC").strip() or "UTC"
    if admin_active is None:
        admin_active = True if role == "admin" else False
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO bot_instances (bot_id, bot_name, bot_token, role, admin_active, default_timezone)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_id) DO UPDATE SET
            bot_name = excluded.bot_name,
            bot_token = excluded.bot_token,
            role = excluded.role,
            admin_active = excluded.admin_active,
            default_timezone = excluded.default_timezone,
            updated_at = CURRENT_TIMESTAMP
    """,
        (bot_id, bot_name, bot_token, role, 1 if admin_active else 0, tz),
    )
    conn.commit()
    conn.close()


def list_bot_instances():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT bot_id, bot_name, bot_token, role, owner_telegram_id, admin_active, default_timezone
        FROM bot_instances
        ORDER BY bot_id ASC
    """
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            "bot_id": r[0],
            "bot_name": r[1],
            "bot_token": r[2],
            "role": r[3] or "user",
            "owner_telegram_id": r[4],
            "admin_active": bool(r[5]),
            "default_timezone": r[6] or "UTC",
        }
        for r in rows
    ]


def get_bot_instance(bot_id: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT bot_id, bot_name, bot_token, role, owner_telegram_id, admin_active, default_timezone
        FROM bot_instances
        WHERE bot_id = ?
        LIMIT 1
    """,
        (bot_id,),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "bot_id": row[0],
        "bot_name": row[1],
        "bot_token": row[2],
        "role": row[3] or "user",
        "owner_telegram_id": row[4],
        "admin_active": bool(row[5]),
        "default_timezone": row[6] or "UTC",
    }


def get_bot_token(bot_id: str) -> str | None:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT bot_token FROM bot_instances WHERE bot_id = ?", (bot_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def list_bots_for_user(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT bot_id, bot_name, bot_token, role, owner_telegram_id, admin_active, default_timezone
        FROM bot_instances
        WHERE owner_telegram_id = ?
        ORDER BY bot_id ASC
    """,
        (telegram_id,),
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            "bot_id": r[0],
            "bot_name": r[1],
            "bot_token": r[2],
            "role": r[3] or "user",
            "owner_telegram_id": r[4],
            "admin_active": bool(r[5]),
            "default_timezone": r[6] or "UTC",
        }
        for r in rows
    ]


def assign_bot_owner(bot_id: str, telegram_id: int) -> tuple[bool, str]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT owner_telegram_id FROM bot_instances WHERE bot_id = ?", (bot_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, "bot_not_found"
    if row[0] and int(row[0]) != int(telegram_id):
        conn.close()
        return False, "bot_already_owned"
    c.execute(
        """
        UPDATE bot_instances
        SET owner_telegram_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE bot_id = ?
    """,
        (int(telegram_id), bot_id),
    )
    conn.commit()
    conn.close()
    return True, "ok"


def set_bot_admin_active(bot_id: str, admin_active: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE bot_instances SET admin_active = ?, updated_at = CURRENT_TIMESTAMP WHERE bot_id = ?",
        (1 if admin_active else 0, bot_id),
    )
    conn.commit()
    conn.close()


def get_bot_admin_active(bot_id: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT admin_active FROM bot_instances WHERE bot_id = ?", (bot_id,))
    row = c.fetchone()
    conn.close()
    return bool(row[0]) if row and row[0] is not None else False
