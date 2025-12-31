import json
import sqlite3
from typing import Optional

from db import DB_FILE


def _get_mobile_token(bot_id: str, user_id: int) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


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
    c.execute("UPDATE users SET active = ? WHERE bot_id = ? AND telegram_id = ?", (1 if active else 0, bot_id, telegram_id))
    conn.commit()
    conn.close()


def get_filters(bot_id: str, telegram_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT filters FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    row = c.fetchone()
    conn.close()
    return json.loads(row[0]) if row and row[0] else {}
