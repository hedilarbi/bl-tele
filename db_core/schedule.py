import sqlite3
from datetime import datetime

from .config import DB_FILE


def get_blocked_days(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT id, day FROM blocked_days
        WHERE bot_id = ? AND telegram_id = ?
        ORDER BY day ASC
    """,
        (bot_id, telegram_id),
    )
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "day": r[1]} for r in rows]


def add_blocked_day(bot_id: str, telegram_id: int, day_str: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        INSERT OR IGNORE INTO blocked_days (bot_id, telegram_id, day)
        VALUES (?, ?, ?)
    """,
        (bot_id, telegram_id, day_str),
    )
    c.execute(
        "UPDATE users SET cache_version = COALESCE(cache_version, 0) + 1 WHERE bot_id = ? AND telegram_id = ?",
        (bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def delete_blocked_day(bot_id: str, day_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET cache_version = COALESCE(cache_version, 0) + 1 "
        "WHERE bot_id = ? AND telegram_id = (SELECT telegram_id FROM blocked_days WHERE id = ? AND bot_id = ?)",
        (bot_id, day_id, bot_id),
    )
    c.execute("DELETE FROM blocked_days WHERE id = ? AND bot_id = ?", (day_id, bot_id))
    conn.commit()
    conn.close()


def prune_blocked_days() -> int:
    """Delete blocked days that are strictly in the past. Returns count deleted."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, day FROM blocked_days")
    rows = c.fetchall()
    today = datetime.now().date()
    expired_ids = []
    for row_id, day in rows:
        try:
            if datetime.strptime(day, "%d/%m/%Y").date() < today:
                expired_ids.append((row_id,))
        except Exception:
            pass
    if expired_ids:
        c.executemany("DELETE FROM blocked_days WHERE id = ?", expired_ids)
        conn.commit()
    conn.close()
    return len(expired_ids)
