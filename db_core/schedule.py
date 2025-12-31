import sqlite3

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
    conn.commit()
    conn.close()


def delete_blocked_day(bot_id: str, day_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM blocked_days WHERE id = ? AND bot_id = ?", (day_id, bot_id))
    conn.commit()
    conn.close()
