import sqlite3

from .config import DB_FILE


def add_booked_slot(bot_id: str, telegram_id: int, from_time: str, to_time: str, name: str = None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO booked_slots (bot_id, telegram_id, from_time, to_time, name)
        VALUES (?, ?, ?, ?, ?)
    """,
        (bot_id, telegram_id, from_time, to_time, name),
    )
    conn.commit()
    conn.close()


def get_booked_slots(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT id, from_time, to_time, name
        FROM booked_slots
        WHERE bot_id = ? AND telegram_id = ?
    """,
        (bot_id, telegram_id),
    )
    rows = c.fetchall()
    conn.close()
    return [{"id": row[0], "from": row[1], "to": row[2], "name": row[3]} for row in rows]


def delete_booked_slot(bot_id: str, slot_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM booked_slots WHERE id = ? AND bot_id = ?", (slot_id, bot_id))
    conn.commit()
    conn.close()
