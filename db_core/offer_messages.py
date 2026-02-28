import sqlite3

from .config import DB_FILE


def save_offer_message(bot_id: str, telegram_id: int, message_key: str, header_text: str, full_text: str):
    if not message_key or not full_text:
        return
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute("PRAGMA busy_timeout=5000")
    c.execute(
        """
        INSERT INTO offer_messages (bot_id, telegram_id, offer_id, full_text, header_text)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(bot_id, telegram_id, offer_id) DO UPDATE SET
            full_text = excluded.full_text,
            header_text = excluded.header_text
    """,
        (bot_id, telegram_id, message_key, full_text, header_text),
    )
    conn.commit()
    conn.close()


def get_offer_message(bot_id: str, telegram_id: int, message_key: str) -> tuple[str | None, str | None]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT header_text, full_text FROM offer_messages
        WHERE bot_id = ? AND telegram_id = ? AND offer_id = ?
        LIMIT 1
    """,
        (bot_id, telegram_id, message_key),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return (None, None)
    return row[0], row[1]
