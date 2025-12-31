import sqlite3

from .config import DB_FILE


def _ensure_pinned_row(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO pinned_warnings (bot_id, telegram_id) VALUES (?, ?)",
        (bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def get_pinned_warnings(bot_id: str, telegram_id: int):
    _ensure_pinned_row(bot_id, telegram_id)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT no_token_msg_id, expired_msg_id FROM pinned_warnings WHERE bot_id = ? AND telegram_id = ?",
        (bot_id, telegram_id),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return {"no_token_msg_id": None, "expired_msg_id": None}
    return {"no_token_msg_id": row[0], "expired_msg_id": row[1]}


def save_pinned_warning(bot_id: str, telegram_id: int, kind: str, message_id: int):
    _ensure_pinned_row(bot_id, telegram_id)
    column = "no_token_msg_id" if kind == "no_token" else "expired_msg_id"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        f"UPDATE pinned_warnings SET {column} = ? WHERE bot_id = ? AND telegram_id = ?",
        (message_id, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()


def clear_pinned_warning(bot_id: str, telegram_id: int, kind: str):
    _ensure_pinned_row(bot_id, telegram_id)
    column = "no_token_msg_id" if kind == "no_token" else "expired_msg_id"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        f"UPDATE pinned_warnings SET {column} = NULL WHERE bot_id = ? AND telegram_id = ?",
        (bot_id, telegram_id),
    )
    conn.commit()
    conn.close()
