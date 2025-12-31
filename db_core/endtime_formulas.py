import sqlite3

from .config import DB_FILE


def get_endtime_formulas(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT id, start_hhmm, end_hhmm, speed_kmh, bonus_min, priority
        FROM endtime_formulas
        WHERE bot_id = ? AND telegram_id = ?
        ORDER BY priority ASC, COALESCE(start_hhmm,''), COALESCE(end_hhmm,'')
    """,
        (bot_id, telegram_id),
    )
    rows = c.fetchall()
    conn.close()
    return [
        {"id": r[0], "start": r[1], "end": r[2], "speed_kmh": r[3], "bonus_min": r[4], "priority": r[5]}
        for r in rows
    ]


def replace_endtime_formulas(bot_id: str, telegram_id: int, items: list[dict]):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM endtime_formulas WHERE bot_id=? AND telegram_id=?", (bot_id, telegram_id))
    for it in items:
        c.execute(
            """
            INSERT INTO endtime_formulas (bot_id, telegram_id, start_hhmm, end_hhmm, speed_kmh, bonus_min, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                bot_id,
                telegram_id,
                (it.get("start") or None),
                (it.get("end") or None),
                float(it["speed_kmh"]),
                float(it.get("bonus_min", 0) or 0),
                int(it.get("priority", 0) or 0),
            ),
        )
    conn.commit()
    conn.close()


def add_endtime_formula(
    bot_id: str,
    telegram_id: int,
    start: str | None,
    end: str | None,
    speed_kmh: float,
    bonus_min: float = 0,
    priority: int = 0,
):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO endtime_formulas (bot_id, telegram_id, start_hhmm, end_hhmm, speed_kmh, bonus_min, priority)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (bot_id, telegram_id, start, end, float(speed_kmh), float(bonus_min or 0), int(priority or 0)),
    )
    conn.commit()
    conn.close()


def delete_endtime_formula(bot_id: str, telegram_id: int, formula_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM endtime_formulas WHERE id=? AND bot_id=? AND telegram_id=?", (formula_id, bot_id, telegram_id))
    conn.commit()
    conn.close()


def get_user_endtime_formulas(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT id, from_time, to_time, speed_kmh, bonus_min, active, position
        FROM user_endtime_formulas
        WHERE bot_id=? AND telegram_id=? AND COALESCE(active,1)=1
        ORDER BY position ASC, id ASC
    """,
        (bot_id, telegram_id),
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            "id": r[0],
            "from": r[1],
            "to": r[2],
            "speed_kmh": float(r[3]),
            "bonus_min": float(r[4]),
            "active": bool(r[5]),
            "position": int(r[6]),
        }
        for r in rows
    ]
