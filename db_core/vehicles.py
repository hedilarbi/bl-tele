import sqlite3

from .config import DB_FILE, VEHICLE_CLASSES


def get_vehicle_classes_state(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        SELECT transfer_SUV, transfer_VAN, transfer_Business, transfer_First, transfer_Electric, transfer_Sprinter,
               hourly_SUV, hourly_VAN, hourly_Business, hourly_First, hourly_Electric, hourly_Sprinter
        FROM users WHERE bot_id = ? AND telegram_id = ?
    """,
        (bot_id, telegram_id),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return {
            "transfer": {v: 0 for v in VEHICLE_CLASSES},
            "hourly": {v: 0 for v in VEHICLE_CLASSES},
        }
    return {
        "transfer": {VEHICLE_CLASSES[i]: row[i] for i in range(6)},
        "hourly": {VEHICLE_CLASSES[i]: row[i + 6] for i in range(6)},
    }


def toggle_vehicle_class(bot_id: str, telegram_id: int, mode: str, vclass: str):
    column = f"{mode}_{vclass}"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT {column} FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    current = c.fetchone()
    if current is None:
        conn.close()
        return None
    current_val = current[0]
    new_val = 0 if current_val == 1 else 1
    c.execute(
        f"UPDATE users "
        f"SET {column} = ?, cache_version = COALESCE(cache_version, 0) + 1 "
        f"WHERE bot_id = ? AND telegram_id = ?",
        (new_val, bot_id, telegram_id),
    )
    conn.commit()
    conn.close()
    return new_val
