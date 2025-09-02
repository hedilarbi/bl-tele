# db.py

import sqlite3, json as _json, os
from datetime import datetime as _dt

DB_FILE = "users.db"

# ✅ Vehicle classes we support
VEHICLE_CLASSES = ["SUV", "VAN", "Business", "First", "Electric", "Sprinter"]


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # ---------- USERS ----------
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            token TEXT,
            filters TEXT,
            active INTEGER DEFAULT 0,
            transfer_SUV INTEGER DEFAULT 0,
            transfer_VAN INTEGER DEFAULT 0,
            transfer_Business INTEGER DEFAULT 0,
            transfer_First INTEGER DEFAULT 0,
            transfer_Electric INTEGER DEFAULT 0,
            transfer_Sprinter INTEGER DEFAULT 0,
            hourly_SUV INTEGER DEFAULT 0,
            hourly_VAN INTEGER DEFAULT 0,
            hourly_Business INTEGER DEFAULT 0,
            hourly_First INTEGER DEFAULT 0,
            hourly_Electric INTEGER DEFAULT 0,
            hourly_Sprinter INTEGER DEFAULT 0
        )
    ''')
    # Add columns safely
    for alter_sql in [
        "ALTER TABLE users ADD COLUMN timezone TEXT DEFAULT 'UTC'",
        "ALTER TABLE users ADD COLUMN token_status TEXT DEFAULT 'unknown'"
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass

    # ---------- BOOKED SLOTS ----------
    c.execute('''
        CREATE TABLE IF NOT EXISTS booked_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            from_time TEXT NOT NULL,   -- dd/mm/YYYY HH:MM (user local)
            to_time TEXT NOT NULL,     -- dd/mm/YYYY HH:MM (user local)
            name TEXT,
            FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
        )
    ''')

    # ---------- BLOCKED DAYS ----------
    c.execute('''
        CREATE TABLE IF NOT EXISTS blocked_days (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            day TEXT NOT NULL, -- format dd/mm/YYYY (user local date)
            UNIQUE (telegram_id, day),
            FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
        )
    ''')
    

    # ---------- OFFER LOGS ----------
    c.execute('''
        CREATE TABLE IF NOT EXISTS offer_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            offer_id TEXT NOT NULL,
            status TEXT NOT NULL,                  -- "accepted" | "rejected"
            type TEXT,                             -- transfer | hourly
            vehicle_class TEXT,
            price REAL,
            currency TEXT,
            pickup_time TEXT,
            ends_at TEXT,                          -- computed / provided ISO
            pu_address TEXT,
            do_address TEXT,
            estimated_distance_meters REAL,
            duration_minutes INTEGER,              -- transfer: estimatedDurationMinutes; hourly: durationMinutes
            km_included INTEGER,                   -- hourly only
            guest_requests TEXT,                   -- NEW
            flight_number TEXT,                    -- NEW
            rejection_reason TEXT,                 -- NULL if accepted
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (telegram_id, offer_id)
        )
    ''')
    
    # safe ALTERs for older DBs
    for col, coltype in [
        ("ends_at", "TEXT"),
        ("pu_address", "TEXT"),
        ("do_address", "TEXT"),
        ("estimated_distance_meters", "REAL"),
        ("duration_minutes", "INTEGER"),
        ("km_included", "INTEGER"),
        ("guest_requests", "TEXT"),   # NEW
        ("flight_number", "TEXT"),    # NEW
        ("rejection_reason", "TEXT"),
        ("created_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE offer_logs ADD COLUMN {col} {coltype}")
        except Exception:
            pass
    # unique index (safe)
    c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_logs_unique
        ON offer_logs(telegram_id, offer_id)
    ''')

    # ---------- PINNED WARNINGS ----------
    c.execute('''
        CREATE TABLE IF NOT EXISTS pinned_warnings (
            telegram_id INTEGER PRIMARY KEY,
            no_token_msg_id INTEGER,
            expired_msg_id INTEGER,
            FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
        )
    ''')
   # ---------- CUSTOM FILTERS (safe migrate) ----------
    c.execute("CREATE TABLE IF NOT EXISTS custom_filters (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    for alter_sql in [
        "ALTER TABLE custom_filters ADD COLUMN slug TEXT",
        "ALTER TABLE custom_filters ADD COLUMN name TEXT",
        "ALTER TABLE custom_filters ADD COLUMN description TEXT",
        "ALTER TABLE custom_filters ADD COLUMN global_enabled INTEGER DEFAULT 1",
        "ALTER TABLE custom_filters ADD COLUMN params TEXT DEFAULT '{}'",
        "ALTER TABLE custom_filters ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP",
        # seen in other snapshots/schemas:
        "ALTER TABLE custom_filters ADD COLUMN rule_kind TEXT",
        "ALTER TABLE custom_filters ADD COLUMN rule_code TEXT",
        "ALTER TABLE custom_filters ADD COLUMN matcher TEXT",
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_filters_slug ON custom_filters(slug)")
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_custom_filters (
        telegram_id INTEGER NOT NULL,
        filter_id   INTEGER NOT NULL,
        enabled     INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (telegram_id, filter_id),
        FOREIGN KEY (telegram_id) REFERENCES users(telegram_id),
        FOREIGN KEY (filter_id)   REFERENCES custom_filters(id)
    )
""")

    # Fill reasonable defaults where past rows have NULL (no effect if column missing)
    try: c.execute("UPDATE custom_filters SET rule_kind = COALESCE(NULLIF(rule_kind,''),'generic') WHERE rule_kind IS NULL OR TRIM(rule_kind)=''")
    except Exception: pass
    try: c.execute("UPDATE custom_filters SET matcher   = COALESCE(matcher,'') WHERE matcher IS NULL")
    except Exception: pass



    conn.commit()
    conn.close()


# ---------------- USERS ----------------
def add_user(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO users (telegram_id, token, filters, timezone, token_status) VALUES (?, ?, ?, ?, ?)",
        (telegram_id, None, '{}', 'UTC', 'unknown')
    )
    conn.commit()
    conn.close()


def update_token(telegram_id: int, token: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET token = ?, token_status = 'unknown' WHERE telegram_id = ?", (token, telegram_id))
    conn.commit()
    conn.close()


def set_token_status(telegram_id: int, status: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET token_status = ? WHERE telegram_id = ?", (status, telegram_id))
    conn.commit()
    conn.close()


def get_token_status(telegram_id: int) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token_status FROM users WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else "unknown"


def update_filters(telegram_id: int, filters_json: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET filters = ? WHERE telegram_id = ?", (filters_json, telegram_id))
    conn.commit()
    conn.close()


def get_all_users():
    # Return ALL users (even if token is NULL), so poller can warn about missing token
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT telegram_id, token, filters, active FROM users")
    users = c.fetchall()
    conn.close()
    return users


def get_active(telegram_id: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT active FROM users WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def set_active(telegram_id: int, active: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET active = ? WHERE telegram_id = ?", (1 if active else 0, telegram_id))
    conn.commit()
    conn.close()


# ⭐ Timezone
def get_user_timezone(telegram_id: int) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT timezone FROM users WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else "UTC"


def set_user_timezone(telegram_id: int, tz: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET timezone = ? WHERE telegram_id = ?", (tz, telegram_id))
    conn.commit()
    conn.close()


# ---------------- BOOKED SLOTS ----------------
def add_booked_slot(telegram_id: int, from_time: str, to_time: str, name: str = None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO booked_slots (telegram_id, from_time, to_time, name)
        VALUES (?, ?, ?, ?)
    """, (telegram_id, from_time, to_time, name))
    conn.commit()
    conn.close()


def get_booked_slots(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT id, from_time, to_time, name
        FROM booked_slots
        WHERE telegram_id = ?
    """, (telegram_id,))
    rows = c.fetchall()
    conn.close()
    return [{"id": row[0], "from": row[1], "to": row[2], "name": row[3]} for row in rows]


def delete_booked_slot(slot_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM booked_slots WHERE id = ?", (slot_id,))
    conn.commit()
    conn.close()


# ---------------- BLOCKED DAYS (SCHEDULE) ----------------
def get_blocked_days(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT id, day FROM blocked_days
        WHERE telegram_id = ?
        ORDER BY day ASC
    """, (telegram_id,))
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "day": r[1]} for r in rows]


def add_blocked_day(telegram_id: int, day_str: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO blocked_days (telegram_id, day)
        VALUES (?, ?)
    """, (telegram_id, day_str))
    conn.commit()
    conn.close()


def delete_blocked_day(day_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM blocked_days WHERE id = ?", (day_id,))
    conn.commit()
    conn.close()


# ---------------- VEHICLE CLASSES ----------------
def get_vehicle_classes_state(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        SELECT transfer_SUV, transfer_VAN, transfer_Business, transfer_First, transfer_Electric, transfer_Sprinter,
               hourly_SUV, hourly_VAN, hourly_Business, hourly_First, hourly_Electric, hourly_Sprinter
        FROM users WHERE telegram_id = ?
    ''', (telegram_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return {
            "transfer": {v: 0 for v in VEHICLE_CLASSES},
            "hourly": {v: 0 for v in VEHICLE_CLASSES}
        }
    return {
        "transfer": {VEHICLE_CLASSES[i]: row[i] for i in range(6)},
        "hourly": {VEHICLE_CLASSES[i]: row[i+6] for i in range(6)}
    }


def toggle_vehicle_class(telegram_id: int, mode: str, vclass: str):
    column = f"{mode}_{vclass}"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT {column} FROM users WHERE telegram_id = ?", (telegram_id,))
    current = c.fetchone()
    if current is None:
        conn.close()
        return None
    current_val = current[0]
    new_val = 0 if current_val == 1 else 1
    c.execute(f"UPDATE users SET {column} = ? WHERE telegram_id = ?", (new_val, telegram_id))
    conn.commit()
    conn.close()
    return new_val


# ---------------- OFFER LOGGING ----------------
def log_offer_decision(telegram_id: int, offer: dict, status: str, reason: str = None):
    rid = (offer.get("rides") or [{}])[0] if offer else {}

    offer_id   = offer.get("id")
    otype      = (rid.get("type") or "")
    vehicle_cl = (offer.get("vehicleClass") or "")
    price      = offer.get("price")
    currency   = offer.get("currency")
    pickup     = rid.get("pickupTime")
    ends_at    = rid.get("endsAt")

    pu_addr    = ((rid.get("pickUpLocation") or {}).get("address")) if rid else None
    do_addr    = ((rid.get("dropOffLocation") or {}).get("address")) if rid else None

    duration   = rid.get("estimatedDurationMinutes") or rid.get("durationMinutes")
    est_dist   = rid.get("estimatedDistanceMeters")
    km_incl    = rid.get("kmIncluded")

    # NEW: optional fields
    guest_raw  = rid.get("guestRequests")
    if isinstance(guest_raw, (list, tuple)):
        guest_requests = ", ".join([str(x) for x in guest_raw if str(x).strip()])
    elif isinstance(guest_raw, dict):
        try:
            import json as _json
            guest_requests = _json.dumps(guest_raw, ensure_ascii=False)
        except Exception:
            guest_requests = str(guest_raw)
    else:
        guest_requests = guest_raw if guest_raw is not None else None
    flight_number = (rid.get("flight") or {}).get("number") if isinstance(rid.get("flight"), dict) else None

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO offer_logs (
            telegram_id, offer_id, status, type, vehicle_class, price, currency,
            pickup_time, ends_at, pu_address, do_address, estimated_distance_meters,
            duration_minutes, km_included, guest_requests, flight_number,
            rejection_reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(telegram_id, offer_id) DO UPDATE SET
            status = excluded.status,
            type = excluded.type,
            vehicle_class = excluded.vehicle_class,
            price = excluded.price,
            currency = excluded.currency,
            pickup_time = excluded.pickup_time,
            ends_at = excluded.ends_at,
            pu_address = excluded.pu_address,
            do_address = excluded.do_address,
            estimated_distance_meters = excluded.estimated_distance_meters,
            duration_minutes = excluded.duration_minutes,
            km_included = excluded.km_included,
            guest_requests = excluded.guest_requests,
            flight_number = excluded.flight_number,
            rejection_reason = excluded.rejection_reason,
            created_at = CURRENT_TIMESTAMP
    """, (
        telegram_id, offer_id, status, otype, vehicle_cl, price, currency,
        pickup, ends_at, pu_addr, do_addr, est_dist, duration, km_incl,
        guest_requests, flight_number, reason
    ))
    conn.commit()
    conn.close()


def get_processed_offer_ids(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT offer_id FROM offer_logs WHERE telegram_id = ?", (telegram_id,))
    rows = c.fetchall()
    conn.close()
    return {r[0] for r in rows}


# ---------------- STATS HELPERS ----------------
def get_offer_logs(telegram_id: int, limit: int = 10, offset: int = 0):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT offer_id, status, type, vehicle_class, price, currency, pickup_time, ends_at,
               pu_address, do_address, estimated_distance_meters, duration_minutes, km_included,
               guest_requests, flight_number,
               rejection_reason, created_at
        FROM offer_logs
        WHERE telegram_id = ?
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ? OFFSET ?
    """, (telegram_id, limit, offset))
    rows = c.fetchall()
    conn.close()
    results = []
    for r in rows:
        results.append({
            "offer_id": r[0],
            "status": r[1],
            "type": r[2],
            "vehicle_class": r[3],
            "price": r[4],
            "currency": r[5],
            "pickup_time": r[6],
            "ends_at": r[7],
            "pu_address": r[8],
            "do_address": r[9],
            "estimated_distance_meters": r[10],
            "duration_minutes": r[11],
            "km_included": r[12],
            "guest_requests": r[13],
            "flight_number": r[14],
            "rejection_reason": r[15],
            "created_at": r[16],
        })
    return results


def get_offer_logs_counts(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM offer_logs WHERE telegram_id = ?", (telegram_id,))
    total = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM offer_logs WHERE telegram_id = ? AND status = 'accepted'", (telegram_id,))
    accepted = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM offer_logs WHERE telegram_id = ? AND status = 'rejected'", (telegram_id,))
    rejected = c.fetchone()[0] or 0
    conn.close()
    return {"total": total, "accepted": accepted, "rejected": rejected}


# ---------------- PINNED WARNINGS HELPERS ----------------
def _ensure_pinned_row(telegram_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO pinned_warnings (telegram_id) VALUES (?)", (telegram_id,))
    conn.commit()
    conn.close()


def get_pinned_warnings(telegram_id: int):
    _ensure_pinned_row(telegram_id)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT no_token_msg_id, expired_msg_id FROM pinned_warnings WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return {"no_token_msg_id": None, "expired_msg_id": None}
    return {"no_token_msg_id": row[0], "expired_msg_id": row[1]}


def save_pinned_warning(telegram_id: int, kind: str, message_id: int):
    # kind in {"no_token", "expired"}
    _ensure_pinned_row(telegram_id)
    column = "no_token_msg_id" if kind == "no_token" else "expired_msg_id"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"UPDATE pinned_warnings SET {column} = ? WHERE telegram_id = ?", (message_id, telegram_id))
    conn.commit()
    conn.close()


def clear_pinned_warning(telegram_id: int, kind: str):
    _ensure_pinned_row(telegram_id)
    column = "no_token_msg_id" if kind == "no_token" else "expired_msg_id"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"UPDATE pinned_warnings SET {column} = NULL WHERE telegram_id = ?", (telegram_id,))
    conn.commit()
    conn.close()
def create_custom_filter(
    slug: str,
    name_: str,
    description: str = "",
    params: dict | None = None,
    global_enabled: bool = True,
    rule_kind: str = "generic",
    rule_code: str | None = None,
):
    schema = _table_schema("custom_filters")
    colmap = {c["name"]: c for c in schema}
    if "slug" not in colmap or "name" not in colmap:
        raise RuntimeError("custom_filters table is missing required columns (slug/name)")

    insert_cols, values = [], []

    def add(col: str, val):
        insert_cols.append(col); values.append(val)

    for col in schema:
        name = col["name"]
        if name == "id":
            continue  # autoincrement
        if name == "slug":
            add("slug", slug)
        elif name == "name":
            add("name", name_.strip())
        elif name == "description":
            add("description", (description or "").strip())
        elif name == "params":
            add("params", _json.dumps(params or {}, ensure_ascii=False))
        elif name == "global_enabled":
            add("global_enabled", 1 if global_enabled else 0)
        elif name == "rule_kind":
            add("rule_kind", (rule_kind or "generic"))
        elif name == "rule_code":
            add("rule_code", rule_code or "")
        elif name == "created_at":
            # let DEFAULT work if present; otherwise fill if NOT NULL without default
            if col["notnull"] and col["dflt_value"] is None:
                add("created_at", _dt.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                pass
        else:
            # Any unknown NOT NULL w/o DEFAULT must be supplied to satisfy constraint (e.g. matcher)
            if col["notnull"] and col["dflt_value"] is None:
                add(name, _default_for_sqlite_type(col["type"]))
            else:
                # nullable or has DEFAULT -> skip (DB will fill)
                pass

    placeholders = ", ".join(["?"] * len(values))
    sql = f"INSERT INTO custom_filters ({', '.join(insert_cols)}) VALUES ({placeholders}) " \
          f"ON CONFLICT(slug) DO UPDATE SET " \
          f"name=excluded.name, description=excluded.description"
    if "global_enabled" in colmap:
        sql += ", global_enabled=excluded.global_enabled"
    if "params" in colmap:
        sql += ", params=excluded.params"
    if "rule_kind" in colmap:
        sql += ", rule_kind=excluded.rule_kind"
    if "rule_code" in colmap:
        sql += ", rule_code=excluded.rule_code"

    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute(sql, values)
    conn.commit(); conn.close()


def list_all_custom_filters():
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    cols = _table_cols("custom_filters")
    sel = "id, slug, name, description"
    if "global_enabled" in cols: sel += ", global_enabled"
    if "params" in cols:         sel += ", params"
    if "rule_kind" in cols:      sel += ", rule_kind"
    c.execute(f"SELECT {sel} FROM custom_filters ORDER BY id DESC")
    rows = c.fetchall(); conn.close()

    # map tuple indexes
    idx = {k: i for i, k in enumerate(sel.replace(" ", "").split(","))}
    out = []
    for r in rows:
        item = {
            "id": r[idx["id"]], "slug": r[idx["slug"]],
            "name": r[idx["name"]], "description": r[idx["description"]],
        }
        if "global_enabled" in idx:
            item["global_enabled"] = bool(r[idx["global_enabled"]])
        if "params" in idx:
            raw = r[idx["params"]]
            try:
                item["params"] = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                item["params"] = {}
        if "rule_kind" in idx:
            item["rule_kind"] = r[idx["rule_kind"]] or "generic"
        out.append(item)
    return out



def get_filter_by_slug(slug: str):
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute("""SELECT id, slug, name, description, global_enabled, params FROM custom_filters WHERE slug=?""", (slug,))
    r = c.fetchone(); conn.close()
    if not r: return None
    return {"id": r[0], "slug": r[1], "name": r[2], "description": r[3], "global_enabled": bool(r[4]), "params": r[5]}

def update_custom_filter(slug: str, **fields):
    allowed = {"name","description","params","global_enabled","rule_kind","rule_code"}
    cols = _table_cols("custom_filters")
    sets, vals = [], []
    for k, v in fields.items():
        if k not in allowed: 
            continue
        if k not in cols:      # skip fields that don't exist in this DB
            continue
        if k == "params" and isinstance(v, (dict, list)):
            v = _json.dumps(v, ensure_ascii=False)
        if k == "global_enabled":
            v = 1 if bool(v) else 0
        if k == "rule_kind":
            v = v or "generic"
        sets.append(f"{k}=?"); vals.append(v)
    if not sets:
        return
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute(f"UPDATE custom_filters SET {', '.join(sets)} WHERE slug=?", (*vals, slug))
    conn.commit(); conn.close()


def assign_custom_filter(telegram_id: int, slug: str, enabled: bool = True):
    f = get_filter_by_slug(slug)
    if not f: raise ValueError("Unknown filter slug")
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute("""INSERT OR IGNORE INTO user_custom_filters (telegram_id, filter_id, enabled) VALUES (?, ?, ?)""",
              (telegram_id, f["id"], 1 if enabled else 0))
    c.execute("""UPDATE user_custom_filters SET enabled=? WHERE telegram_id=? AND filter_id=?""",
              (1 if enabled else 0, telegram_id, f["id"]))
    conn.commit(); conn.close()

def unassign_custom_filter(telegram_id: int, slug: str):
    f = get_filter_by_slug(slug)
    if not f: return
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute("""DELETE FROM user_custom_filters WHERE telegram_id=? AND filter_id=?""", (telegram_id, f["id"]))
    conn.commit(); conn.close()

def toggle_user_custom_filter(telegram_id: int, slug: str, enabled: bool):
    assign_custom_filter(telegram_id, slug, enabled)

def list_user_custom_filters(telegram_id: int):
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute("""
        SELECT cf.slug, cf.name, cf.description, cf.global_enabled, ucf.enabled, cf.params
        FROM custom_filters cf
        JOIN user_custom_filters ucf ON ucf.filter_id = cf.id
        WHERE ucf.telegram_id = ?
        ORDER BY cf.id ASC
    """, (telegram_id,))
    rows = c.fetchall(); conn.close()
    return [
        {"slug": r[0], "name": r[1], "description": r[2], "global_enabled": bool(r[3]), "user_enabled": bool(r[4]), "params": r[5]}
        for r in rows
    ]

DB_FILE = os.path.join(os.path.dirname(__file__), "users.db")

def _table_cols(table: str):
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in c.fetchall()]
    conn.close()
    return set(cols)
def _table_schema(table: str):
    conn = sqlite3.connect(DB_FILE); c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    rows = c.fetchall(); conn.close()
    # rows: (cid, name, type, notnull, dflt_value, pk)
    return [
        {"name": r[1], "type": (r[2] or ""), "notnull": bool(r[3]),
         "dflt_value": r[4], "pk": bool(r[5])}
        for r in rows
    ]

def _default_for_sqlite_type(type_str: str):
    t = (type_str or "").upper()
    if "INT" in t:  return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t: return 0.0
    if "BLOB" in t: return b""
    # TEXT / unknown
    return ""
