import sqlite3

from .config import DB_FILE


def _add_column(cur, table, column, coltype):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            pass
        else:
            raise


def _ensure_tg_user_columns(cur):
    cols = [
        ("tg_first_name", "TEXT"),
        ("tg_last_name", "TEXT"),
        ("tg_username", "TEXT"),
        ("tg_lang", "TEXT"),
        ("tg_is_premium", "INTEGER"),
        ("tg_last_seen", "TEXT"),
        ("tg_first_seen", "TEXT"),
        ("tg_chat_type", "TEXT"),
        ("tg_chat_id", "INTEGER"),
        ("tg_chat_title", "TEXT"),
    ]
    for name, typ in cols:
        _add_column(cur, "users", name, typ)


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # bot instances
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_instances (
            bot_id TEXT PRIMARY KEY,
            bot_name TEXT,
            bot_token TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            owner_telegram_id INTEGER,
            admin_active INTEGER DEFAULT 0,
            default_timezone TEXT DEFAULT 'UTC',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    _add_column(c, "bot_instances", "admin_active", "INTEGER DEFAULT 0")
    _add_column(c, "bot_instances", "default_timezone", "TEXT DEFAULT 'UTC'")
    c.execute("UPDATE bot_instances SET admin_active = COALESCE(admin_active, 0)")
    c.execute("UPDATE bot_instances SET default_timezone = COALESCE(default_timezone, 'UTC')")
    try:
        c.execute("DROP INDEX IF EXISTS idx_bot_instances_owner")
    except Exception:
        pass
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bot_instances_owner
        ON bot_instances(owner_telegram_id)
    """
    )
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_instances_token
        ON bot_instances(bot_token)
    """
    )

    # users
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
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
            hourly_Sprinter INTEGER DEFAULT 0,
            PRIMARY KEY (bot_id, telegram_id),
            FOREIGN KEY (bot_id) REFERENCES bot_instances(bot_id)
        )
    """
    )
    for alter_sql in [
        "ALTER TABLE users ADD COLUMN timezone TEXT DEFAULT 'UTC'",
        "ALTER TABLE users ADD COLUMN token_status TEXT DEFAULT 'unknown'",
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in [
        "ALTER TABLE users ADD COLUMN notify_accepted INTEGER DEFAULT 1",
        "ALTER TABLE users ADD COLUMN notify_not_accepted INTEGER DEFAULT 1",
        "ALTER TABLE users ADD COLUMN notify_rejected INTEGER DEFAULT 1",
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in [
        "ALTER TABLE users ADD COLUMN bl_email TEXT",
        "ALTER TABLE users ADD COLUMN bl_password TEXT",
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in ["ALTER TABLE users ADD COLUMN portal_token TEXT"]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in ["ALTER TABLE users ADD COLUMN mobile_headers TEXT"]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in ["ALTER TABLE users ADD COLUMN mobile_auth_json TEXT"]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass
    for alter_sql in ["ALTER TABLE users ADD COLUMN bl_uuid TEXT"]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass

    # booked slots
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS booked_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            from_time TEXT NOT NULL,
            to_time TEXT NOT NULL,
            name TEXT,
            FOREIGN KEY (bot_id, telegram_id) REFERENCES users(bot_id, telegram_id)
        )
    """
    )

    # blocked days
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS blocked_days (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            UNIQUE (bot_id, telegram_id, day),
            FOREIGN KEY (bot_id, telegram_id) REFERENCES users(bot_id, telegram_id)
        )
    """
    )

    # offer logs
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS offer_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            offer_id TEXT NOT NULL,
            status TEXT NOT NULL,
            type TEXT,
            vehicle_class TEXT,
            price REAL,
            currency TEXT,
            pickup_time TEXT,
            ends_at TEXT,
            pu_address TEXT,
            do_address TEXT,
            estimated_distance_meters REAL,
            duration_minutes INTEGER,
            km_included INTEGER,
            guest_requests TEXT,
            flight_number TEXT,
            rejection_reason TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (bot_id, telegram_id, offer_id)
        )
    """
    )
    for col, coltype in [
        ("ends_at", "TEXT"),
        ("pu_address", "TEXT"),
        ("do_address", "TEXT"),
        ("estimated_distance_meters", "REAL"),
        ("duration_minutes", "INTEGER"),
        ("km_included", "INTEGER"),
        ("guest_requests", "TEXT"),
        ("flight_number", "TEXT"),
        ("rejection_reason", "TEXT"),
        ("created_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE offer_logs ADD COLUMN {col} {coltype}")
        except Exception:
            pass
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_logs_unique
        ON offer_logs(bot_id, telegram_id, offer_id)
    """
    )

    # pinned warnings
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS pinned_warnings (
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            no_token_msg_id INTEGER,
            expired_msg_id INTEGER,
            PRIMARY KEY (bot_id, telegram_id),
            FOREIGN KEY (bot_id, telegram_id) REFERENCES users(bot_id, telegram_id)
        )
    """
    )

    # custom filters (safe migrate)
    c.execute("CREATE TABLE IF NOT EXISTS custom_filters (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    for alter_sql in [
        "ALTER TABLE custom_filters ADD COLUMN slug TEXT",
        "ALTER TABLE custom_filters ADD COLUMN name TEXT",
        "ALTER TABLE custom_filters ADD COLUMN description TEXT",
        "ALTER TABLE custom_filters ADD COLUMN global_enabled INTEGER DEFAULT 1",
        "ALTER TABLE custom_filters ADD COLUMN params TEXT DEFAULT '{}'",
        "ALTER TABLE custom_filters ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE custom_filters ADD COLUMN rule_kind TEXT",
        "ALTER TABLE custom_filters ADD COLUMN rule_code TEXT",
        "ALTER TABLE custom_filters ADD COLUMN matcher TEXT",
    ]:
        try:
            c.execute(alter_sql)
        except Exception:
            pass

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_filters_slug ON custom_filters(slug)")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS user_custom_filters (
            bot_id      TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            filter_id   INTEGER NOT NULL,
            enabled     INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (bot_id, telegram_id, filter_id),
            FOREIGN KEY (bot_id, telegram_id) REFERENCES users(bot_id, telegram_id),
            FOREIGN KEY (filter_id)   REFERENCES custom_filters(id)
        )
    """
    )

    try:
        c.execute(
            "UPDATE custom_filters SET rule_kind = COALESCE(NULLIF(rule_kind,''),'generic') "
            "WHERE rule_kind IS NULL OR TRIM(rule_kind)=''"
        )
    except Exception:
        pass
    try:
        c.execute("UPDATE custom_filters SET matcher   = COALESCE(matcher,'') WHERE matcher IS NULL")
    except Exception:
        pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS endtime_formulas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            start_hhmm TEXT,
            end_hhmm   TEXT,
            speed_kmh  REAL NOT NULL,
            bonus_min  REAL NOT NULL DEFAULT 0,
            priority   INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (bot_id, telegram_id) REFERENCES users(bot_id, telegram_id)
        )
    """
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_endtime_formulas_user "
        "ON endtime_formulas(bot_id, telegram_id, priority, id)"
    )

    _ensure_tg_user_columns(c)

    conn.commit()
    conn.close()
