# bot.py
import json
import sqlite3
import requests
from datetime import datetime
from dateutil.tz import gettz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters
)
import os, time, threading, base64
from typing import Optional
from db import (
    init_db,
    add_user,
    update_token,
    update_filters,
    DB_FILE,
    add_booked_slot,
    get_booked_slots,
    get_vehicle_classes_state,
    toggle_vehicle_class,
    # Timezone
    get_user_timezone,
    set_user_timezone,
    # Schedule
    get_blocked_days,
    add_blocked_day,
    delete_blocked_day,
    # Stats
    get_offer_logs,
    get_offer_logs_counts,
    # Token status
    get_token_status,
    set_token_status,
    # Pinned warnings
    get_pinned_warnings,
    clear_pinned_warning,
    get_notifications,
    set_notification,
    get_endtime_formulas,
    list_user_custom_filters,          # NEW (for "Additional filters")
   
    upsert_user_from_bot,
    get_endtime_formulas
)
import os
from dotenv import load_dotenv
from db import (
    # ... keep existing imports ...
    get_portal_token, update_portal_token,        # reuse portal token storage
    get_bl_account_full,                          # BL email/password
    set_bl_uuid, get_bl_uuid,                     # <-- new helpers you just added
    get_offer_message,
)

try:
    from db import get_bl_account
except Exception:
    def get_bl_account(_uid):
        return None  # if not implemented in db.py yet



load_dotenv()  # reads .env in project root

def _ensure_https_base(url: str) -> str:
    """
    Telegram WebApp buttons only accept HTTPS URLs.
    Coerce any http/relative base into https and strip trailing slash.
    """
    url = (url or "").strip().rstrip("/")
    if not url:
        return url
    if url.lower().startswith("http://"):
        return "https://" + url[7:]
    if not url.lower().startswith("https://"):
        return "https://" + url
    return url

BOT_TOKEN = os.getenv("BOT_TOKEN")
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
MINI_APP_BASE = _ensure_https_base(os.getenv("MINI_APP_BASE", "http://localhost:3000"))
BOOKED_SLOTS_URL = f"{MINI_APP_BASE}/booked-slots"
SCHEDULE_URL = f"{MINI_APP_BASE}/schedule"
CURRENT_SCHEDULE_URL = f"{MINI_APP_BASE}/current-schedule"
BL_ACCOUNT_URL = f"{MINI_APP_BASE}/bl-account"


import re
API_HOST = os.getenv("API_HOST", "https://chauffeur-app-api.blacklane.com")

PORTAL_CLIENT_ID      = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
PORTAL_AUTH_BASE      = os.getenv("PORTAL_AUTH_BASE", "https://athena.blacklane.com")
PARTNER_PORTAL_API    = os.getenv("PARTNER_PORTAL_API", "https://partner-portal-api.blacklane.com")
P1_API_BASE           = os.getenv("API_HOST", "https://chauffeur-app-api.blacklane.com")

_UUID_ATTEMPT_COOLDOWN_S = 3600  # avoid hammering: try at most once/hour per user
_last_uuid_attempt: dict[int, float] = {}



def _get_mobile_token(user_id: int) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token FROM users WHERE telegram_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def _jwt_exp_unverified(token: str) -> Optional[int]:
    try:
        parts = (token or "").split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "==="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
        exp = payload.get("exp")
        return int(exp) if isinstance(exp, (int, float)) else None
    except Exception:
        return None

def _portal_token_expired(token: Optional[str]) -> bool:
    if not token:
        return True
    exp = _jwt_exp_unverified(token)
    if exp is None:
        # If we can't parse, assume valid and let 401 drive re-login.
        return False
    return int(time.time()) >= (exp - 60)  # refresh ~1min early

def _athena_login(email: str, password: str) -> tuple[bool, Optional[str], str]:
    url = f"{PORTAL_AUTH_BASE}/oauth/token"
    payload = {
        "client_id": PORTAL_CLIENT_ID,
        "username": email,
        "password": password,
        "grant_type": "implicit",
        "resource_owner_type": "driver",
    }
    try:
        r = requests.post(url, data=payload, headers={"Accept": "application/json"}, timeout=15)
        if 200 <= r.status_code < 300:
            try:
                j = r.json() or {}
            except Exception:
                return (False, None, "upstream:bad_json")
            tok = (j.get("result") or {}).get("access_token") or j.get("access_token")
            return (True, tok, "ok") if tok else (False, None, "upstream:no_token")
        if r.status_code in (401, 403):
            return (False, None, f"unauthorized:{r.status_code}")
        return (False, None, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, None, f"network:{type(e).__name__}")
    
async def open_settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)               # keep telemetry + uuid background try
    add_user(update.effective_user.id)         # ensure user row exists
    info_text, menu = build_settings_menu(update.effective_user.id)
    await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)


def _portal_get_me(access_token: str) -> tuple[Optional[int], Optional[dict]]:
    url = f"{PARTNER_PORTAL_API}/me"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "BLPortal/uuid-fetch (+bot)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            return r.status_code, r.json()
        return r.status_code, None
    except requests.exceptions.RequestException:
        return None, None

def _p1_get_me_profile(token: str) -> tuple[Optional[int], Optional[dict]]:
    url = f"{P1_API_BASE}/api/v1/me/profile"
    headers = {
        "Authorization": token,  # 'Bearer <JWT>'
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Chauffeur/uuid-fetch (+bot)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            return r.status_code, r.json()
        return r.status_code, None
    except requests.exceptions.RequestException:
        return None, None


def _try_update_bl_uuid(user_id: int):
    # debounce
    now = time.time()
    last = _last_uuid_attempt.get(user_id, 0)
    if now - last < _UUID_ATTEMPT_COOLDOWN_S:
        return
    _last_uuid_attempt[user_id] = now

    # already saved?
    try:
        if get_bl_uuid(user_id):
            return
    except Exception:
        pass

    # 1) Prefer Partner Portal (/me) if we have BL email+password
    try:
        creds = get_bl_account_full(user_id)  # returns (email, password) or (None, None)
    except Exception:
        creds = (None, None)

    email, password = (creds or (None, None))
    if email and password:
        # ensure portal token
        ptoken = get_portal_token(user_id)
        if _portal_token_expired(ptoken):
            ok, new_tok, note = _athena_login(email, password)
            if ok and new_tok:
                update_portal_token(user_id, new_tok)
                ptoken = new_tok
            else:
                ptoken = None  # fallback to P1 below
        if ptoken:
            status, payload = _portal_get_me(ptoken)
            if status == 401 or status == 403:
                # try one re-login
                ok, new_tok, note = _athena_login(email, password)
                if ok and new_tok:
                    update_portal_token(user_id, new_tok)
                    status, payload = _portal_get_me(new_tok)
            if status and 200 <= status < 300 and isinstance(payload, dict):
                bl_id = payload.get("id")
                if isinstance(bl_id, str) and bl_id.strip():
                    set_bl_uuid(user_id, bl_id.strip())
                    return  # done

    # 2) Fallback to Mobile API (/api/v1/me/profile)
    token = _get_mobile_token(user_id)
    if token:
        status, payload = _p1_get_me_profile(token)
        if status and 200 <= status < 300 and isinstance(payload, dict):
            # Prefer 'uuid' if present; else try common alternates
            bl_id = payload.get("uuid") or payload.get("id") or payload.get("chauffeur_id")
            if isinstance(bl_id, str) and bl_id.strip():
                set_bl_uuid(user_id, bl_id.strip())
                return



# --- Capture Telegram user/chat info on every interaction ---
def _capture_from_update(update: Update):
    try:
        u = update.effective_user
        c = update.effective_chat
        if not u:
            return
        # Prefer native dicts when available
        user_d = u.to_dict() if hasattr(u, "to_dict") else {
            "id": u.id,
            "first_name": getattr(u, "first_name", None),
            "last_name": getattr(u, "last_name", None),
            "username": getattr(u, "username", None),
            "language_code": getattr(u, "language_code", None),
            "is_premium": getattr(u, "is_premium", None),
        }
        chat_d = c.to_dict() if (c and hasattr(c, "to_dict")) else (
            {"id": c.id, "type": c.type, "title": getattr(c, "title", None)} if c else {}
        )
        upsert_user_from_bot(user_d, chat_d)
        try:
            threading.Thread(target=_try_update_bl_uuid, args=(u.id,), daemon=True).start()
        except Exception:
            pass
    except Exception:
        # don’t interrupt UX if logging fails
        pass

def normalize_token(s: str) -> str:
    """
    Canonicalize to: 'Bearer <JWT>'.
    Accepts:
      - 'Bearer <JWT>'
      - 'authorization: Bearer <JWT>'
      - raw '<JWT>' (xxx.yyy.zzz)
      - quoted / multiline pastes
    """
    if not s:
        return ""
    s = str(s).strip()

    # remove surrounding quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # collapse whitespace/newlines
    s = " ".join(s.replace("\r", "\n").split())

    # drop leading 'authorization:' if present
    if s.lower().startswith("authorization:"):
        s = s.split(":", 1)[1].strip()

    # already Bearer? keep but normalize capitalization/spacing
    if s.lower().startswith("bearer "):
        tok = s[7:].strip()
        return f"Bearer {tok}"

    # plain JWT pattern?
    is_jwt = bool(re.match(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", s))
    if is_jwt:
        return f"Bearer {s}"

    # fallback: return as-is (some exotic formats)
    return s

def mask_secret(s: str, keep: int = 4) -> str:
    if not s:
        return "—"
    s = str(s)
    if len(s) <= keep * 2:
        return s[:keep] + "…"
    return f"{s[:keep]}…{s[-keep:]}"

def _http_ok(status: int) -> bool:
    return 200 <= status < 300

def validate_mobile_session(token: str) -> tuple[bool, str]:
    """
    Quick upstream probe. Token should already be normalized
    (i.e., 'Bearer <JWT>').
    """
    if not token:
        return (False, "empty_token")
    headers = {
        "Authorization": token,          # <— send exactly what we store
        "Accept": "application/json",
    }
    try:
        r = requests.get(f"{API_HOST}/rides?limit=1", headers=headers, timeout=12)
        if _http_ok(r.status_code):
            return (True, "ok")
        if r.status_code in (401, 403):
            return (False, f"unauthorized:{r.status_code}")
        return (False, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, f"network:{type(e).__name__}")



# ---------------- DB Helpers ----------------
def get_active(telegram_id: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT active FROM users WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return bool(row[0]) if row else False
def mask_email(email: str | None) -> str:
    if not email:
        return "—"
    try:
        local, domain = str(email).split("@", 1)
    except ValueError:
        return str(email)
    if len(local) <= 4:
        return f"{local}*****@{domain}"
    head = local[:4]
    tail = local[-4:] if len(local) > 8 else ""
    return f"{head}*****{tail}@{domain}"


def set_active(telegram_id: int, active: bool):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET active = ? WHERE telegram_id = ?", (1 if active else 0, telegram_id))
    conn.commit()
    conn.close()


def get_filters(telegram_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT filters FROM users WHERE telegram_id = ?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return json.loads(row[0]) if row and row[0] else {}


# ---------------- Small utils ----------------
def fmt_money(price, currency):
    if price is None:
        return "—"
    try:
        return f"{float(price):.2f} {currency or ''}".strip()
    except Exception:
        return f"{price} {currency or ''}".strip()


def fmt_km(meters):
    if meters is None:
        return "—"
    try:
        return f"{float(meters)/1000.0:.1f} km"
    except Exception:
        return str(meters)


def fmt_minutes(mins):
    if mins is None:
        return "—"
    try:
        return f"{float(mins):.0f} min"
    except Exception:
        return str(mins)


def fmt_dt_local(s, tz_name=None):
    if not s:
        return "—"
    try:
        iso = s.replace("Z", "+00:00")
        if "T" in iso or "+" in iso:
            dt = datetime.fromisoformat(iso)
        else:
            dt = datetime.strptime(iso, "%Y-%m-%d %H:%M:%S")
        tzinfo = gettz(tz_name) if tz_name else None
        if tzinfo:
            return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M")
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception:
        return s


def status_emoji(status):
    return "✅" if status == "accepted" else ("❌" if status == "rejected" else "ℹ️")


def safe(v, fallback="—"):
    return fallback if v in (None, "", []) else v


def _esc(s):
    if s is None:
        return "—"
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _norm_guest_requests(val):
    """
    Accepts:
      - JSON string like '["A","B"]'
      - list[str]/list[dict]
      - plain string
    Returns a displayable comma-separated string or None.
    """
    if not val:
        return None
    try:
        if isinstance(val, str):
            # try json decode first
            parsed = json.loads(val)
            val = parsed
    except Exception:
        # keep as plain string
        return str(val)

    if isinstance(val, list):
        out = []
        for it in val:
            if isinstance(it, str):
                out.append(it)
            elif isinstance(it, dict):
                for k in ("label", "name", "value", "text"):
                    if it.get(k):
                        out.append(str(it[k]))
                        break
        return ", ".join(out) if out else None

    return str(val)


def unpin_warning_if_any(telegram_id: int, kind: str):
    # kind: "no_token" | "expired"
    ids = get_pinned_warnings(telegram_id)
    message_id = ids["no_token_msg_id"] if kind == "no_token" else ids["expired_msg_id"]
    if not message_id:
        return
    try:
        requests.post(f"{TG_API}/unpinChatMessage", json={"chat_id": telegram_id, "message_id": message_id}, timeout=10)
    except Exception:
        pass
    clear_pinned_warning(telegram_id, kind)


# ---------------- Menus ----------------
def build_main_menu(is_active: bool):
    status_text = "✅ Active" if is_active else "❌ Not active"
    action_buttons = [InlineKeyboardButton("🔴 Deactivate", callback_data="deactivate")] if is_active else [
        InlineKeyboardButton("🟢 Activate", callback_data="activate")
    ]
    keyboard = [
        [
            InlineKeyboardButton("📊 Statistic", callback_data="statistic"),
            InlineKeyboardButton("✅ Checked statistic", callback_data="checked_statistic"),
        ],
        [
            InlineKeyboardButton("⚙️ Filters", callback_data="filters"),
            InlineKeyboardButton("🔧 Settings", callback_data="settings"),
        ],
        action_buttons,
    ]
    return InlineKeyboardMarkup(keyboard), status_text



def build_settings_menu(user_id: int):
    tz = get_user_timezone(user_id) or "—"
    token_status = get_token_status(user_id) or "unknown"
    dot = "🟢" if token_status == "valid" else ("🔴" if token_status == "expired" else "⚪")

    # Notifications status summary
    prefs = get_notifications(user_id) or {}
    def onoff(flag): return "🟢" if flag else "🔴"
    notif_line = (
        f"{onoff(prefs.get('accepted', True))} Accepted  |  "
        f"{onoff(prefs.get('not_accepted', True))} Not accepted  |  "
        f"{onoff(prefs.get('rejected', True))} Not valid"
    )

    # BL account masked email (wrap in backticks to avoid Markdown parsing of *)
    try:
        acc = get_bl_account(user_id)
        if isinstance(acc, dict):
            bl_email = acc.get("email")
        elif isinstance(acc, (list, tuple)):
            bl_email = acc[0] if acc else None
        else:
            bl_email = acc if isinstance(acc, str) else None
    except Exception:
        bl_email = None

    bl_email_disp = mask_email(bl_email) if bl_email else "—"
    bl_email_line = f"`{bl_email_disp}`" if bl_email_disp != "—" else "—"

    info_text = (
        "🔧 *Settings*\n\n"
        f"🌍 Timezone: `{tz}`\n"
        f"📱 Mobile session: {dot} ({token_status})\n"
        f"🔔 Notifications: {notif_line}\n"
        f"🪪 BL account: {bl_email_line}\n\n"
        "• *Change timezone* to set your local time\n"
        "• *Mobile sessions* to add/update your Blacklane token\n"
        "• *BL account* to set your Blacklane email/password"
    )

    keyboard = [
        [InlineKeyboardButton("🔔 Notifications", callback_data="notifications")],
        [InlineKeyboardButton("🌍 Change timezone", callback_data="change_tz")],
        [InlineKeyboardButton("🪪 BL account", web_app=WebAppInfo(url=BL_ACCOUNT_URL))],
        [InlineKeyboardButton("📱 Mobile sessions", callback_data="mobile_sessions")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)



def build_mobile_sessions_menu(user_id: int):
    token_status = get_token_status(user_id)
    dot = "🟢" if token_status == "valid" else ("🔴" if token_status == "expired" else "⚪")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token FROM users WHERE telegram_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()

    token = row[0] if row else None
    # show only head/tail (6 chars) to avoid leaking the JWT in chat logs
    token_disp = mask_secret(token, keep=6) if token else "—"

    info_text = (
        "📱 *Mobile Sessions*\n\n"
        f"Status: {dot} `{token_status}`\n"
        f"Token:\n`{token_disp}`\n\n"
        "Use *Add/Update token* to paste your current mobile session token."
    )
    keyboard = [
        [InlineKeyboardButton("➕ Add/Update token", callback_data="add_mobile_session")],
        [InlineKeyboardButton("⬅️ Back to Settings", callback_data="settings")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)



def build_filters_menu(filters_data: dict, user_id: int):
    min_price   = filters_data.get("price_min", 0)
    max_price   = filters_data.get("price_max", 0)
    work_start  = filters_data.get("work_start", "00:00")
    work_end    = filters_data.get("work_end", "00:00")
    delay       = filters_data.get("gap", 120)
    min_duration = filters_data.get("min_duration", 0)
    min_km      = filters_data.get("min_km", 0)
    max_km      = filters_data.get("max_km", 0)

    # End-time formulas (admin-assigned)
    rows = get_endtime_formulas(user_id) or []
    if rows:
        def fmt_row(it):
            win = f"{it['start']}–{it['end']}" if it.get("start") and it.get("end") else "else"
            try:
                spd = int(float(it["speed_kmh"]))
            except Exception:
                spd = it["speed_kmh"]
            try:
                bon = int(float(it.get("bonus_min", 0)))
            except Exception:
                bon = it.get("bonus_min", 0)
            return f"• {win}: {spd} km/h + {bon} min"
        formulas_text = "\n" + "\n".join(fmt_row(it) for it in rows)
    else:
        formulas_text = "\n— (not assigned)"

    info_text = (
        f"⚙️ *Bot filters*\n\n"
        f"💸 Min price: {min_price}\n"
        f"💸 Max price: {max_price}\n"
        f"🕒 Work schedule: {work_start} – {work_end}\n"
        f"⏳ Delay (gap): {delay} min\n"
        f"⌛ Min duration: {min_duration} h\n"
        f"📏 Min km: {min_km}\n"
        f"📏 Max km: {max_km}\n"
       
    )

    keyboard = [
        [InlineKeyboardButton("📦 Booked slots", web_app=WebAppInfo(url=BOOKED_SLOTS_URL))],
        [InlineKeyboardButton("📅 Schedule (blocked days)", web_app=WebAppInfo(url=SCHEDULE_URL))],
        [InlineKeyboardButton("🗓️ Show current schedule", web_app=WebAppInfo(url=CURRENT_SCHEDULE_URL))],
       
        [InlineKeyboardButton("🚗 Change classes", callback_data="change_classes")],
        [InlineKeyboardButton("⚖️ Show current filters",  callback_data="show_all_filters")],
        [InlineKeyboardButton("🕒 Work schedule", callback_data="work_schedule")],
        [InlineKeyboardButton("🧩 Custom filters", web_app=WebAppInfo(url=f"{MINI_APP_BASE}/custom-filters"))],

        [
            InlineKeyboardButton("💸 Change min price", callback_data="change_price_min"),
            InlineKeyboardButton("💸 Change max price", callback_data="change_price_max"),
        ],
        [
            InlineKeyboardButton("⏳ Change gap (delay)", callback_data="change_gap"),
            InlineKeyboardButton("⌛ Change duration", callback_data="change_min_duration"),
        ],
        [
            InlineKeyboardButton("📏 Change min km", callback_data="change_min_km"),
            InlineKeyboardButton("📏 Change max km", callback_data="change_max_km"),
        ],
        [
            InlineKeyboardButton("🚫 Pickup blacklist", callback_data="pickup_blacklist"),
            InlineKeyboardButton("🚫 Dropoff blacklist", callback_data="dropoff_blacklist"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)



# --- Work schedule submenu & prompts ---
def build_work_schedule_menu(user_id: int):
    f = get_filters(user_id)
    ws = f.get("work_start", "00:00")
    we = f.get("work_end", "00:00")
    info_text = (
        "🕒 *Work schedule*\n\n"
        f"Current: `{ws}` – `{we}`\n\n"
        "Use *Update schedule* to set start & end (HH:MM)."
    )
    keyboard = [
        [InlineKeyboardButton("✏️ Update schedule", callback_data="update_work_schedule")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)

def build_work_schedule_start_prompt():
    info_text = "🕒 *Enter work START* as `HH:MM` (e.g., `08:00`)."
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)

def build_work_schedule_end_prompt():
    info_text = "🕒 *Enter work END* as `HH:MM` (e.g., `20:00`)."
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)

# --- KM prompts ---
def build_min_km_input_menu():
    info_text = (
        "📏 *Enter MIN kilometers* as a float (e.g., `50`)."
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)

def build_max_km_input_menu():
    info_text = (
        "📏 *Enter MAX kilometers* as a float (e.g., `150`)."
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_gap_input_menu():
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    info_text = (
        "✏️ *Send me the new gap (delay) in MINUTES (format: 100)*\n\n"
        "**It will be the new delay before accepting rides.**"
    )
    return info_text, InlineKeyboardMarkup(keyboard)


def build_min_price_input_menu():
    info_text = (
        "💸 *Specify a float greater than 0*\n\n"
        "**This will be the new minimum price**"
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_max_price_input_menu():
    info_text = (
        "💸 *Specify a float greater than 0*\n\n"
        "**This will be the new maximum price**"
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_min_duration_input_menu():
    info_text = (
        "⌛ *Send me the new minimal hourly rides duration in HOURS (format : 2)*\n\n"
        "**It will be the new minimum for hourly**"
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_booked_slots_menu(user_id: int):
    slots = get_booked_slots(user_id)
    if not slots:
        info_text = "📦 *Booked slots*\n\n_Aucun créneau bloqué pour l’instant._"
    else:
        info_text = "📦 *Vos créneaux bloqués*\n\n"
        for s in slots:
            info_text += f"🕒 {s['from']} → {s['to']}"
            if s['name']:
                info_text += f" ({s['name']})"
            info_text += "\n"
    keyboard = [
        [InlineKeyboardButton("➕ Add booked slot", callback_data="add_booked_slot")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]
    ]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_schedule_menu(user_id: int):
    days = get_blocked_days(user_id)
    if not days:
        info_text = "📅 *Blocked days*\n\n_Aucun jour bloqué pour le moment._"
    else:
        info_text = "📅 *Blocked days*\n\n" + "\n".join([f"• {d['day']}" for d in days])
    keyboard = []
    for d in days:
        keyboard.append([InlineKeyboardButton(f"🗑️ {d['day']}", callback_data=f"delete_day_{d['id']}")])
    keyboard.append([InlineKeyboardButton("➕ Add a day", callback_data="add_blocked_day")])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")])
    return info_text, InlineKeyboardMarkup(keyboard)


def build_classes_menu(user_id: int):
    state = get_vehicle_classes_state(user_id)
    vehicles = ["SUV", "VAN", "Business", "First", "Electric", "Sprinter"]
    info_text = "🚗 *Change Classes*\n\nClick below to toggle each class:"
    keyboard = [
        [
            InlineKeyboardButton("TRANSFER", callback_data="noop"),
            InlineKeyboardButton("HOURLY", callback_data="noop")
        ]
    ]
    for v in vehicles:
        t_state = state["transfer"].get(v, 0)
        h_state = state["hourly"].get(v, 0)
        t_symbol = "🟢" if t_state else "🔴"
        h_symbol = "🟢" if h_state else "🔴"
        keyboard.append([
            InlineKeyboardButton(f"{t_symbol} {v}", callback_data=f"toggle_transfer_{v}"),
            InlineKeyboardButton(f"{h_symbol} {v}", callback_data=f"toggle_hourly_{v}")
        ])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")])
    return info_text, InlineKeyboardMarkup(keyboard)


def build_pickup_blacklist_menu(user_id: int):
    filters_data = get_filters(user_id)
    items = (filters_data.get("pickup_blacklist") or [])
    if items:
        lines = "\n".join([f"• {x}" for x in items])
        info_text = f"🚫 *Pickup blacklist*\n\n{lines}"
    else:
        info_text = "🚫 *Pickup blacklist*\n\n_Aucune entrée pour le moment._"
    keyboard = [
        [InlineKeyboardButton("➕ Add pickup term", callback_data="add_pickup_blacklist")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_dropoff_blacklist_menu(user_id: int):
    filters_data = get_filters(user_id)
    items = (filters_data.get("dropoff_blacklist") or [])
    if items:
        lines = "\n".join([f"• {x}" for x in items])
        info_text = f"🚫 *Dropoff blacklist*\n\n{lines}"
    else:
        info_text = "🚫 *Dropoff blacklist*\n\n_Aucune entrée pour le moment._"
    keyboard = [
        [InlineKeyboardButton("➕ Add dropoff term", callback_data="add_dropoff_blacklist")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)


# --- NEW: Ends datetime menu ---
def build_ends_dt_menu(user_id: int):
    f = get_filters(user_id)
    speed = f.get("avg_speed_kmh")
    bonus = f.get("bonus_time_min")
    speed_txt = speed if speed is not None else "—"
    bonus_txt = bonus if bonus is not None else "—"
    info_text = (
        "🧮 *Ends datetime parameters*\n\n"
        f"• Average speed (km/h): {speed_txt}\n"
        f"• Bonus time (minutes): {bonus_txt}\n\n"
        "Use *Update params* to change them."
    )
    keyboard = [
        [InlineKeyboardButton("✏️ Update params", callback_data="update_ends_dt")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]
    ]
    return info_text, InlineKeyboardMarkup(keyboard)


# ---------------- Stats view ----------------
PAGE_SIZE = 5

def _build_stats_block(r: dict, tz: str) -> str:
    """
    Build one HTML block with the same look & fields as offer messages.
    """
    status = r.get("status")
    header = "✅ <b>Offer accepted</b>" if status == "accepted" else "⛔ <b>Offer rejected</b>"
    reason = r.get("rejection_reason")

    typ = safe(r.get("type"), "—").lower()
    typ_disp = "transfer" if typ == "transfer" else ("hourly" if typ == "hourly" else "—")
    vclass = safe(r.get("vehicle_class"), "—")
    price = fmt_money(r.get("price"), r.get("currency"))

    # Optional columns (present if you extended offer_logs)
    flight_number = r.get("flight_number")
    guest_reqs   = _norm_guest_requests(r.get("guest_requests"))

    pu = _esc(safe(r.get("pu_address")))
    do = _esc(r.get("do_address")) if r.get("do_address") not in (None, "", []) else None
    dist = fmt_km(r.get("estimated_distance_meters"))
    dur  = fmt_minutes(r.get("duration_minutes"))
    pu_time = _esc(fmt_dt_local(r.get("pickup_time"), tz))
    end_time = _esc(fmt_dt_local(r.get("ends_at"), tz))

    lines = [header]
    if status == "rejected" and reason:
        lines.append(f"<i>Reason:</i> {_esc(reason)}")

    lines += [
        f"🚘 <b>Type:</b> {_esc(typ_disp)}",
        f"🚗 <b>Class:</b> {_esc(vclass)}",
        f"💰 <b>Price:</b> {_esc(price)}",
    ]
    if flight_number:
        lines.append(f"✈️ <b>Flight number:</b> {_esc(flight_number)}")
    if guest_reqs:
        lines.append(f"👁️ <b>Special requests:</b> {_esc(guest_reqs)}")

    if dist != "—":
        lines.append(f"📏 <b>Distance:</b> {_esc(dist)}")
    if dur != "—":
        lines.append(f"⏱️ <b>Duration:</b> {_esc(dur)}")

    lines += [
        f"🕒 <b>Starts at:</b> {pu_time}",
        f"⏳ <b>Ends at:</b> {end_time}",
        "",
        f"⬆️ <b>Pickup:</b>\n{pu}",
    ]
    if do:
        lines += ["", f"⬇️ <b>Dropoff:</b>\n{do}"]

    return "\n".join(lines)

def build_stats_view(user_id: int, page: int = 0):
    tz = get_user_timezone(user_id)

    counts = get_offer_logs_counts(user_id)
    total = counts.get("total", 0)
    accepted = counts.get("accepted", 0)
    rejected = counts.get("rejected", 0)

    offset = page * PAGE_SIZE
    rows = get_offer_logs(user_id, limit=PAGE_SIZE, offset=offset)

    header = (
        "📊 <b>Your offers</b>\n\n"
        f"Total: <b>{total}</b>  |  ✅ <b>{accepted}</b>  |  ❌ <b>{rejected}</b>\n"
    )
    if not rows:
        info_text = header + "\n<i>No data yet.</i>"
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")]]
        return info_text, InlineKeyboardMarkup(keyboard)

    blocks = []
    for r in rows:
        blocks.append(_build_stats_block(r, tz))

    body = "\n\n".join(blocks)
    info_text = header + "\n" + body  # HTML

    has_prev = page > 0
    has_next = (offset + PAGE_SIZE) < total
    keyboard = []
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"stats_page:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"stats_page:{page+1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")])

    return info_text, InlineKeyboardMarkup(keyboard)


# ---------------- State ----------------
user_waiting_input = {}
adding_slot_step = {}
work_schedule_state = {}  # holds partial schedule input across two steps

FIELD_MAPPING = {
    "change_price_min": "price_min",
    "change_price_max": "price_max",
    "change_work_start": "work_start",   # kept for backward compatibility (unused in UI)
    "change_work_end": "work_end",       # kept for backward compatibility (unused in UI)
    "change_gap": "gap",
    "change_min_duration": "min_duration",
    "min_km": "min_km",
    "max_km": "max_km",
}


# ---------------- Utils ----------------
def validate_datetime(text: str):
    try:
        return datetime.strptime(text, "%d/%m/%Y %H:%M")
    except ValueError:
        return None


def validate_day(text: str):
    try:
        return datetime.strptime(text, "%d/%m/%Y")
    except ValueError:
        return None


# ---------------- Handlers ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)
    add_user(update.effective_user.id)
    is_active = get_active(update.effective_user.id)
    menu, status_text = build_main_menu(is_active)
    await update.message.reply_text(
        f"**Main menu**\n\nBot status: {status_text}\n\nChoose your action:",
        parse_mode="Markdown",
        reply_markup=menu,
    )


async def set_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)
    if not context.args:
        await update.message.reply_text(
            "Usage: /token <your token>\n"
            "You can paste either `Bearer <JWT>` or just the raw `<JWT>`."
        )
        return
    raw = " ".join(context.args)
    token = normalize_token(raw)
    update_token(update.effective_user.id, token)

    ok, note = validate_mobile_session(token)
    set_token_status(
        update.effective_user.id,
        "valid" if ok else ("expired" if note.startswith("unauthorized") else "unknown")
    )

    # Unpin warnings
    unpin_warning_if_any(update.effective_user.id, "no_token")
    unpin_warning_if_any(update.effective_user.id, "expired")

    if ok:
        await update.message.reply_text("✅ Mobile session token saved and validated.")
    else:
        hint = "Token looks invalid." if note.startswith("unauthorized") else "Couldn’t verify right now; I’ll retry soon."
        await update.message.reply_text(f"⚠️ Saved, but validation not OK yet. {hint}")

    info_text, menu = build_mobile_sessions_menu(update.effective_user.id)
    await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)

# bot.py (add below build_filters_menu etc.)
def _fmt_bool(b): return "ON" if b else "OFF"

def _enabled_classes_text(user_id: int) -> str:
    state = get_vehicle_classes_state(user_id)
    def line(mode):
        enabled = [name for name, on in (state.get(mode) or {}).items() if on]
        return f"{mode.capitalize()}: " + (", ".join(enabled) if enabled else "—")
    return f"{line('transfer')}\n{line('hourly')}"

# bot.py — replace build_all_filters_view with this one

def build_all_filters_view(user_id: int):
    f = get_filters(user_id) or {}

    # === Basics from user filters ===
    pickup_bl   = f.get("pickup_blacklist")  or []
    dropoff_bl  = f.get("dropoff_blacklist") or []
    ws_from     = f.get("work_start") or "—"
    ws_to       = f.get("work_end")   or "—"
    gap         = f.get("gap")
    pmin        = f.get("price_min")
    pmax        = f.get("price_max")
    kmin        = f.get("min_km")
    kmax        = f.get("max_km")
    min_dur     = f.get("min_duration")

    # === Classes (per mode & class) ===
    classes_state = get_vehicle_classes_state(user_id) or {}
    ORDER = ["SUV", "VAN", "Business", "First", "Electric", "Sprinter"]
    CLASS_ICON = {
        "SUV": "🚙",
        "VAN": "🚐",
        "Business": "💼🚘",
        "First": "🥇🚘",
        "Electric": "⚡🚗",
        "Sprinter": "🚐",
    }

    def render_mode(mode: str) -> str:
        rows = []
        mode_state = classes_state.get(mode) or {}
        for name in ORDER:
            on = bool(mode_state.get(name, 0))
            chip = "🟢 Active" if on else "🔴 Inactive"
            rows.append(f"{CLASS_ICON.get(name,'🚗')} <b>{name}</b>: {chip}")
        return "\n".join(rows) if rows else "—"

    # === Helper: quoted CSV like your screenshots ===
    def _csv_quoted(items):
        return ", ".join(f"\"{str(x)}\"" for x in (items or [])) if items else "—"

    # === End-time formulas from existing table ===
    formulas = get_endtime_formulas(user_id) or []

    # === Blocked days & booked slots ===
    days  = get_blocked_days(user_id) or []
    slots = get_booked_slots(user_id) or []

    # -------- Build HTML text --------
    lines = []

    # Blacklists
    lines.append("🚫 <b>Pickup blacklist</b>:")
    lines.append(_csv_quoted(pickup_bl))
    lines.append("")
    lines.append("🚫 <b>Dropoff blacklist</b>:")
    lines.append(_csv_quoted(dropoff_bl))
    lines.append("")

    # Prices
    lines.append("💸 <b>Prices</b>:")
    lines.append(f"• Min: {pmin}" if pmin is not None else "• Min: —")
    lines.append(f"• Max: {pmax}" if pmax is not None else "• Max: —")
    lines.append("")

    # Distance
    lines.append("📏 <b>Distance limits</b>:")
    lines.append(f"• Min: {kmin} km" if kmin is not None else "• Min: —")
    lines.append(f"• Max: {kmax} km" if kmax is not None else "• Max: —")
    lines.append("")

    # Hourly min duration
    lines.append("⌛ <b>Minimal hourly duration</b>:")
    lines.append(f"{min_dur} h" if isinstance(min_dur, (int, float)) else "—")
    lines.append("")

    # Delay (gap)
    lines.append("⏳ <b>Delay from now</b>:")
    lines.append(f"{int(gap)} minutes" if isinstance(gap, (int, float)) else "—")
    lines.append("")

    # Work schedule
    lines.append("🕒 <b>Work schedule</b>:")
    if ws_from != "—" and ws_to != "—":
        lines.append(f"from {ws_from}:00 to {ws_to}:00")
    else:
        lines.append("—")
    lines.append("")

    # Classes
    lines.append("🚗 <b>Transfer classes</b>:")
    lines.append(render_mode("transfer"))
    lines.append("")
    lines.append("🧭 <b>Hourly classes</b>:")
    lines.append(render_mode("hourly"))
    lines.append("")

    # End-time formulas
    lines.append("🧮 <b>Calculation of end time</b>:")
    if formulas:
        for idx, it in enumerate(formulas, 1):
            frm = it.get("start") or "—"
            to  = it.get("end")   or "—"
            spd = it.get("speed_kmh")
            bon = it.get("bonus_min", 0)
            lines.append(f"{idx}) {frm} → {to}")
            lines.append(f"   formula: ((distance_km / {spd} km/h) * 60) * 2 + {int(bon)} min")
    else:
        lines.append("—")
    lines.append("")

    # Blocked days
    lines.append("📅 <b>Blocked days</b>:")
    if days:
        for d in days:
            lines.append(f"• {d['day']}")
    else:
        lines.append("—")
    lines.append("")

    # Booked slots
    lines.append("📦 <b>Booked slots</b>:")
    if slots:
        for s in slots:
            nm = f" ({s['name']})" if s.get("name") else ""
            lines.append(f"• {s['from']} → {s['to']}{nm}")
    else:
        lines.append("—")

    info_text = "\n".join(lines)
    kb = [[InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")]]
    return info_text, InlineKeyboardMarkup(kb)




async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    # Activate / Deactivate
    if query.data == "activate":
        set_active(user_id, True)
        menu, status_text = build_main_menu(True)
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu
        )
        return
    if query.data == "deactivate":
        set_active(user_id, False)
        menu, status_text = build_main_menu(False)
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu
        )
        return

    # Settings
    if query.data == "settings":
        info_text, menu = build_settings_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_tz":
        user_waiting_input[user_id] = "set_timezone"
        await query.edit_message_text(
            "🌍 *Send your timezone* as IANA name (e.g., `Africa/Casablanca`, `America/Toronto`).",
            parse_mode="Markdown"
        )
        return
    
    if query.data == "notifications":
        info_text, menu = build_notifications_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    if query.data.startswith("toggle_n:"):
        kind = query.data.split(":", 1)[1]  # accepted | not_accepted | rejected
        prefs = get_notifications(user_id)
        new_val = not prefs.get(kind, True)
        set_notification(user_id, kind, new_val)
        info_text, menu = build_notifications_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Mobile sessions
    if query.data in ("mobile_sessions", "open_mobile_sessions"):
        info_text, menu = build_mobile_sessions_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_mobile_session":
        user_waiting_input[user_id] = "set_token"
        example = "Bearer eyJhbGciOi...<snip>...xyz"
        await query.edit_message_text(
        "🔑 *Paste your mobile session token*\n\n"
        "• Paste *starting from* the word **Bearer** all the way to the end.\n"
        "• Example:\n"
        f"`{example}`\n\n"
        "_Tip: If you only paste the raw JWT (`xxx.yyy.zzz`), I’ll add `Bearer` for you._",
        parse_mode="Markdown"
    )
        return
    if query.data == "show_all_filters":
        info_text, menu = build_all_filters_view(user_id)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return

    if query.data.startswith("show_offer:"):
        key = query.data.split(":", 1)[1]
        header, full = get_offer_message(user_id, key)
        if not full:
            await query.edit_message_text(
                "No details available for this offer.",
                parse_mode="HTML",
            )
            return
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Hide details", callback_data=f"hide_offer:{key}")]])
        await query.edit_message_text(full, parse_mode="HTML", reply_markup=kb)
        return

    if query.data.startswith("hide_offer:"):
        key = query.data.split(":", 1)[1]
        header, full = get_offer_message(user_id, key)
        if not header:
            header = "Details hidden."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Show details", callback_data=f"show_offer:{key}")]])
        await query.edit_message_text(header, parse_mode="HTML", reply_markup=kb)
        return

    # Stats
    if query.data == "statistic":
        info_text, menu = build_stats_view(user_id, page=0)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return
    if query.data.startswith("stats_page:"):
        try:
            page = int(query.data.split(":")[1])
        except Exception:
            page = 0
        info_text, menu = build_stats_view(user_id, page=page)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return

    # Filters menu & back
    if query.data in ("filters", "back_to_filters"):
        info_text, menu = build_filters_menu(get_filters(user_id), user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "back_to_main":
        menu, status_text = build_main_menu(get_active(user_id))
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu
        )
        return


    # Show filters summary
    if query.data == "show_filters":
        info_text, menu = build_filters_menu(get_filters(user_id), user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Gap
    if query.data == "change_gap":
        info_text, menu = build_gap_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input[user_id] = "gap"
        return

    # Min / Max price
    if query.data == "change_price_min":
        user_waiting_input[user_id] = "price_min"
        info_text, menu = build_min_price_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_price_max":
        user_waiting_input[user_id] = "price_max"
        info_text, menu = build_max_price_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Work schedule submenu & flow
    if query.data == "work_schedule":
        info_text, menu = build_work_schedule_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "update_work_schedule":
        user_waiting_input[user_id] = "work_schedule_start"
        work_schedule_state[user_id] = {}
        info_text, menu = build_work_schedule_start_prompt()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Min duration
    if query.data == "change_min_duration":
        info_text, menu = build_min_duration_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input[user_id] = "min_duration"
        return

    # Schedule (blocked days)
    if query.data == "schedule":
        info_text, menu = build_schedule_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_blocked_day":
        user_waiting_input[user_id] = "add_blocked_day"
        await query.edit_message_text(
            "📅 *Enter a day to block* in format `dd/mm/yyyy` (e.g., `31/12/2025`).",
            parse_mode="Markdown"
        )
        return
    if query.data.startswith("delete_day_"):
        try:
            day_id = int(query.data.split("_")[-1])
            delete_blocked_day(day_id)
        except:
            pass
        info_text, menu = build_schedule_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Change classes
    if query.data == "change_classes":
        info_text, menu = build_classes_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Toggle vehicle classes
    if query.data.startswith("toggle_transfer_") or query.data.startswith("toggle_hourly_"):
        parts = query.data.split("_")
        ttype = parts[1]
        vclass = parts[2]
        toggle_vehicle_class(user_id, ttype, vclass)
        info_text, menu = build_classes_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Blacklists
    if query.data == "pickup_blacklist":
        info_text, menu = build_pickup_blacklist_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "dropoff_blacklist":
        info_text, menu = build_dropoff_blacklist_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_pickup_blacklist":
        user_waiting_input[user_id] = "pickup_blacklist_add"
        await query.edit_message_text(
            "✏️ *Send pickup blacklist terms*\n"
            "• One per message (e.g., `USA`)\n"
            "• Or multiple separated by commas (e.g., `USA, NYC, Boston`)",
            parse_mode="Markdown",
        )
        return
    if query.data == "add_dropoff_blacklist":
        user_waiting_input[user_id] = "dropoff_blacklist_add"
        await query.edit_message_text(
            "✏️ *Send dropoff blacklist terms*\n"
            "• One per message (e.g., `USA`)\n"
            "• Or multiple separated by commas (e.g., `USA, NYC, Boston`)",
            parse_mode="Markdown",
        )
        return

    # Ends datetime callbacks
    if query.data == "ends_dt":
        info_text, menu = build_ends_dt_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "update_ends_dt":
        user_waiting_input[user_id] = "avg_speed_kmh"
        await query.edit_message_text(
            "🚗 *Enter average speed in km/h* (example: `50`)\n\n"
            "_This will be used to estimate ride end time._",
            parse_mode="Markdown"
        )
        return

    # KM changes
    if query.data == "change_min_km":
        user_waiting_input[user_id] = "min_km"
        info_text, menu = build_min_km_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_max_km":
        user_waiting_input[user_id] = "max_km"
        info_text, menu = build_max_km_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

async def _tap_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _capture_from_update(update)
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # Timezone input
    if user_waiting_input.get(user_id) == "set_timezone":
        tz = gettz(text)
        if not tz:
            await update.message.reply_text("❌ Unknown timezone. Please send a valid IANA name like `Africa/Casablanca`.")
            return
        set_user_timezone(user_id, text)
        await update.message.reply_text(f"✅ Timezone set to `{text}`.", parse_mode="Markdown")
        info_text, menu = build_settings_menu(user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input.pop(user_id, None)
        return

    # Token input (Mobile Sessions)
    if user_waiting_input.get(user_id) == "set_token":
        token = normalize_token(text)
        update_token(user_id, token)

        ok, note = validate_mobile_session(token)
        set_token_status(user_id, "valid" if ok else ("expired" if note.startswith("unauthorized") else "unknown"))

        unpin_warning_if_any(user_id, "no_token")
        unpin_warning_if_any(user_id, "expired")

        if ok:
            await update.message.reply_text("✅ Mobile session token saved and validated.")
        else:
            hint = "Token looks invalid." if note.startswith("unauthorized") else "Couldn’t verify right now; I’ll retry soon."
            await update.message.reply_text(f"⚠️ Saved, but validation not OK yet. {hint}")

        info_text, menu = build_mobile_sessions_menu(user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input.pop(user_id, None)
        return


    # Booked slot creation
    if user_id in adding_slot_step:
        step_info = adding_slot_step[user_id]
        if step_info["step"] == 1:
            dt = validate_datetime(text)
            if not dt:
                await update.message.reply_text("❌ Format incorrect. Utilise `dd/mm/yyyy hh:mm`.")
                return
            step_info["from"] = text
            step_info["step"] = 2
            await update.message.reply_text(
                "📅 Send *end date/time* in format `dd/mm/yyyy hh:mm`:",
                parse_mode="Markdown"
            )
            return
        if step_info["step"] == 2:
            dt = validate_datetime(text)
            if not dt:
                await update.message.reply_text("❌ Format incorrect. Utilise `dd/mm/yyyy hh:mm`.")
                return
            step_info["to"] = text
            step_info["step"] = 3
            await update.message.reply_text(
                "✏️ Optionally send a *name* for this slot, or type `-` to skip:",
                parse_mode="Markdown"
            )
            return
        if step_info["step"] == 3:
            name = None if text == "-" else text
            add_booked_slot(user_id, step_info["from"], step_info["to"], name)
            await update.message.reply_text("✅ Booked slot saved!")
            del adding_slot_step[user_id]
            info_text, menu = build_booked_slots_menu(user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

    # Work schedule 2-step flow
    if user_waiting_input.get(user_id) == "work_schedule_start":
        # validate HH:MM
        try:
            datetime.strptime(text, "%H:%M")
        except Exception:
            info_text, menu = build_work_schedule_start_prompt()
            await update.message.reply_text("❌ Invalid time. Please use `HH:MM` (e.g., `08:00`).", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        work_schedule_state[user_id] = {"start": text}
        user_waiting_input[user_id] = "work_schedule_end"
        info_text, menu = build_work_schedule_end_prompt()
        await update.message.reply_text("✅ Start time saved.", parse_mode="Markdown")
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    if user_waiting_input.get(user_id) == "work_schedule_end":
        try:
            datetime.strptime(text, "%H:%M")
        except Exception:
            info_text, menu = build_work_schedule_end_prompt()
            await update.message.reply_text("❌ Invalid time. Please use `HH:MM` (e.g., `20:00`).", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        start = (work_schedule_state.get(user_id) or {}).get("start")
        if not start:
            # safety: restart flow
            user_waiting_input[user_id] = "work_schedule_start"
            info_text, menu = build_work_schedule_start_prompt()
            await update.message.reply_text("⚠️ Let's try again. Please enter work START.", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        # Save both
        filters_data = get_filters(user_id)
        filters_data["work_start"] = start
        filters_data["work_end"]   = text
        update_filters(user_id, json.dumps(filters_data))
        # cleanup
        user_waiting_input.pop(user_id, None)
        work_schedule_state.pop(user_id, None)
        await update.message.reply_text(f"✅ Work schedule updated to `{start} – {text}`.", parse_mode="Markdown")
        info_text, menu = build_filters_menu(filters_data,user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Field updates & special inputs
    if user_id in user_waiting_input:
        field = user_waiting_input.pop(user_id)

        # Add a blocked day
        if field == "add_blocked_day":
            if not validate_day(text):
                await update.message.reply_text("❌ Wrong format. Please send a date like `31/12/2025`.")
                return
            existing = [d["day"] for d in get_blocked_days(user_id)]
            if text in existing:
                await update.message.reply_text(f"ℹ️ `{text}` is already blocked.")
            else:
                add_blocked_day(user_id, text)
                await update.message.reply_text(f"✅ Day `{text}` added to blocked days.")
            info_text, menu = build_schedule_menu(user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Add to blacklists (single value or comma-separated list)
        if field in ("pickup_blacklist_add", "dropoff_blacklist_add"):
            # Normalize to a list of non-empty terms
            items = [p.strip() for p in text.split(",") if p.strip()]
            if not items:
                await update.message.reply_text(
                    "❌ Please send at least one value (e.g., `USA` or `USA, NYC`).",
                    parse_mode="Markdown",
                )
                # keep the state so they can resend
                user_waiting_input[user_id] = field
                return

            filters_data = get_filters(user_id)
            key = "pickup_blacklist" if field == "pickup_blacklist_add" else "dropoff_blacklist"
            current = filters_data.get(key, []) or []

            # Case-insensitive dedupe
            current_lower = {x.lower() for x in current}
            added, skipped = [], []
            for item in items:
                if item.lower() in current_lower:
                    skipped.append(item)
                else:
                    current.append(item)
                    current_lower.add(item.lower())
                    added.append(item)

            filters_data[key] = current
            update_filters(user_id, json.dumps(filters_data))

            msg_lines = []
            if added:
                msg_lines.append("✅ Added: " + ", ".join(f"`{a}`" for a in added))
            if skipped:
                msg_lines.append("ℹ️ Already present: " + ", ".join(f"`{s}`" for s in skipped))
            await update.message.reply_text(
                "\n".join(msg_lines) if msg_lines else "Nothing to add.",
                parse_mode="Markdown"
            )

            # Show menu again
            if key == "pickup_blacklist":
                info_text, menu = build_pickup_blacklist_menu(user_id)
            else:
                info_text, menu = build_dropoff_blacklist_menu(user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # NEW: Ends datetime step 1 (speed)
        if field == "avg_speed_kmh":
            try:
                speed = float(text)
                if speed <= 0:
                    raise ValueError()
            except:
                await update.message.reply_text("❌ Please send a float greater than 0 for *average speed (km/h)*.")
                user_waiting_input[user_id] = "avg_speed_kmh"
                return
            filters_data = get_filters(user_id)
            filters_data["avg_speed_kmh"] = speed
            update_filters(user_id, json.dumps(filters_data))
            user_waiting_input[user_id] = "bonus_time_min"
            await update.message.reply_text(
                "⏱️ *Enter bonus time in minutes* (example: `60`)\n\n"
                "_This is added to the estimated duration._",
                parse_mode="Markdown"
            )
            return

        # NEW: Ends datetime step 2 (bonus)
        if field == "bonus_time_min":
            try:
                bonus = float(text)
                if bonus < 0:
                    raise ValueError()
            except:
                await update.message.reply_text("❌ Please send a non-negative float for *bonus time (minutes)*.")
                user_waiting_input[user_id] = "bonus_time_min"
                return
            filters_data = get_filters(user_id)
            filters_data["bonus_time_min"] = bonus
            update_filters(user_id, json.dumps(filters_data))
            await update.message.reply_text("✅ Ends datetime parameters saved.")
            info_text, menu = build_ends_dt_menu(user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Work start/end (legacy direct, not used in UI now)
        if field in ("work_start", "work_end"):
            try:
                datetime.strptime(text, "%H:%M")
            except Exception:
                await update.message.reply_text("❌ Please send time as `HH:MM` (e.g., `08:00`).")
                return
            filters_data = get_filters(user_id)
            filters_data[field] = text
            update_filters(user_id, json.dumps(filters_data))
            await update.message.reply_text(f"✅ Updated {field} to {text}")
            info_text, menu = build_filters_menu(filters_data,user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Numeric fields
        value = text
        if field in ["price_min", "price_max", "gap", "min_duration", "min_km", "max_km"]:
            try:
                val = float(value)
                if val <= 0:
                    raise ValueError()
            except:
                await update.message.reply_text("❌ Please send a float greater than 0 (e.g., `50`).")
                return
            value = val

        filters_data = get_filters(user_id)
        filters_data[field] = value
        update_filters(user_id, json.dumps(filters_data))
        await update.message.reply_text(f"✅ Updated {field} to {value}")
        info_text, menu = build_filters_menu(filters_data,user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
def build_notifications_menu(user_id: int):
    prefs = get_notifications(user_id)

    def line(name, flag):
        return f"{'🟢' if flag else '🔴'} {name}: {'Active' if flag else 'Inactive'}"

    info_text = (
        "🔔 *Notifications*\n\n"
        f"{line('Accepted offers', prefs['accepted'])}\n"
        f"{line('Not accepted offers', prefs['not_accepted'])}\n"
        f"{line('Not valid offers', prefs['rejected'])}\n\n"
        "Choose what you want to be notified about:"
    )

    # Show enable/disable per current state
    kb = []
    kb.append([InlineKeyboardButton(
        ("Disable accepted offers" if prefs["accepted"] else "Enable accepted offers"),
        callback_data="toggle_n:accepted"
    )])
    kb.append([InlineKeyboardButton(
        ("Disable not accepted offers" if prefs["not_accepted"] else "Enable not accepted offers"),
        callback_data="toggle_n:not_accepted"
    )])
    kb.append([InlineKeyboardButton(
        ("Disable not valid offers" if prefs["rejected"] else "Enable not valid offers"),
        callback_data="toggle_n:rejected"
    )])
    kb.append([InlineKeyboardButton("⬅️ Back to Settings", callback_data="settings")])

    return info_text, InlineKeyboardMarkup(kb)


# ---------------- Main ----------------
if __name__ == "__main__":
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ALL, _tap_all), group=-1)
    app.add_handler(CallbackQueryHandler(_tap_all), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("token", set_token))
    app.add_handler(CommandHandler("settings", open_settings_cmd))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    print("✅ Bot started...")
    app.run_polling()
