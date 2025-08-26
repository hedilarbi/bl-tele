# bot.py
import json
import sqlite3
import requests
from datetime import datetime
from dateutil.tz import gettz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters
)
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
)
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env in project root

BOT_TOKEN = os.getenv("BOT_TOKEN")
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ---------------- DB Helpers ----------------
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
    action_buttons = [InlineKeyboardButton("🔴 Desactivate", callback_data="deactivate")] if is_active else [
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
    tz = get_user_timezone(user_id)
    token_status = get_token_status(user_id)
    dot = "🟢" if token_status == "valid" else ("🔴" if token_status == "expired" else "⚪")
    info_text = (
        "🔧 *Settings*\n\n"
        f"🌍 Timezone: `{tz}`\n"
        f"📱 Mobile session: {dot} ({token_status})\n\n"
        "• *Change timezone* to set your local time\n"
        "• *Mobile sessions* to add/update your Blacklane token"
    )
    keyboard = [
        [InlineKeyboardButton("🌍 Change timezone", callback_data="change_tz")],
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
    token_disp = token if token else "—"

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


def build_filters_menu(filters_data: dict):
    min_price   = filters_data.get("price_min", 0)
    max_price   = filters_data.get("price_max", 0)
    work_start  = filters_data.get("work_start", "00:00")
    work_end    = filters_data.get("work_end", "00:00")
    delay       = filters_data.get("gap", 120)
    min_duration = filters_data.get("min_duration", 0)
    min_km      = filters_data.get("min_km", 0)
    max_km      = filters_data.get("max_km", 0)

    info_text = (
        f"⚙️ *Bot filters*\n\n"
        f"💸 Min price: {min_price}\n"
        f"💸 Max price: {max_price}\n"
        f"🕒 Work schedule: {work_start} – {work_end}\n"
        f"⏳ Delay (gap): {delay} min\n"
        f"⌛ Min duration: {min_duration} h\n"
        f"📏 Min km: {min_km}\n"
        f"📏 Max km: {max_km}"
    )
    keyboard = [
        [InlineKeyboardButton("📦 Booked slots", callback_data="booked_slots")],
        [InlineKeyboardButton("📅 Schedule (blocked days)", callback_data="schedule")],
        [InlineKeyboardButton("🧮 Ends datetime", callback_data="ends_dt")],
        [InlineKeyboardButton("🚗 Change classes", callback_data="change_classes")],
        [InlineKeyboardButton("⚖️ Show current filters", callback_data="show_filters")],
        [InlineKeyboardButton("🕒 Work schedule", callback_data="work_schedule")],
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
    add_user(update.effective_user.id)
    is_active = get_active(update.effective_user.id)
    menu, status_text = build_main_menu(is_active)
    await update.message.reply_text(
        f"**Main menu**\n\nBot status: {status_text}\n\nChoose your action:",
        parse_mode="Markdown",
        reply_markup=menu,
    )


async def set_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /token <your_token>")
        return
    token = " ".join(context.args)
    update_token(update.effective_user.id, token)
    set_token_status(update.effective_user.id, "unknown")
    # Unpin any warnings
    unpin_warning_if_any(update.effective_user.id, "no_token")
    unpin_warning_if_any(update.effective_user.id, "expired")
    await update.message.reply_text("✅ Mobile session token saved.\nI’ll validate it on the next polling cycle.")
    info_text, menu = build_mobile_sessions_menu(update.effective_user.id)
    await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    # Mobile sessions
    if query.data in ("mobile_sessions", "open_mobile_sessions"):
        info_text, menu = build_mobile_sessions_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_mobile_session":
        user_waiting_input[user_id] = "set_token"
        await query.edit_message_text(
            "🔑 *Paste your mobile session token*\n\n"
            "You can also use /token <your_token> anytime.",
            parse_mode="Markdown"
        )
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
        info_text, menu = build_filters_menu(get_filters(user_id))
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

    # Booked slots
    if query.data == "booked_slots":
        info_text, menu = build_booked_slots_menu(user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_booked_slot":
        adding_slot_step[user_id] = {"step": 1}
        await query.edit_message_text(
            "✍️ Send *start date/time* in format `dd/mm/yyyy hh:mm` (your local timezone).",
            parse_mode="Markdown"
        )
        return

    # Show filters summary
    if query.data == "show_filters":
        info_text, menu = build_filters_menu(get_filters(user_id))
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


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        update_token(user_id, text)
        set_token_status(user_id, "unknown")
        # Unpin any warnings right away
        unpin_warning_if_any(user_id, "no_token")
        unpin_warning_if_any(user_id, "expired")
        await update.message.reply_text("✅ Mobile session token saved.\nI’ll validate it on the next polling cycle.")
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
        info_text, menu = build_filters_menu(filters_data)
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
            info_text, menu = build_filters_menu(filters_data)
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
        info_text, menu = build_filters_menu(filters_data)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)


# ---------------- Main ----------------
if __name__ == "__main__":
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("token", set_token))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    print("✅ Bot started...")
    app.run_polling()
