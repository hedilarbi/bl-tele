import sqlite3
from typing import Optional
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from .config import BOOKED_SLOTS_URL, SCHEDULE_URL, CURRENT_SCHEDULE_URL, BL_ACCOUNT_URL, MINI_APP_BASE, _with_bot_id
from .storage import get_filters
from .utils import (
    mask_email,
    fmt_money,
    fmt_km,
    fmt_minutes,
    fmt_dt_local,
    status_emoji,
    safe,
    _esc,
    _norm_guest_requests,
)
from db import (
    DB_FILE,
    get_user_timezone,
    get_token_status,
    get_notifications,
    get_booked_slots,
    get_blocked_days,
    get_vehicle_classes_state,
    get_endtime_formulas,
    get_offer_logs_counts,
    get_offer_logs,
)


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


def build_settings_menu(user_id: int, bot_id: Optional[str] = None):
    tz = get_user_timezone(bot_id, user_id) if bot_id else "—"
    token_status = get_token_status(bot_id, user_id) if bot_id else "unknown"
    dot = "🟢" if token_status == "valid" else ("🔴" if token_status == "expired" else "⚪")

    # Notifications status summary
    prefs = get_notifications(bot_id, user_id) if bot_id else {}
    def onoff(flag): return "🟢" if flag else "🔴"
    notif_line = (
        f"{onoff(prefs.get('accepted', True))} Accepted  |  "
        f"{onoff(prefs.get('not_accepted', True))} Not accepted  |  "
        f"{onoff(prefs.get('rejected', True))} Not valid"
    )

    # BL account masked email (wrap in backticks to avoid Markdown parsing of *)
    try:
        from db import get_bl_account
    except Exception:
        def get_bl_account(_bot_id, _uid):
            return None

    try:
        acc = get_bl_account(bot_id, user_id) if bot_id else None
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
        "• *Mobile sessions* to add/update your Blacklane token\n"
        "• *BL account* to set your Blacklane email/password"
    )

    keyboard = [
        [InlineKeyboardButton("🔔 Notifications", callback_data="notifications")],
        [InlineKeyboardButton("🪪 BL account", web_app=WebAppInfo(url=_with_bot_id(BL_ACCOUNT_URL, bot_id)))],
        [InlineKeyboardButton("📱 Mobile sessions", callback_data="mobile_sessions")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")],
    ]
    return info_text, InlineKeyboardMarkup(keyboard)


def build_mobile_sessions_menu(bot_id: str, user_id: int):
    token_status = get_token_status(bot_id, user_id)
    dot = "🟢" if token_status == "valid" else ("🔴" if token_status == "expired" else "⚪")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT token FROM users WHERE bot_id = ? AND telegram_id = ?", (bot_id, user_id))
    row = c.fetchone()
    conn.close()

    token = row[0] if row else None
    # show only head/tail (6 chars) to avoid leaking the JWT in chat logs
    from .utils import mask_secret
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


def build_filters_menu(filters_data: dict, user_id: int, bot_id: Optional[str] = None):
    min_price   = filters_data.get("price_min", 0)
    max_price   = filters_data.get("price_max", 0)
    work_start  = filters_data.get("work_start", "00:00")
    work_end    = filters_data.get("work_end", "00:00")
    delay       = filters_data.get("gap", 120)
    min_duration = filters_data.get("min_duration", 0)
    min_km      = filters_data.get("min_km", 0)
    max_km      = filters_data.get("max_km", 0)

    # End-time formulas (admin-assigned)
    rows = get_endtime_formulas(bot_id, user_id) if bot_id else []
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
        [InlineKeyboardButton("📦 Booked slots", web_app=WebAppInfo(url=_with_bot_id(BOOKED_SLOTS_URL, bot_id)))],
        [InlineKeyboardButton("📅 Schedule (blocked days)", web_app=WebAppInfo(url=_with_bot_id(SCHEDULE_URL, bot_id)))],
        [InlineKeyboardButton("🗓️ Show current schedule", web_app=WebAppInfo(url=_with_bot_id(CURRENT_SCHEDULE_URL, bot_id)))],
       
        [InlineKeyboardButton("🚗 Change classes", callback_data="change_classes")],
        [InlineKeyboardButton("⚖️ Show current filters",  callback_data="show_all_filters")],
        [InlineKeyboardButton("🕒 Work schedule", callback_data="work_schedule")],
        [InlineKeyboardButton("🧩 Custom filters", web_app=WebAppInfo(url=_with_bot_id(f"{MINI_APP_BASE}/custom-filters", bot_id)))],

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
def build_work_schedule_menu(bot_id: str, user_id: int):
    f = get_filters(bot_id, user_id)
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


def build_booked_slots_menu(bot_id: str, user_id: int):
    slots = get_booked_slots(bot_id, user_id)
    if not slots:
        info_text = "📦 *Booked slots*\n\n_Aucun créneau bloqué pour l'instant._"
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


def build_schedule_menu(bot_id: str, user_id: int):
    days = get_blocked_days(bot_id, user_id)
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


def build_classes_menu(bot_id: str, user_id: int):
    state = get_vehicle_classes_state(bot_id, user_id)
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


def build_pickup_blacklist_menu(bot_id: str, user_id: int):
    filters_data = get_filters(bot_id, user_id)
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


def build_dropoff_blacklist_menu(bot_id: str, user_id: int):
    filters_data = get_filters(bot_id, user_id)
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
def build_ends_dt_menu(bot_id: str, user_id: int):
    f = get_filters(bot_id, user_id)
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
    guest_reqs = _norm_guest_requests(r.get("guest_requests"))

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


def build_stats_view(bot_id: str, user_id: int, page: int = 0):
    tz = get_user_timezone(bot_id, user_id)

    counts = get_offer_logs_counts(bot_id, user_id)
    total = counts.get("total", 0)
    accepted = counts.get("accepted", 0)
    rejected = counts.get("rejected", 0)

    offset = page * PAGE_SIZE
    rows = get_offer_logs(bot_id, user_id, limit=PAGE_SIZE, offset=offset)

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


def build_all_filters_view(bot_id: str, user_id: int):
    f = get_filters(bot_id, user_id) or {}

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
    classes_state = get_vehicle_classes_state(bot_id, user_id) or {}
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
    formulas = get_endtime_formulas(bot_id, user_id) or []

    # === Blocked days & booked slots ===
    days  = get_blocked_days(bot_id, user_id) or []
    slots = get_booked_slots(bot_id, user_id) or []

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


def build_notifications_menu(bot_id: str, user_id: int):
    prefs = get_notifications(bot_id, user_id)

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
