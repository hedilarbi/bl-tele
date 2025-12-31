import html
import json
import re
from typing import Optional

import requests
from dateutil.tz import gettz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from .utils import mask_secret, mask_email
from db import (
    get_bot_instance,
    add_bot_instance,
    list_bot_instances,
    get_all_users,
    get_user_row,
    get_vehicle_classes_state,
    get_booked_slots,
    get_blocked_days,
    list_user_custom_filters,
    get_endtime_formulas,
)


def _admin_owner_ok(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    bot_id = (context.application.bot_data or {}).get("bot_id")
    if not bot_id:
        return False
    bot = get_bot_instance(bot_id)
    owner_id = bot.get("owner_telegram_id") if bot else None
    return bool(owner_id and int(owner_id) == int(update.effective_user.id))


def _sanitize_bot_id(raw: str) -> str:
    base = (raw or "").strip().lower()
    base = re.sub(r"[^a-z0-9_-]+", "-", base).strip("-")
    return base or "bot"


def _tg_get_bot_info(token: str) -> Optional[dict]:
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=10)
        if 200 <= r.status_code < 300:
            j = r.json() or {}
            if j.get("ok") and isinstance(j.get("result"), dict):
                return j["result"]
    except Exception:
        pass
    return None


async def admin_add_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _admin_owner_ok(update, context):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /addbot <token> [name] [timezone]")
        return

    def _looks_like_tz(s: str) -> bool:
        if not s:
            return False
        if s.upper() in ("UTC", "GMT"):
            return True
        return ("/" in s) and (gettz(s) is not None)

    args = context.args[:]
    token_idx = next((i for i, a in enumerate(args) if ":" in a), None)
    if token_idx is None:
        await update.message.reply_text("Usage: /addbot <token> [name] [timezone]")
        return
    token = args[token_idx]
    rest = args[:token_idx] + args[token_idx + 1:]

    tz = None
    if rest and _looks_like_tz(rest[-1]):
        tz = rest[-1].strip()
        if gettz(tz) is None and tz.upper() not in ("UTC", "GMT"):
            await update.message.reply_text("Invalid timezone. Example: America/Toronto")
            return
        rest = rest[:-1]

    name = " ".join(rest).strip() or None

    info = _tg_get_bot_info(token)
    username = (info or {}).get("username")
    bot_name = name or username or "New Bot"
    bot_id = _sanitize_bot_id(username or bot_name)

    add_bot_instance(bot_id, token, bot_name, role="user", default_timezone=tz)
    tz_disp = tz or "UTC"
    bot_id_disp = html.escape(str(bot_id))
    bot_name_disp = html.escape(str(bot_name))
    tz_disp_safe = html.escape(str(tz_disp))
    await update.message.reply_text(
        f"✅ Bot added: <code>{bot_id_disp}</code> ({bot_name_disp}) tz={tz_disp_safe}",
        parse_mode="HTML",
    )


async def admin_list_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _admin_owner_ok(update, context):
        await update.message.reply_text("⛔ Not authorized.")
        return
    bots = list_bot_instances()
    if not bots:
        await update.message.reply_text("No bots registered.")
        return
    lines = ["🤖 <b>Bots</b>:"]
    buttons = []
    for b in bots:
        role = html.escape(str(b.get("role") or "user"))
        owner = html.escape(str(b.get("owner_telegram_id") or "—"))
        name = html.escape(str(b.get("bot_name") or "—"))
        tz = html.escape(str(b.get("default_timezone") or "UTC"))
        admin_active = html.escape("ON" if b.get("admin_active") else "OFF")
        bot_id = html.escape(str(b.get("bot_id") or "—"))
        token_mask = html.escape(mask_secret(str(b.get("bot_token") or ""), keep=6))
        lines.append(
            f"• <code>{bot_id}</code> ({name}) role={role} owner={owner} tz={tz} "
            f"admin_active={admin_active} token=<code>{token_mask}</code>"
        )
        btn_label = f"{b.get('bot_id') or '—'} • {b.get('bot_name') or 'Bot'}"
        buttons.append([InlineKeyboardButton(btn_label, callback_data=f"admin_botinfo:{b.get('bot_id')}")])
    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    if reply_markup:
        await update.message.reply_text("Select a bot to view details:", reply_markup=reply_markup)


async def admin_list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _admin_owner_ok(update, context):
        await update.message.reply_text("⛔ Not authorized.")
        return
    rows = get_all_users()
    if not rows:
        await update.message.reply_text("No users in DB.")
        return
    lines = [f"👤 <b>Users</b> ({len(rows)}):"]
    for bot_id, uid, _token, _filters, active in rows[:50]:
        bot_id_disp = html.escape(str(bot_id))
        uid_disp = html.escape(str(uid))
        lines.append(f"• <code>{bot_id_disp}/{uid_disp}</code> active={bool(active)}")
    if len(rows) > 50:
        lines.append(f"... and {len(rows) - 50} more")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def _render_filters(filters_raw: str) -> list[str]:
    try:
        data = json.loads(filters_raw or "{}")
        if not isinstance(data, dict):
            return [str(data)]
    except Exception:
        return [filters_raw or "—"]
    if not data:
        return ["—"]
    lines = []
    for k in sorted(data.keys()):
        v = data[k]
        if isinstance(v, (list, tuple)):
            val = ", ".join([str(x) for x in v]) if v else "—"
        else:
            val = str(v)
        lines.append(f"{k}: {val}")
    return lines


def _render_classes(state: dict, mode: str) -> str:
    items = [name for name, on in (state.get(mode) or {}).items() if on]
    return ", ".join(items) if items else "—"


async def _send_chunks(update: Update, text: str, limit: int = 3500):
    if not text:
        return
    msg = update.effective_message
    if not msg:
        return
    lines = text.split("\n")
    buf = []
    size = 0
    for line in lines:
        if size + len(line) + 1 > limit and buf:
            await msg.reply_text("\n".join(buf))
            buf = []
            size = 0
        buf.append(line)
        size += len(line) + 1
    if buf:
        await msg.reply_text("\n".join(buf))


async def admin_botinfo_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not _admin_owner_ok(update, context):
        await query.answer("Not authorized.", show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("admin_botinfo:"):
        await query.answer()
        return
    bot_id = data.split(":", 1)[1].strip()
    await query.answer()
    if not bot_id:
        msg = update.effective_message
        if msg:
            await msg.reply_text("Bot not found.")
        return
    await _send_bot_info(update, bot_id, show_full_token=False)


async def _send_bot_info(update: Update, bot_id: str, show_full_token: bool = False):
    bot = get_bot_instance(bot_id)
    if not bot:
        msg = update.effective_message
        if msg:
            await msg.reply_text("Bot not found.")
        return
    header = [
        "🧾 Bot configuration",
        f"Bot ID: {bot_id}",
        f"Name: {bot.get('bot_name') or '—'}",
        f"Role: {bot.get('role') or 'user'}",
        f"Admin active: {'ON' if bot.get('admin_active') else 'OFF'}",
        f"Owner: {bot.get('owner_telegram_id') or '—'}",
        f"Default timezone: {bot.get('default_timezone') or 'UTC'}",
        f"Bot token: {bot.get('bot_token') if show_full_token else mask_secret(bot.get('bot_token') or '', keep=6)}",
    ]
    await _send_chunks(update, "\n".join(header))

    owner_id = bot.get("owner_telegram_id")
    if not owner_id:
        await _send_chunks(update, "No user linked to this bot yet.")
        return

    user = get_user_row(bot_id, int(owner_id))
    if not user:
        await _send_chunks(update, "User row not found for this bot.")
        return

    full_name = " ".join(
        [x for x in [user.get("tg_first_name"), user.get("tg_last_name")] if x]
    ).strip()
    if not full_name:
        full_name = "—"
    user_lines = [
        "",
        "👤 User config",
        f"Telegram ID: {owner_id}",
        f"Name: {full_name}",
        f"Username: @{user.get('tg_username')}" if user.get("tg_username") else "Username: —",
        f"Active: {'ON' if user.get('active') else 'OFF'}",
        f"Timezone: {user.get('timezone') or 'UTC'}",
        f"Token status: {user.get('token_status') or 'unknown'}",
        f"Mobile token: {'set' if user.get('token') else '—'}",
        f"Portal token: {'set' if user.get('portal_token') else '—'}",
        f"BL email: {mask_email(user.get('bl_email')) if user.get('bl_email') else '—'}",
        f"BL UUID: {user.get('bl_uuid') or '—'}",
        "Notifications:",
        f"  accepted={ 'ON' if user.get('notify_accepted') else 'OFF' }",
        f"  not_accepted={ 'ON' if user.get('notify_not_accepted') else 'OFF' }",
        f"  rejected={ 'ON' if user.get('notify_rejected') else 'OFF' }",
    ]
    await _send_chunks(update, "\n".join(user_lines))

    filters_lines = ["", "⚙️ Filters"] + _render_filters(user.get("filters") or "{}")
    await _send_chunks(update, "\n".join(filters_lines))

    classes_state = get_vehicle_classes_state(bot_id, int(owner_id))
    classes_lines = [
        "",
        "🚗 Vehicle classes",
        f"Transfer: {_render_classes(classes_state, 'transfer')}",
        f"Hourly: {_render_classes(classes_state, 'hourly')}",
    ]
    await _send_chunks(update, "\n".join(classes_lines))

    days = get_blocked_days(bot_id, int(owner_id))
    slots = get_booked_slots(bot_id, int(owner_id))
    slot_parts = [f"{s['from']}->{s['to']}" for s in slots] if slots else []
    schedule_lines = [
        "",
        "📅 Schedule",
        f"Blocked days ({len(days)}): " + (", ".join([d['day'] for d in days]) if days else "—"),
        f"Booked slots ({len(slots)}): " + (", ".join(slot_parts) if slot_parts else "—"),
    ]
    await _send_chunks(update, "\n".join(schedule_lines))

    custom_filters = list_user_custom_filters(bot_id, int(owner_id)) or []
    cf_lines = ["", "🧩 Custom filters"]
    if custom_filters:
        for it in custom_filters:
            effective = bool(it.get("user_enabled")) and bool(it.get("global_enabled", 1))
            cf_lines.append(
                f"- {it.get('slug')} ({it.get('name')}): {'ON' if effective else 'OFF'}"
            )
    else:
        cf_lines.append("—")
    await _send_chunks(update, "\n".join(cf_lines))

    formulas = get_endtime_formulas(bot_id, int(owner_id)) or []
    ef_lines = ["", "🧮 Endtime formulas"]
    if formulas:
        for f in formulas:
            win = f"{f.get('start')}-{f.get('end')}" if f.get("start") and f.get("end") else "else"
            ef_lines.append(
                f"- {win}: {f.get('speed_kmh')} km/h + {f.get('bonus_min', 0)} min (prio {f.get('priority', 0)})"
            )
    else:
        ef_lines.append("—")
    await _send_chunks(update, "\n".join(ef_lines))


async def admin_bot_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _admin_owner_ok(update, context):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /botinfo <bot_id> [--full]")
        return

    bot_id = context.args[0].strip()
    show_full_token = False
    if len(context.args) > 1:
        flag = context.args[1].strip().lower()
        show_full_token = flag in ("full", "--full", "token", "--token")
    await _send_bot_info(update, bot_id, show_full_token=show_full_token)
