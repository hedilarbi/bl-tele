import json
import re
from datetime import datetime
from typing import Optional

import requests
from dateutil.tz import gettz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ApplicationHandlerStop

from .capture import _capture_from_update
from .menus import (
    build_main_menu,
    build_settings_menu,
    build_mobile_sessions_menu,
    build_filters_menu,
    build_work_schedule_menu,
    build_work_schedule_start_prompt,
    build_work_schedule_end_prompt,
    build_min_km_input_menu,
    build_max_km_input_menu,
    build_gap_input_menu,
    build_min_price_input_menu,
    build_max_price_input_menu,
    build_min_duration_input_menu,
    build_booked_slots_menu,
    build_schedule_menu,
    build_classes_menu,
    build_pickup_blacklist_menu,
    build_dropoff_blacklist_menu,
    build_flight_blacklist_menu,
    build_ends_dt_menu,
    build_stats_view,
    build_stats_summary,
    build_all_filters_view,
    build_notifications_menu,
)
from .state import user_waiting_input, adding_slot_step, work_schedule_state, _ctx_bot_id, _state_key
from .storage import get_active, set_active, get_filters
from .utils import (
    parse_mobile_session_dump,
    parse_mobile_auth_meta,
    parse_mobile_auth_material,
    validate_mobile_session,
    validate_datetime,
    validate_day,
)
from db import (
    add_user,
    assign_bot_owner,
    update_token,
    update_filters,
    add_booked_slot,
    get_blocked_days,
    add_blocked_day,
    delete_blocked_day,
    toggle_vehicle_class,
    set_user_timezone,
    set_token_status,
    get_pinned_warnings,
    clear_pinned_warning,
    get_notifications,
    set_notification,
    get_offer_message,
    get_bot_instance,
    get_mobile_auth,
    get_mobile_headers,
    get_user_row,
)


def _resolve_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app_bot_id = _ctx_bot_id(context)
    role = (context.application.bot_data or {}).get("role", "user")
    if role == "admin":
        target_bot_id = (context.user_data or {}).get("admin_target_bot_id")
        target_user_id = (context.user_data or {}).get("admin_target_user_id")
        if target_bot_id and target_user_id:
            return app_bot_id, target_bot_id, int(target_user_id), True
        return app_bot_id, None, None, False
    return app_bot_id, app_bot_id, update.effective_user.id, False


def unpin_warning_if_any(bot_id: Optional[str], telegram_id: int, kind: str, bot_token: Optional[str] = None):
    # kind: "no_token" | "expired"
    if not bot_token or not bot_id:
        return
    ids = get_pinned_warnings(bot_id, telegram_id)
    message_id = ids["no_token_msg_id"] if kind == "no_token" else ids["expired_msg_id"]
    if not message_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/unpinChatMessage",
            json={"chat_id": telegram_id, "message_id": message_id},
            timeout=10,
        )
    except Exception:
        pass
    clear_pinned_warning(bot_id, telegram_id, kind)


def _merge_auth_meta(existing: Optional[dict], patch: Optional[dict]) -> dict:
    out = dict(existing or {})
    p = dict(patch or {})
    if p.get("refresh_token"):
        out["refresh_token"] = str(p["refresh_token"]).strip()
    if p.get("client_id"):
        out["client_id"] = str(p["client_id"]).strip()
    p_oh = p.get("oauth_headers")
    if isinstance(p_oh, dict):
        merged_oh = dict(out.get("oauth_headers") or {})
        for k, v in p_oh.items():
            if k and v is not None:
                merged_oh[k] = v
        if merged_oh:
            out["oauth_headers"] = merged_oh
    return out


def _is_bearer_like(token: Optional[str]) -> bool:
    return bool(token and isinstance(token, str) and token.lower().startswith("bearer "))


def _missing_refresh_parts(auth_meta: dict) -> list[str]:
    missing = []
    if not (auth_meta or {}).get("refresh_token"):
        missing.append("refresh_token")
    if not (auth_meta or {}).get("client_id"):
        missing.append("client_id")
    if not isinstance((auth_meta or {}).get("oauth_headers"), dict) or not (auth_meta or {}).get("oauth_headers"):
        missing.append("oauth_headers")
    return missing


def _validation_note_hint(note: str) -> str:
    if not note:
        return "Saved but couldn't verify right now."
    if note.startswith("unauthorized:401"):
        return "Token looks invalid or expired."
    if note.startswith("unauthorized:403"):
        return "Token seems valid but access is forbidden (403) from this environment/account."
    if note.startswith("upstream:410"):
        return "HTTP 410 means the probe endpoint is gone (token may still be usable for other paths)."
    if note.startswith("upstream:404") or note.startswith("upstream:405"):
        return "Probe endpoint/method not available right now."
    if note.startswith("network:"):
        return "Validation request hit a network timeout/error."
    return "Saved but couldn't verify right now."


def _save_mobile_input_for_user(
    bot_id: str,
    user_id: int,
    raw: str,
    bot_token: Optional[str] = None,
) -> str:
    row = get_user_row(bot_id, user_id) or {}
    existing_token = (row.get("token") or "").strip()
    existing_status = (row.get("token_status") or "unknown").strip() or "unknown"
    existing_headers = get_mobile_headers(bot_id, user_id)
    existing_auth = get_mobile_auth(bot_id, user_id) or {}

    token_from_dump, headers_from_dump = parse_mobile_session_dump(raw)
    auth_meta_from_dump = parse_mobile_auth_meta(raw, headers_from_dump if headers_from_dump else None)
    material = parse_mobile_auth_material(raw)

    token_candidate = ""
    if _is_bearer_like(token_from_dump):
        token_candidate = token_from_dump.strip()
    elif _is_bearer_like(material.get("token")):
        token_candidate = material.get("token", "").strip()

    auth_patch = {}
    if material.get("refresh_token"):
        auth_patch["refresh_token"] = material["refresh_token"]
    if material.get("client_id"):
        auth_patch["client_id"] = material["client_id"]
    auth_patch = _merge_auth_meta(auth_patch, auth_meta_from_dump)
    merged_auth = _merge_auth_meta(existing_auth, auth_patch)

    # Only replace mobile_headers when this input also includes a bearer token
    # (oauth-only dumps should not overwrite P1 request headers).
    final_headers = existing_headers
    if token_candidate and headers_from_dump:
        final_headers = headers_from_dump

    final_token = token_candidate or existing_token

    changed = (
        bool(token_candidate)
        or bool(headers_from_dump and token_candidate)
        or (json.dumps(merged_auth, sort_keys=True, ensure_ascii=True) != json.dumps(existing_auth or {}, sort_keys=True, ensure_ascii=True))
    )
    if not changed:
        return "❌ Nothing usable found. Send full dump, OAuth JSON, or refresh_token/client_id."

    update_token(
        bot_id,
        user_id,
        final_token,
        headers=final_headers,
        auth_meta=merged_auth if merged_auth else None,
    )

    if token_candidate:
        ok, note = validate_mobile_session(final_token, final_headers if final_headers else None)
        if ok:
            next_status = "valid"
        elif note.startswith("unauthorized:401"):
            next_status = "expired"
        else:
            next_status = "unknown"
        set_token_status(
            bot_id,
            user_id,
            next_status,
        )
        if ok:
            unpin_warning_if_any(bot_id, user_id, "no_token", bot_token)
            unpin_warning_if_any(bot_id, user_id, "expired", bot_token)
            return "✅ Mobile token saved and validated."
        hint = _validation_note_hint(note)
        return f"⚠️ Token saved, validation not OK yet ({note}). {hint}"

    # Auth material only: keep previous status until poller refreshes successfully.
    set_token_status(bot_id, user_id, existing_status)
    missing = _missing_refresh_parts(merged_auth)
    if missing:
        return f"✅ Partial refresh material saved. Missing: {', '.join(missing)}."
    return "✅ OAuth refresh material saved. Poller will auto-renew access token."


async def open_settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app_bot_id, bot_id, user_id, admin_mode = _resolve_target(update, context)
    _capture_from_update(update, app_bot_id)
    if bot_id is None or user_id is None:
        await update.message.reply_text("Select a bot first with /listbots.")
        return
    add_user(bot_id, user_id)
    info_text, menu = build_settings_menu(user_id, bot_id, allow_tz_change=admin_mode, as_user_id=user_id)
    await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = (context.application.bot_data or {}).get("role", "user")
    bot_id = (context.application.bot_data or {}).get("bot_id")
    user_id = update.effective_user.id

    if role == "admin":
        if bot_id:
            ok, reason = assign_bot_owner(bot_id, user_id)
            if not ok and reason == "bot_already_owned":
                await update.message.reply_text("⛔ Admin bot is already assigned to another user.")
                return
        await update.message.reply_text(
            "🛠️ <b>Admin Bot</b>\n\n"
            "Commands:\n"
            "• /addbot <code>&lt;token&gt; [name] [timezone]</code>\n"
            "• /listbots\n"
            "• /botinfo <code>&lt;bot_id&gt;</code>\n"
            "• /listusers\n",
            parse_mode="HTML",
        )
        return

    if not bot_id:
        await update.message.reply_text("❌ Bot not registered. Please contact admin.")
        return

    ok, reason = assign_bot_owner(bot_id, user_id)
    if not ok:
        if reason == "bot_already_owned":
            await update.message.reply_text("⛔ This bot is already assigned to another user.")
        else:
            await update.message.reply_text("❌ Bot not registered. Please contact admin.")
        return

    _capture_from_update(update, bot_id)
    add_user(bot_id, user_id)
    is_active = get_active(bot_id, user_id)
    menu, status_text = build_main_menu(is_active)
    await update.message.reply_text(
        f"**Main menu**\n\nBot status: {status_text}\n\nChoose your action:",
        parse_mode="Markdown",
        reply_markup=menu,
    )


async def set_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app_bot_id, bot_id, user_id, _admin_mode = _resolve_target(update, context)
    _capture_from_update(update, app_bot_id)
    if bot_id is None or user_id is None:
        await update.message.reply_text("Select a bot first with /listbots.")
        return
    message_text = (update.message.text or "") if update and update.message else ""
    raw = re.sub(r"(?is)^/token(?:@\w+)?\s*", "", message_text, count=1).strip()
    if not raw and context.args:
        # fallback when command text is not available from adapter
        raw = " ".join(context.args).strip()
    if not raw:
        await update.message.reply_text(
            "Usage: /token <full HTTP dump>\n"
            "Accepted: full HTTP dump, OAuth JSON, or refresh_token/client_id pieces."
        )
        return
    bot_token = context.bot.token if context and context.bot else None
    add_user(bot_id, user_id)
    result_msg = _save_mobile_input_for_user(bot_id, user_id, raw, bot_token=bot_token)
    await update.message.reply_text(result_msg)

    info_text, menu = build_mobile_sessions_menu(bot_id, user_id)
    await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app_bot_id, bot_id, user_id, admin_mode = _resolve_target(update, context)
    _capture_from_update(update, app_bot_id)
    query = update.callback_query
    await query.answer()
    if bot_id is None or user_id is None:
        await query.edit_message_text("Select a bot first with /listbots.", parse_mode="Markdown")
        return
    state_key = _state_key(bot_id, user_id)

    # Activate / Deactivate
    if query.data == "activate":
        set_active(bot_id, user_id, True)
        menu, status_text = build_main_menu(True)
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu,
        )
        return
    if query.data == "deactivate":
        set_active(bot_id, user_id, False)
        menu, status_text = build_main_menu(False)
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu,
        )
        return

    # Settings
    if query.data == "settings":
        info_text, menu = build_settings_menu(user_id, bot_id, allow_tz_change=admin_mode, as_user_id=user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_tz":
        if not admin_mode:
            await query.edit_message_text(
                "🌍 Timezone is managed by the admin for this bot.",
                parse_mode="Markdown",
            )
            return
        user_waiting_input[state_key] = "set_timezone"
        await query.edit_message_text(
            "🌍 *Send timezone* as IANA name (e.g., `Africa/Casablanca`, `America/Toronto`).",
            parse_mode="Markdown",
        )
        return

    if query.data == "notifications":
        info_text, menu = build_notifications_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    if query.data.startswith("toggle_n:"):
        kind = query.data.split(":", 1)[1]
        prefs = get_notifications(bot_id, user_id)
        new_val = not prefs.get(kind, True)
        set_notification(bot_id, user_id, kind, new_val)
        info_text, menu = build_notifications_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Mobile sessions
    if query.data in ("mobile_sessions", "open_mobile_sessions"):
        info_text, menu = build_mobile_sessions_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_mobile_session":
        user_waiting_input[state_key] = "set_token"
        await query.edit_message_text(
            "🔑 *Send mobile auth input*\n\n"
            "Accepted:\n"
            "• full HTTP dump (/rides, /offers, /oauth/token)\n"
            "• OAuth JSON\n"
            "• pieces: `refresh_token=...`, `client_id=...`, then headers block",
            parse_mode="Markdown",
        )
        return
    if query.data == "show_all_filters":
        info_text, menu = build_all_filters_view(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return

    if query.data.startswith("show_offer:"):
        key = query.data.split(":", 1)[1]
        header, full = get_offer_message(bot_id, user_id, key)
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
        header, full = get_offer_message(bot_id, user_id, key)
        if not header:
            header = "Details hidden."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Show details", callback_data=f"show_offer:{key}")]])
        await query.edit_message_text(header, parse_mode="HTML", reply_markup=kb)
        return

    # Stats
    if query.data == "statistic":
        info_text, menu = build_stats_summary(bot_id, user_id, range_key="today")
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return
    if query.data.startswith("stats_range:"):
        range_key = query.data.split(":", 1)[1]
        info_text, menu = build_stats_summary(bot_id, user_id, range_key=range_key)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return
    if query.data == "checked_statistic":
        info_text, menu = build_stats_view(bot_id, user_id, page=0)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return
    if query.data.startswith("stats_page:"):
        try:
            page = int(query.data.split(":")[1])
        except Exception:
            page = 0
        info_text, menu = build_stats_view(bot_id, user_id, page=page)
        await query.edit_message_text(info_text, parse_mode="HTML", reply_markup=menu)
        return

    # Filters menu & back
    if query.data in ("filters", "back_to_filters"):
        info_text, menu = build_filters_menu(get_filters(bot_id, user_id), user_id, bot_id, as_user_id=user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "back_to_main":
        menu, status_text = build_main_menu(get_active(bot_id, user_id))
        await query.edit_message_text(
            f"**Main menu**\n\nBot status: {status_text}",
            parse_mode="Markdown",
            reply_markup=menu,
        )
        return

    # Show filters summary
    if query.data == "show_filters":
        info_text, menu = build_filters_menu(get_filters(bot_id, user_id), user_id, bot_id, as_user_id=user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Gap
    if query.data == "change_gap":
        info_text, menu = build_gap_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input[state_key] = "gap"
        return

    # Min / Max price
    if query.data == "change_price_min":
        user_waiting_input[state_key] = "price_min"
        info_text, menu = build_min_price_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_price_max":
        user_waiting_input[state_key] = "price_max"
        info_text, menu = build_max_price_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Work schedule submenu & flow
    if query.data == "work_schedule":
        info_text, menu = build_work_schedule_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "update_work_schedule":
        user_waiting_input[state_key] = "work_schedule_start"
        work_schedule_state[state_key] = {}
        info_text, menu = build_work_schedule_start_prompt()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Min duration
    if query.data == "change_min_duration":
        info_text, menu = build_min_duration_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input[state_key] = "min_duration"
        return

    # Schedule (blocked days)
    if query.data == "schedule":
        info_text, menu = build_schedule_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_blocked_day":
        user_waiting_input[state_key] = "add_blocked_day"
        await query.edit_message_text(
            "📅 *Enter a day to block* in format `dd/mm/yyyy` (e.g., `31/12/2025`).",
            parse_mode="Markdown",
        )
        return
    if query.data.startswith("delete_day_"):
        try:
            day_id = int(query.data.split("_")[-1])
            delete_blocked_day(bot_id, day_id)
        except Exception:
            pass
        info_text, menu = build_schedule_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Change classes
    if query.data == "change_classes":
        info_text, menu = build_classes_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Toggle vehicle classes
    if query.data.startswith("toggle_transfer_") or query.data.startswith("toggle_hourly_"):
        parts = query.data.split("_")
        ttype = parts[1]
        vclass = parts[2]
        toggle_vehicle_class(bot_id, user_id, ttype, vclass)
        info_text, menu = build_classes_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Blacklists
    if query.data == "pickup_blacklist":
        info_text, menu = build_pickup_blacklist_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "dropoff_blacklist":
        info_text, menu = build_dropoff_blacklist_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "flight_blacklist":
        info_text, menu = build_flight_blacklist_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "add_pickup_blacklist":
        user_waiting_input[state_key] = "pickup_blacklist_add"
        await query.edit_message_text(
            "✏️ *Send pickup blacklist terms*\n"
            "• One per message (e.g., `USA`)\n"
            "• Or multiple separated by commas (e.g., `USA, NYC, Boston`)",
            parse_mode="Markdown",
        )
        return
    if query.data == "add_dropoff_blacklist":
        user_waiting_input[state_key] = "dropoff_blacklist_add"
        await query.edit_message_text(
            "✏️ *Send dropoff blacklist terms*\n"
            "• One per message (e.g., `USA`)\n"
            "• Or multiple separated by commas (e.g., `USA, NYC, Boston`)",
            parse_mode="Markdown",
        )
        return
    if query.data == "add_flight_blacklist":
        user_waiting_input[state_key] = "flight_blacklist_add"
        await query.edit_message_text(
            "✏️ *Send flight numbers to block*\n"
            "• One per message (e.g., `EK 243`)\n"
            "• Or multiple separated by commas (e.g., `EK 243, BA 002`)",
            parse_mode="Markdown",
        )
        return

    # Ends datetime callbacks
    if query.data == "ends_dt":
        info_text, menu = build_ends_dt_menu(bot_id, user_id)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "update_ends_dt":
        user_waiting_input[state_key] = "avg_speed_kmh"
        await query.edit_message_text(
            "🚗 *Enter average speed in km/h* (example: `50`)\n\n"
            "_This will be used to estimate ride end time._",
            parse_mode="Markdown",
        )
        return

    # KM changes
    if query.data == "change_min_km":
        user_waiting_input[state_key] = "min_km"
        info_text, menu = build_min_km_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return
    if query.data == "change_max_km":
        user_waiting_input[state_key] = "max_km"
        info_text, menu = build_max_km_input_menu()
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return


async def _tap_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = (context.application.bot_data or {}).get("role", "user")
    bot_id = (context.application.bot_data or {}).get("bot_id")

    if role == "admin":
        return

    user = update.effective_user
    if not user or not bot_id:
        return

    bot = get_bot_instance(bot_id)
    owner_id = bot.get("owner_telegram_id") if bot else None

    is_start = bool(update.message and (update.message.text or "").strip().startswith("/start"))
    if owner_id is None:
        if not is_start:
            msg = update.effective_message
            if msg:
                await msg.reply_text("⚠️ This bot is not linked yet. Send /start to link it.")
            raise ApplicationHandlerStop
        return

    if int(owner_id) != int(user.id):
        msg = update.effective_message
        if msg:
            await msg.reply_text("⛔ This bot is already assigned to another user.")
        raise ApplicationHandlerStop

    _capture_from_update(update, bot_id)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app_bot_id, bot_id, user_id, admin_mode = _resolve_target(update, context)
    _capture_from_update(update, app_bot_id)
    if bot_id is None or user_id is None:
        await update.message.reply_text("Select a bot first with /listbots.")
        return
    state_key = _state_key(bot_id, user_id)
    text = update.message.text.strip()

    if user_waiting_input.get(state_key) == "set_timezone":
        tz = text.strip()
        if tz.upper() not in ("UTC", "GMT") and gettz(tz) is None:
            await update.message.reply_text("❌ Unknown timezone. Please send a valid IANA name like `America/Toronto`.")
            return
        set_user_timezone(bot_id, user_id, tz)
        await update.message.reply_text(f"✅ Timezone set to `{tz}`.", parse_mode="Markdown")
        info_text, menu = build_settings_menu(user_id, bot_id, allow_tz_change=admin_mode, as_user_id=user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input.pop(state_key, None)
        return

    # Token input (Mobile Sessions)
    if user_waiting_input.get(state_key) == "set_token":
        bot_token = context.bot.token if context and context.bot else None
        add_user(bot_id, user_id)
        result_msg = _save_mobile_input_for_user(bot_id, user_id, text, bot_token=bot_token)
        await update.message.reply_text(result_msg)

        info_text, menu = build_mobile_sessions_menu(bot_id, user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        user_waiting_input.pop(state_key, None)
        return

    # Booked slot creation
    if state_key in adding_slot_step:
        step_info = adding_slot_step[state_key]
        if step_info["step"] == 1:
            dt = validate_datetime(text)
            if not dt:
                await update.message.reply_text("❌ Format incorrect. Utilise `dd/mm/yyyy hh:mm`.")
                return
            step_info["from"] = text
            step_info["step"] = 2
            await update.message.reply_text(
                "📅 Send *end date/time* in format `dd/mm/yyyy hh:mm`:",
                parse_mode="Markdown",
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
                parse_mode="Markdown",
            )
            return
        if step_info["step"] == 3:
            name = None if text == "-" else text
            add_booked_slot(bot_id, user_id, step_info["from"], step_info["to"], name)
            await update.message.reply_text("✅ Booked slot saved!")
            del adding_slot_step[state_key]
            info_text, menu = build_booked_slots_menu(bot_id, user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

    # Work schedule 2-step flow
    if user_waiting_input.get(state_key) == "work_schedule_start":
        try:
            datetime.strptime(text, "%H:%M")
        except Exception:
            info_text, menu = build_work_schedule_start_prompt()
            await update.message.reply_text("❌ Invalid time. Please use `HH:MM` (e.g., `08:00`).", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        work_schedule_state[state_key] = {"start": text}
        user_waiting_input[state_key] = "work_schedule_end"
        info_text, menu = build_work_schedule_end_prompt()
        await update.message.reply_text("✅ Start time saved.", parse_mode="Markdown")
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    if user_waiting_input.get(state_key) == "work_schedule_end":
        try:
            datetime.strptime(text, "%H:%M")
        except Exception:
            info_text, menu = build_work_schedule_end_prompt()
            await update.message.reply_text("❌ Invalid time. Please use `HH:MM` (e.g., `20:00`).", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        start = (work_schedule_state.get(state_key) or {}).get("start")
        if not start:
            user_waiting_input[state_key] = "work_schedule_start"
            info_text, menu = build_work_schedule_start_prompt()
            await update.message.reply_text("⚠️ Let's try again. Please enter work START.", parse_mode="Markdown")
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return
        filters_data = get_filters(bot_id, user_id)
        filters_data["work_start"] = start
        filters_data["work_end"] = text
        update_filters(bot_id, user_id, json.dumps(filters_data))
        user_waiting_input.pop(state_key, None)
        work_schedule_state.pop(state_key, None)
        await update.message.reply_text(f"✅ Work schedule updated to `{start} – {text}`.", parse_mode="Markdown")
        info_text, menu = build_filters_menu(filters_data, user_id, bot_id, as_user_id=user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
        return

    # Field updates & special inputs
    if state_key in user_waiting_input:
        field = user_waiting_input.pop(state_key)

        # Add a blocked day
        if field == "add_blocked_day":
            if not validate_day(text):
                await update.message.reply_text("❌ Wrong format. Please send a date like `31/12/2025`.")
                return
            existing = [d["day"] for d in get_blocked_days(bot_id, user_id)]
            if text in existing:
                await update.message.reply_text(f"ℹ️ `{text}` is already blocked.")
            else:
                add_blocked_day(bot_id, user_id, text)
                await update.message.reply_text(f"✅ Day `{text}` added to blocked days.")
            info_text, menu = build_schedule_menu(bot_id, user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Add to blacklists (single value or comma-separated list)
        if field in ("pickup_blacklist_add", "dropoff_blacklist_add", "flight_blacklist_add"):
            items = [p.strip() for p in text.split(",") if p.strip()]
            if not items:
                await update.message.reply_text(
                    "❌ Please send at least one value (e.g., `USA` or `USA, NYC`).",
                    parse_mode="Markdown",
                )
                user_waiting_input[state_key] = field
                return

            filters_data = get_filters(bot_id, user_id)
            if field == "pickup_blacklist_add":
                key = "pickup_blacklist"
            elif field == "dropoff_blacklist_add":
                key = "dropoff_blacklist"
            else:
                key = "flight_blacklist"
            current = filters_data.get(key, []) or []

            def _norm_flight(s: str) -> str:
                return re.sub(r"[^A-Za-z0-9]", "", s or "").upper()

            if key == "flight_blacklist":
                current_norm = {_norm_flight(x): x for x in current if _norm_flight(x)}
            else:
                current_lower = {x.lower() for x in current}
            added, skipped = [], []
            for item in items:
                if key == "flight_blacklist":
                    norm = _norm_flight(item)
                    if not norm:
                        continue
                    disp = re.sub(r"\s+", " ", item.strip()).upper()
                    if norm in current_norm:
                        skipped.append(disp)
                    else:
                        current.append(disp)
                        current_norm[norm] = disp
                        added.append(disp)
                else:
                    if item.lower() in current_lower:
                        skipped.append(item)
                    else:
                        current.append(item)
                        current_lower.add(item.lower())
                        added.append(item)

            filters_data[key] = current
            update_filters(bot_id, user_id, json.dumps(filters_data))

            msg_lines = []
            if added:
                msg_lines.append("✅ Added: " + ", ".join(f"`{a}`" for a in added))
            if skipped:
                msg_lines.append("ℹ️ Already present: " + ", ".join(f"`{s}`" for s in skipped))
            await update.message.reply_text(
                "\n".join(msg_lines) if msg_lines else "Nothing to add.",
                parse_mode="Markdown",
            )

            if key == "pickup_blacklist":
                info_text, menu = build_pickup_blacklist_menu(bot_id, user_id)
            elif key == "dropoff_blacklist":
                info_text, menu = build_dropoff_blacklist_menu(bot_id, user_id)
            else:
                info_text, menu = build_flight_blacklist_menu(bot_id, user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Ends datetime step 1 (speed)
        if field == "avg_speed_kmh":
            try:
                speed = float(text)
                if speed <= 0:
                    raise ValueError()
            except Exception:
                await update.message.reply_text("❌ Please send a float greater than 0 for *average speed (km/h)*.")
                user_waiting_input[state_key] = "avg_speed_kmh"
                return
            filters_data = get_filters(bot_id, user_id)
            filters_data["avg_speed_kmh"] = speed
            update_filters(bot_id, user_id, json.dumps(filters_data))
            user_waiting_input[state_key] = "bonus_time_min"
            await update.message.reply_text(
                "⏱️ *Enter bonus time in minutes* (example: `60`)\n\n"
                "_This is added to the estimated duration._",
                parse_mode="Markdown",
            )
            return

        # Ends datetime step 2 (bonus)
        if field == "bonus_time_min":
            try:
                bonus = float(text)
                if bonus < 0:
                    raise ValueError()
            except Exception:
                await update.message.reply_text("❌ Please send a non-negative float for *bonus time (minutes)*.")
                user_waiting_input[state_key] = "bonus_time_min"
                return
            filters_data = get_filters(bot_id, user_id)
            filters_data["bonus_time_min"] = bonus
            update_filters(bot_id, user_id, json.dumps(filters_data))
            await update.message.reply_text("✅ Ends datetime parameters saved.")
            info_text, menu = build_ends_dt_menu(bot_id, user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Work start/end (legacy direct, not used in UI now)
        if field in ("work_start", "work_end"):
            try:
                datetime.strptime(text, "%H:%M")
            except Exception:
                await update.message.reply_text("❌ Please send time as `HH:MM` (e.g., `08:00`).")
                return
            filters_data = get_filters(bot_id, user_id)
            filters_data[field] = text
            update_filters(bot_id, user_id, json.dumps(filters_data))
            await update.message.reply_text(f"✅ Updated {field} to {text}")
            info_text, menu = build_filters_menu(filters_data, user_id, bot_id, as_user_id=user_id)
            await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
            return

        # Numeric fields
        value = text
        if field in ["price_min", "price_max", "gap", "min_duration", "min_km", "max_km"]:
            try:
                val = float(value)
                if val <= 0:
                    raise ValueError()
            except Exception:
                await update.message.reply_text("❌ Please send a float greater than 0 (e.g., `50`).")
                return
            value = val

        filters_data = get_filters(bot_id, user_id)
        filters_data[field] = value
        update_filters(bot_id, user_id, json.dumps(filters_data))
        await update.message.reply_text(f"✅ Updated {field} to {value}")
        info_text, menu = build_filters_menu(filters_data, user_id, bot_id, as_user_id=user_id)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=menu)
