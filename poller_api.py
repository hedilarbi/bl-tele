# poller_api.py
"""
Internal API for VPS poller nodes.

Auth: Authorization: poller <POLLER_API_KEY>

Mount in webapp_api.py:
    from poller_api import router as poller_router
    app.include_router(poller_router)
"""
import os
import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from db import (
    get_all_users_with_bot_admin_active,
    get_user_timezone,
    get_mobile_headers,
    get_mobile_auth,
    get_endtime_formulas,
    get_vehicle_classes_state,
    get_booked_slots,
    get_blocked_days,
    get_bl_uuid,
    get_bl_account_full,
    get_token_auto_refresh,
    get_bot_token,
    get_notifications,
    get_pinned_warnings,
    get_portal_token,
    list_user_custom_filters,
    set_token_status,
    set_token_auto_refresh,
    update_token,
    update_portal_token,
    log_offer_decision,
    save_pinned_warning,
    clear_pinned_warning,
)
from poller_core.filters import _get_enabled_filter_slugs
from poller_core.notify import pin_warning_if_needed, unpin_warning_if_any

_POLLER_API_KEY = os.getenv("POLLER_API_KEY", "")

router = APIRouter(prefix="/internal/poller")


def _auth(authorization: Optional[str]):
    if not _POLLER_API_KEY:
        return  # key not configured → open (dev/local)
    if not authorization or not authorization.lower().startswith("poller "):
        raise HTTPException(401, "Use Authorization: poller <key>")
    if authorization[7:].strip() != _POLLER_API_KEY:
        raise HTTPException(403, "Invalid poller key")


# ── Users list ────────────────────────────────────────────────────────────────

@router.get("/users")
def get_users(Authorization: Optional[str] = Header(default=None)):
    _auth(Authorization)
    rows = get_all_users_with_bot_admin_active() or []
    users = []
    for row in rows:
        if len(row) >= 7:
            bot_id, telegram_id, token, filters_json, active, bot_admin_active, cache_version = row[:7]
        else:
            bot_id, telegram_id, token, filters_json, active, bot_admin_active = row[:6]
            cache_version = 0
        users.append({
            "bot_id": bot_id,
            "telegram_id": telegram_id,
            "token": token,
            "filters_json": filters_json,
            "active": bool(active),
            "bot_admin_active": bool(bot_admin_active),
            "cache_version": int(cache_version or 0),
        })
    return users


# ── Full user config (all data a poller needs for one user) ──────────────────

@router.get("/user/{bot_id}/{telegram_id}/config")
def get_user_config(
    bot_id: str,
    telegram_id: int,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    try:
        tz_name = get_user_timezone(bot_id, telegram_id) or "UTC"
        mobile_headers = get_mobile_headers(bot_id, telegram_id)
        mobile_auth = get_mobile_auth(bot_id, telegram_id) or {}
        endtime_formulas = get_endtime_formulas(bot_id, telegram_id) or []
        class_state = get_vehicle_classes_state(bot_id, telegram_id) or {}
        booked_slots = get_booked_slots(bot_id, telegram_id) or []
        blocked_days_raw = get_blocked_days(bot_id, telegram_id) or []
        blocked_days = [
            d["day"] for d in blocked_days_raw
            if isinstance(d, dict) and d.get("day")
        ]
        bl_uuid = get_bl_uuid(bot_id, telegram_id)
        creds = get_bl_account_full(bot_id, telegram_id)
        if isinstance(creds, (list, tuple)) and len(creds) >= 2:
            email, password = creds[0], creds[1]
        elif isinstance(creds, dict):
            email = creds.get("email")
            password = creds.get("password")
        else:
            email = password = None
        token_auto_refresh = bool(get_token_auto_refresh(bot_id, telegram_id))
        bot_token = get_bot_token(bot_id)
        notifications = get_notifications(bot_id, telegram_id) or {}
        pinned_warnings = get_pinned_warnings(bot_id, telegram_id) or {}
        portal_token = get_portal_token(bot_id, telegram_id)
        user_custom_filters = list_user_custom_filters(bot_id, telegram_id) or []
        # user_cfilters: {slug: filter_dict} — enabled filters ready for processing
        user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
    except Exception as e:
        logging.exception("Config fetch error for %s/%s: %s", bot_id, telegram_id, e)
        raise HTTPException(500, f"Config fetch error: {e}")

    return {
        "tz_name": tz_name,
        "mobile_headers": mobile_headers,
        "mobile_auth": mobile_auth,
        "endtime_formulas": endtime_formulas,
        "class_state": class_state,
        "booked_slots": booked_slots,
        "blocked_days": blocked_days,
        "bl_uuid": bl_uuid,
        "email": email,
        "password": password,
        "token_auto_refresh": token_auto_refresh,
        "bot_token": bot_token,
        "notifications": notifications,
        "pinned_warnings": pinned_warnings,
        "portal_token": portal_token,
        "user_custom_filters": user_custom_filters,
        "user_cfilters": user_cfilters,
    }


# ── Token writes ─────────────────────────────────────────────────────────────

class TokenStatusBody(BaseModel):
    status: str  # "valid" | "expired" | "unknown"


@router.post("/user/{bot_id}/{telegram_id}/token-status")
def post_token_status(
    bot_id: str,
    telegram_id: int,
    body: TokenStatusBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    set_token_status(bot_id, telegram_id, body.status)
    return {"ok": True}


class SaveTokenBody(BaseModel):
    token: str
    mobile_headers: Optional[dict] = None
    auth_meta: Optional[dict] = None


@router.post("/user/{bot_id}/{telegram_id}/token")
def post_save_token(
    bot_id: str,
    telegram_id: int,
    body: SaveTokenBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    update_token(
        bot_id, telegram_id,
        body.token,
        headers=body.mobile_headers,
        auth_meta=body.auth_meta or None,
    )
    set_token_status(bot_id, telegram_id, "valid")
    return {"ok": True}


class AutoRefreshBody(BaseModel):
    enabled: bool


@router.post("/user/{bot_id}/{telegram_id}/auto-refresh")
def post_auto_refresh(
    bot_id: str,
    telegram_id: int,
    body: AutoRefreshBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    set_token_auto_refresh(bot_id, telegram_id, body.enabled)
    return {"ok": True}


class SavePortalTokenBody(BaseModel):
    portal_token: str


@router.post("/user/{bot_id}/{telegram_id}/portal-token")
def post_portal_token(
    bot_id: str,
    telegram_id: int,
    body: SavePortalTokenBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    update_portal_token(bot_id, telegram_id, body.portal_token)
    return {"ok": True}


# ── Pinned warnings (low-level: save / clear) ─────────────────────────────────

class SavePinnedWarningBody(BaseModel):
    bot_id: str
    telegram_id: int
    kind: str       # "no_token" | "expired"
    message_id: int


@router.post("/pinned-warning/save")
def post_save_pinned_warning(
    body: SavePinnedWarningBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    save_pinned_warning(body.bot_id, body.telegram_id, body.kind, body.message_id)
    return {"ok": True}


class ClearPinnedWarningBody(BaseModel):
    bot_id: str
    telegram_id: int
    kind: str


@router.post("/pinned-warning/clear")
def post_clear_pinned_warning(
    body: ClearPinnedWarningBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    clear_pinned_warning(body.bot_id, body.telegram_id, body.kind)
    return {"ok": True}


# ── Pinned warnings (high-level: EC2 sends Telegram + saves to DB) ────────────

class PinWarningBody(BaseModel):
    bot_id: str
    telegram_id: int
    kind: str  # "no_token" | "expired"


@router.post("/pin-warning")
def post_pin_warning(
    body: PinWarningBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    try:
        pin_warning_if_needed(body.bot_id, body.telegram_id, body.kind)
    except Exception as e:
        logging.warning("pin_warning_if_needed failed: %s", e)
    return {"ok": True}


class UnpinWarningBody(BaseModel):
    bot_id: str
    telegram_id: int
    kind: str


@router.post("/unpin-warning")
def post_unpin_warning(
    body: UnpinWarningBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    try:
        unpin_warning_if_any(body.bot_id, body.telegram_id, body.kind)
    except Exception as e:
        logging.warning("unpin_warning_if_any failed: %s", e)
    return {"ok": True}


# ── Offer logging ─────────────────────────────────────────────────────────────

class OfferLogBody(BaseModel):
    bot_id: str
    telegram_id: int
    offer: dict
    status: str
    reason: Optional[str] = None
    notify_text: Optional[str] = None


@router.post("/offer-log")
def post_offer_log(
    body: OfferLogBody,
    Authorization: Optional[str] = Header(default=None),
):
    _auth(Authorization)
    try:
        log_offer_decision(
            body.bot_id, body.telegram_id,
            body.offer, body.status,
            body.reason, body.notify_text,
        )
    except Exception as e:
        logging.warning("log_offer_decision failed: %s", e)
    return {"ok": True}
