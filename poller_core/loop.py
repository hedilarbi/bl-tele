import json
import time
import traceback
import threading
import builtins as _builtins
from typing import Optional, List, Tuple, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from .config import (
    ALWAYS_POLL_REAL_ORDERS,
    USE_MOCK_P1,
    USE_MOCK_P2,
    ENABLE_P1,
    ENABLE_P2,
    ATHENA_PRINT_DEBUG,
    POLL_INTERVAL,
    BURST_POLL_INTERVAL_S,
    BURST_DURATION_S,
    MAX_WORKERS,
    LOG_OFFERS_PAYLOAD,
    MAX_LOGGED_OFFERS,
    OFFER_MEMORY_DEDUPE,
    ATHENA_USE_OFFERS_ETAG,
)
from .state import (
    maybe_reset_inmem_caches,
    cleanup_not_valid_cache,
    get_rides_intervals,
    maybe_cleanup_rides,
    invalidate_rides_cache,
    get_offers_etag,
    set_offers_etag,
    get_user_runtime_cache,
    set_user_runtime_cache,
    mark_token_invalid,
    clear_token_invalid,
    is_token_invalid,
    get_portal_token_mem,
    set_portal_token_mem,
    clear_portal_token_mem,
    is_token_ok_mem,
    set_token_ok_mem,
)
from .utils import _normalize_formulas
from .p1_client import get_offers_p1
from .p1_auth import get_playwright_p1_token, save_playwright_p1_token
from .p2_client import (
    _map_portal_offer,
    _athena_get_offers,
    _ensure_portal_token,
)
from .processing import debug_print_offers, _process_offers_for_user, _init_rides_cache_async
from .p1_client import warmup_p1_reserve_connection
from .filters import _get_enabled_filter_slugs
from .metrics import observe_ms
from .notify import (
    pin_warning_if_needed,
    unpin_warning_if_any,
    _resolve_bot_token,
    tg_unpin_message,
    tg_send_message,
)
from db import (
    init_db,
    get_all_users_with_bot_admin_active,
    get_booked_slots,
    get_vehicle_classes_state,
    get_user_timezone,
    get_blocked_days,
    get_pinned_warnings,
    set_token_status,
    clear_pinned_warning,
    get_endtime_formulas,
    get_bl_uuid,
    get_bl_account_full,
    get_mobile_headers,
    get_token_auto_refresh,
    set_token_auto_refresh,
)


def _quiet_print(*args, **kwargs):
    return None


def _quiet_exc(*args, **kwargs):
    return None


print = _quiet_print

_burst_until = 0.0
_burst_lock = threading.Lock()
_user_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Consecutive P1 401/403 counter per (bot_id, telegram_id).
# Token is only marked invalid after _P1_FAIL_THRESHOLD consecutive failures
# to absorb transient Auth0 hiccups that resolve on their own.
_p1_fail_counts: Dict[Tuple[str, int], int] = {}
_P1_FAIL_THRESHOLD = 3
# Consecutive auto-refresh failures per (bot_id, telegram_id).
# When auto_refresh mode is ON and Playwright re-login fails this many times,
# the mode is disabled and the user is notified to update manually.
_auto_refresh_fail_counts: Dict[Tuple[str, int], int] = {}
_AUTO_REFRESH_FAIL_THRESHOLD = 3
# Dedicated executor for parallel P1+P2 fetch inside poll_user.
# Needs MAX_WORKERS*2 slots so all concurrent users can fetch both platforms simultaneously.
_fetch_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS * 2)

# Cache for the active-users DB query: refreshed every 3 seconds instead of every 200ms.
_USERS_CACHE_TTL_S = 3.0
_users_cache_data: Optional[list] = None
_users_cache_ts: float = 0.0


def _get_users_cached() -> list:
    global _users_cache_data, _users_cache_ts
    now = time.time()
    if _users_cache_data is None or (now - _users_cache_ts) >= _USERS_CACHE_TTL_S:
        _users_cache_data = get_all_users_with_bot_admin_active()
        _users_cache_ts = now
    return _users_cache_data or []


def _bump_burst():
    global _burst_until
    if BURST_DURATION_S <= 0:
        return
    with _burst_lock:
        new_until = time.time() + BURST_DURATION_S
        if new_until > _burst_until:
            _burst_until = new_until


def _sleep_interval() -> float:
    if BURST_POLL_INTERVAL_S <= 0:
        return POLL_INTERVAL
    with _burst_lock:
        if time.time() < _burst_until:
            return BURST_POLL_INTERVAL_S
    return POLL_INTERVAL


def _poll_log(msg: str):
    _builtins.print(f"[{datetime.now()}] {msg}")


def _log_offers_found(platform: str, telegram_id: int, offers: List[dict]):
    if not offers:
        return
    _bump_burst()


def _read_portal_creds(bot_id: str, telegram_id: int) -> Tuple[Optional[str], Optional[str]]:
    try:
        creds = get_bl_account_full(bot_id, telegram_id)
    except Exception:
        creds = None
    email = password = None
    if isinstance(creds, dict):
        email = creds.get("email") or creds.get("bl_email")
        password = creds.get("password") or creds.get("bl_password")
    elif isinstance(creds, (list, tuple)):
        if len(creds) >= 2:
            email, password = creds[0], creds[1]
        elif len(creds) == 1:
            email = creds[0]
    return (email, password)


def poll_user(user):
    if len(user) >= 7:
        bot_id, telegram_id, token, filters_json, active, bot_admin_active, cache_version = user[:7]
    else:
        bot_id, telegram_id, token, filters_json, active, bot_admin_active = user[:6]
        cache_version = 0

    if not bot_admin_active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive bot {bot_id} (admin inactive)")
        return
    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    runtime_cached = get_user_runtime_cache(bot_id, telegram_id, cache_version)
    if runtime_cached:
        tz_name = runtime_cached.get("tz_name") or "UTC"
        mobile_headers = runtime_cached.get("mobile_headers")
        filters = runtime_cached.get("filters") or {}
        if "user_cfilters" in runtime_cached:
            user_cfilters = runtime_cached.get("user_cfilters") or {}
        else:
            user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
        class_state = runtime_cached.get("class_state") or {}
        booked_slots = runtime_cached.get("booked_slots") or []
        blocked_days = set(runtime_cached.get("blocked_days") or set())
        bl_uuid = runtime_cached.get("bl_uuid")
        email = runtime_cached.get("email")
        password = runtime_cached.get("password")
    else:
        tz_name = get_user_timezone(bot_id, telegram_id) or "UTC"
        mobile_headers = get_mobile_headers(bot_id, telegram_id)
        formulas_raw = get_endtime_formulas(bot_id, telegram_id)
        filters = json.loads(filters_json) if filters_json else {}
        filters["__endtime_formulas__"] = _normalize_formulas(formulas_raw)
        user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
        class_state = get_vehicle_classes_state(bot_id, telegram_id)
        booked_slots = get_booked_slots(bot_id, telegram_id)
        blocked_days = {d["day"] for d in get_blocked_days(bot_id, telegram_id)}
        bl_uuid = get_bl_uuid(bot_id, telegram_id)
        email, password = _read_portal_creds(bot_id, telegram_id)
        set_user_runtime_cache(
            bot_id,
            telegram_id,
            cache_version,
            {
                "tz_name": tz_name,
                "mobile_headers": mobile_headers,
                "filters": filters,
                "user_cfilters": user_cfilters,
                "class_state": class_state,
                "booked_slots": booked_slots,
                "blocked_days": list(blocked_days),
                "bl_uuid": bl_uuid,
                "email": email,
                "password": password,
            },
        )

    # ---------- Build busy intervals from Rides (Athena preferred) ----------
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]] = []

    has_portal_creds = bool(email and password)
    portal_token = None

    poll_real_orders = ALWAYS_POLL_REAL_ORDERS and not (USE_MOCK_P1 and USE_MOCK_P2)

    # Skip immediately if this user's token was already marked invalid (unchanged since 401/403)
    if is_token_invalid(str(bot_id), int(telegram_id), token, int(cache_version)):
        if get_token_auto_refresh(str(bot_id), int(telegram_id)) and email and password:
            # Auto-refresh is ON: clear invalid mark and pre-arm fail counter so
            # the very next 401/403 triggers Playwright immediately (skip the 3-cycle warmup).
            clear_token_invalid(str(bot_id), int(telegram_id))
            _p1_fail_counts[(str(bot_id), int(telegram_id))] = _P1_FAIL_THRESHOLD - 1
        else:
            return f"Skipped {telegram_id} (token invalid — waiting for update)"

    def _set_token_problem(kind: str):
        """Mark token invalid in memory + send pinned warning. kind: 'no_token' | 'expired'"""
        mark_token_invalid(bot_id, telegram_id, token, cache_version)
        set_token_status(bot_id, telegram_id, "expired" if kind == "expired" else "unknown")
        existing = get_pinned_warnings(bot_id, telegram_id)
        target_id = existing.get("expired_msg_id") if kind == "expired" else existing.get("no_token_msg_id")
        if not target_id:
            other = "no_token" if kind == "expired" else "expired"
            other_id = existing.get("no_token_msg_id") if kind == "expired" else existing.get("expired_msg_id")
            if other_id:
                bot_tok = _resolve_bot_token(bot_id, telegram_id)
                tg_unpin_message(bot_tok, telegram_id, other_id)
                clear_pinned_warning(bot_id, telegram_id, other)
            pin_warning_if_needed(bot_id, telegram_id, kind)

    # ---------- PLATFORM 1 OFFERS ----------
    offers_p1: List[dict] = []
    if ENABLE_P1 and USE_MOCK_P1:
        offers_p1 = [
            {
                "type": "ride",
                "id": "mock-hourly-1",
                "price": 120.9,
                "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "van",
                "_platform": "p1",
                "rides": [
                    {
                        "type": "hourly",
                        "createdAt": "2025-09-17T19:40:19Z",
                        "pickUpLocation": {
                            "name": "la Vie en Rose Quartiers Dix 30",
                            "address": "la Vie en Rose Quartiers Dix 30, Avenue des Lumières 1600, J4Y 0A5 Brossard, Québec",
                        },
                        "pickupTime": "2025-12-24T20:45:00-04:00",
                        "kmIncluded": 80,
                        "durationMinutes": 120,
                        "guestRequests": ["Baby seat", "VIP pickup"],
                        "flight": {"number": "EK 001"},
                    }
                ],
            },
            {
                "type": "ride",
                "id": "mock-transfer-1",
                "price": 90.05,
                "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "business",
                "_platform": "p1",
                "rides": [
                    {
                        "type": "transfer",
                        "createdAt": "2025-08-31T19:34:08Z",
                        "pickUpLocation": {
                            "name": "Centropolis",
                            "address": "Centropolis, Avenue Pierre-Péladeau 1799, H7T 2Y5 Laval, Québec",
                        },
                        "dropOffLocation": {
                            "name": "CF Carrefour Laval",
                            "address": "CF Carrefour Laval, Boulevard le Carrefour 3003, H7T 1C7 Laval, Québec",
                        },
                        "pickupTime": "2025-09-19T22:30:00-04:00",
                        "estimatedDurationMinutes": 32,
                        "estimatedDistanceMeters": 22266,
                        "guestRequests": [
                            "EK Complimentary",
                            "2 Guest(s)",
                            "[6432 40E AVENUE H1T 2V7 MONTREAL]"
                        ],
                        "flight": {"number": "EK 243"},
                    }
                ],
            },
        ]

    def _fetch_p1_offers_real():
        nonlocal token, mobile_headers
        if not token or not str(token).strip():
            _set_token_problem("no_token")
            return []
        t0 = time.perf_counter()
        status_code, results = get_offers_p1(token, headers=mobile_headers)
        observe_ms("p1_fetch_ms", (time.perf_counter() - t0) * 1000.0)
        if status_code in (401, 403):
            _fail_key = (str(bot_id), int(telegram_id))
            _p1_fail_counts[_fail_key] = _p1_fail_counts.get(_fail_key, 0) + 1
            _fail_n = _p1_fail_counts[_fail_key]
            _poll_log(
                f"⚠️ P1 offers {status_code} for user {telegram_id} "
                f"(consecutive fail {_fail_n}/{_P1_FAIL_THRESHOLD})"
            )
            if _fail_n < _P1_FAIL_THRESHOLD:
                # Transient failure — skip this cycle without marking invalid
                return []
            # Threshold reached — check auto-refresh mode
            _poll_log(f"⚠️ P1 threshold reached for {telegram_id} — checking auto-refresh mode...")
            auto_refresh = get_token_auto_refresh(str(bot_id), int(telegram_id))
            _ar_key = (str(bot_id), int(telegram_id))
            if auto_refresh and email and password:
                # Auto-refresh ON: attempt Playwright re-login
                ok, new_token, new_refresh, note = get_playwright_p1_token(bot_id, telegram_id, email, password)
                if ok and new_token:
                    status_code2, results2 = get_offers_p1(new_token, headers=mobile_headers)
                    if status_code2 == 200:
                        # Token works — save and reset counters
                        save_playwright_p1_token(bot_id, telegram_id, new_token, new_refresh, mobile_headers)
                        token = new_token
                        _p1_fail_counts.pop(_fail_key, None)
                        _auto_refresh_fail_counts.pop(_ar_key, None)
                        set_token_ok_mem(bot_id, telegram_id, cache_version)
                        unpin_warning_if_any(bot_id, telegram_id, "expired")
                        unpin_warning_if_any(bot_id, telegram_id, "no_token")
                        bot_tok = _resolve_bot_token(bot_id, telegram_id)
                        tg_send_message(bot_tok, telegram_id, "✅ <b>Token refreshed successfully</b> — bot is back online.")
                        _log_offers_found("P1", telegram_id, results2 or [])
                        return results2 or []
                    _poll_log(f"⚠️ P1 auto-refresh new token still {status_code2} for {telegram_id}")
                else:
                    _poll_log(f"⚠️ P1 auto-refresh Playwright failed for {telegram_id}: {note}")
                # Auto-refresh attempt failed
                _ar_n = _auto_refresh_fail_counts.get(_ar_key, 0) + 1
                _auto_refresh_fail_counts[_ar_key] = _ar_n
                _poll_log(f"⚠️ P1 auto-refresh fail {_ar_n}/{_AUTO_REFRESH_FAIL_THRESHOLD} for {telegram_id}")
                if _ar_n >= _AUTO_REFRESH_FAIL_THRESHOLD:
                    # Disable auto-refresh and notify user with pinned warning
                    set_token_auto_refresh(str(bot_id), int(telegram_id), False)
                    _auto_refresh_fail_counts.pop(_ar_key, None)
                    _poll_log(f"⚠️ Auto-refresh disabled for {telegram_id} after {_AUTO_REFRESH_FAIL_THRESHOLD} failures")
                    _set_token_problem("expired")
                else:
                    # Let polling continue so another attempt can be made
                    _p1_fail_counts.pop(_fail_key, None)
                return []
            else:
                # Auto-refresh OFF or no credentials: notify user to update manually
                if auto_refresh and not (email and password):
                    _poll_log(f"⚠️ Auto-refresh ON for {telegram_id} but no BL credentials — falling back to manual")
                _set_token_problem("expired")
                return []
        if status_code == 200:
            _p1_fail_counts.pop((str(bot_id), int(telegram_id)), None)
            if not is_token_ok_mem(bot_id, telegram_id, cache_version):
                set_token_status(bot_id, telegram_id, "valid")
                unpin_warning_if_any(bot_id, telegram_id, "expired")
                unpin_warning_if_any(bot_id, telegram_id, "no_token")
                set_token_ok_mem(bot_id, telegram_id, cache_version)
            offers = results or []
            _log_offers_found("P1", telegram_id, offers)
            return offers
        if status_code is None:
            err_detail = results.get("error") if isinstance(results, dict) else None
            if err_detail:
                _poll_log(f"⚠️ P1 offers error for user {telegram_id} | {err_detail}")
            else:
                _poll_log(f"⚠️ P1 offers returned None for user {telegram_id} | body={results}")
        else:
            _poll_log(f"⚠️ P1 offers returned {status_code} for user {telegram_id} | body={results}")
        return []

    # ---------- PLATFORM 2 OFFERS (Portal/Athena) ----------
    offers_p2: List[dict] = []
    if USE_MOCK_P2:
        portal_sample = {
            "data": [
                {
                    "id": "bfd8e29a-964d-4bce-9f21-9f336d30cb91",
                    "type": "offers",
                    "attributes": {
                        "starts_at": "2026-01-14T11:16:00-05:00",
                        "ends_at": "2026-01-14T10:11:20-05:00",
                        "created_at": "2026-01-14T16:02:16+01:00",
                        "price": "175.77",
                        "next_price_change_in": 42,
                        "currency": "USD",
                        "distance": 94363,
                        "service_class": "van",
                        "flight_number": "EK 243",
                        "special_requests": "Pieces of luggage: 1",
                        "booking_number": "764316632",
                        "legacy_id": 16183685,
                        "booked_buffer_time": None,
                        "business_district_slug": "montreal",
                        "pickup_sign": "",
                        "passenger_first_name": "",
                        "passenger_last_name": "",
                        "booking_type": "transfer",
                        "is_final_price": False,
                        "estimated_duration": 3836,
                    },
                    "relationships": {
                        "dropoff_location": {"data": {"id": "31404853", "type": "locations"}},
                        "pickup_location": {"data": {"id": "31404852", "type": "locations"}},
                    },
                },
                {
                    "id": "fa9d91b3-4189-4135-a36b-03ccd847bbe6",
                    "type": "offers",
                    "attributes": {
                        "starts_at": "2026-01-14T11:16:35-05:00",
                        "ends_at": "2026-01-14T10:27:01-05:00",
                        "created_at": "2026-01-14T15:22:00+01:00",
                        "price": "160.58",
                        "next_price_change_in": 143,
                        "currency": "USD",
                        "distance": 94363,
                        "service_class": "business",
                        "flight_number": None,
                        "special_requests": "Pieces of luggage: 1",
                        "booking_number": "227809739",
                        "legacy_id": 16183317,
                        "booked_buffer_time": None,
                        "business_district_slug": "montreal",
                        "pickup_sign": "",
                        "passenger_first_name": "",
                        "passenger_last_name": "",
                        "booking_type": "transfer",
                        "is_final_price": False,
                        "estimated_duration": 3836,
                    },
                    "relationships": {
                        "dropoff_location": {"data": {"id": "31404357", "type": "locations"}},
                        "pickup_location": {"data": {"id": "31404356", "type": "locations"}},
                    },
                }
            ],
            "included": [
                {
                    "id": "31404852",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "airport Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "formatted_address_de": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "latitude": "44.785985",
                        "longitude": "-73.382924",
                        "place_id": "P:Q2hJSjg4YkNpSEE4eWt3UnlHOVJ0X09hSkJn",
                        "city": "Plattsburgh",
                        "country_code": "US",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404853",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "formatted_address_de": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "latitude": "45.500164",
                        "longitude": "-73.565964",
                        "place_id": "P:Q2hJSnlkd3NFbkFieVV3Umo5ZnFRSGZaRmgw",
                        "city": "Montréal",
                        "country_code": "CA",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404356",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "formatted_address_de": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "latitude": "44.785985",
                        "longitude": "-73.382924",
                        "place_id": "P:Q2hJSjg4YkNpSEE4eWt3UnlHOVJ0X09hSkJn",
                        "city": "Plattsburgh",
                        "country_code": "US",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404357",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "formatted_address_de": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "latitude": "45.500164",
                        "longitude": "-73.565964",
                        "place_id": "P:Q2hJSnlkd3NFbkFieVV3Umo5ZnFRSGZaRmgw",
                        "city": "Montréal",
                        "country_code": "CA",
                        "airport_iata": None,
                    },
                },
            ],
        }
        included = portal_sample.get("included") or []
        for raw in (portal_sample.get("data") or []):
            mapped = _map_portal_offer(raw, included)
            if mapped:
                offers_p2.append(mapped)

    def _fetch_p2_offers_real():
        nonlocal portal_token
        if not ENABLE_P2:
            return [], portal_token
        # Check in-memory cache first — avoids a DB read every 200ms cycle.
        tok = portal_token or get_portal_token_mem(bot_id, telegram_id)
        if not tok and has_portal_creds:
            tok = _ensure_portal_token(bot_id, telegram_id, email, password)
            if tok:
                set_portal_token_mem(bot_id, telegram_id, tok)
        portal_token = tok
        if not tok:
            return [], tok
        etag = get_offers_etag(bot_id, telegram_id) if ATHENA_USE_OFFERS_ETAG else None
        t0 = time.perf_counter()
        status_code, payload, new_etag = _athena_get_offers(tok, etag=etag)
        observe_ms("p2_fetch_ms", (time.perf_counter() - t0) * 1000.0)

        if status_code in (401, 403):
            _poll_log(f"⚠️ Athena token unauthorized for user {telegram_id}. Re-logging...")
            clear_portal_token_mem(bot_id, telegram_id)
            tok = _ensure_portal_token(bot_id, telegram_id, email, password)
            if tok:
                set_portal_token_mem(bot_id, telegram_id, tok)
                t1 = time.perf_counter()
                status_code, payload, new_etag = _athena_get_offers(tok)
                observe_ms("p2_fetch_ms", (time.perf_counter() - t1) * 1000.0)

        offers: List[dict] = []
        if status_code == 200 and isinstance(payload, dict):
            if ATHENA_USE_OFFERS_ETAG and new_etag:
                set_offers_etag(bot_id, telegram_id, new_etag)
            included = payload.get("included") or []
            for raw in (payload.get("data") or []):
                mapped = _map_portal_offer(raw, included)
                if mapped:
                    offers.append(mapped)
            _log_offers_found("P2", telegram_id, offers)
        return offers, tok

    if not USE_MOCK_P1 or not USE_MOCK_P2:
        if ENABLE_P1 and ENABLE_P2 and not USE_MOCK_P1 and not USE_MOCK_P2:
            # Parallel P1+P2: both requests fired simultaneously.
            # Eliminates the sequential RTT wasted waiting for P1 before starting P2.
            _f1 = _fetch_executor.submit(_fetch_p1_offers_real)
            _f2 = _fetch_executor.submit(_fetch_p2_offers_real)
            try:
                offers_p1 = _f1.result()
            except Exception:
                offers_p1 = []
            try:
                _p2_res = _f2.result()
                if isinstance(_p2_res, tuple):
                    offers_p2, portal_token = _p2_res
            except Exception:
                offers_p2 = []
        else:
            if ENABLE_P1 and not USE_MOCK_P1:
                offers_p1 = _fetch_p1_offers_real()
            if ENABLE_P2 and not USE_MOCK_P2:
                offers_p2, portal_token = _fetch_p2_offers_real()

    # ---------- Combine and process ----------
    # Deduplicate by offer id: P1 wins if the same id appears on both platforms.
    now_ts = time.time()
    _seen_ids: set = set()
    all_offers: List[dict] = []
    for offer in (offers_p1 or []) + (offers_p2 or []):
        if not isinstance(offer, dict):
            continue
        oid = offer.get("id")
        if oid and oid in _seen_ids:
            continue
        if oid:
            _seen_ids.add(oid)
        if offer.get("_poll_ts") is None:
            offer["_poll_ts"] = now_ts
        all_offers.append(offer)

    if poll_real_orders:
        maybe_cleanup_rides(bot_id, telegram_id)
        cached_intervals = get_rides_intervals(bot_id, telegram_id)
        if cached_intervals is None:
            # First time: lazy-init rides cache in background (one-shot fetch)
            p2_init_token = portal_token if portal_token else None
            p1_init_token = token if (token and str(token).strip()) else None
            if p2_init_token or p1_init_token:
                _init_rides_cache_async(
                    bot_id,
                    telegram_id,
                    tz_name,
                    p1_init_token,
                    mobile_headers,
                    p2_init_token,
                    bl_uuid=bl_uuid,
                    portal_email=email,
                    portal_password=password,
                )
        accepted_intervals = cached_intervals or []

    if not all_offers:
        return f"Done with user {telegram_id}"

    debug_print_offers(telegram_id, all_offers)

    _process_offers_for_user(
        bot_id,
        telegram_id,
        all_offers,
        filters,
        class_state,
        booked_slots,
        blocked_days,
        accepted_intervals,
        tz_name,
        p1_token=token,
        p1_headers=mobile_headers,
        p2_token=portal_token,
        cache_version=cache_version,
        bl_uuid=bl_uuid,
        user_cfilters=user_cfilters,
        portal_email=email,
        portal_password=password,
    )

    return f"Done with user {telegram_id}"


def _user_key(user_row) -> Tuple[str, int]:
    return (str(user_row[0]), int(user_row[1]))


def _warmup_reserve_connections_async():
    """Fire-and-forget: warm up P1+P2 reserve connections using first available user token."""
    def _job():
        try:
            users = _get_users_cached()
            for u in users:
                tok = u[2] if len(u) > 2 else None
                hdrs = None
                try:
                    from db import get_mobile_headers as _gmh
                    hdrs = _gmh(u[0], u[1])
                except Exception:
                    pass
                if tok and str(tok).strip():
                    warmup_p1_reserve_connection(tok, hdrs)
                    return
        except Exception:
            pass
    _fetch_executor.submit(_job)


def run():
    init_db()
    _poll_log("🚀 Poller started")
    inflight: Dict[Tuple[str, int], tuple] = {}
    cycle_idx = 0

    _CLEANUP_CYCLE_EVERY = 200    # cleanup not_valid cache every ~20s at 100ms poll
    _WARMUP_CYCLE_EVERY = 300    # re-warm reserve connections every ~30s at 100ms poll

    # Pre-warm reserve connections immediately at startup
    _warmup_reserve_connections_async()

    while True:
        cycle_idx += 1
        if cycle_idx % _CLEANUP_CYCLE_EVERY == 0:
            cleanup_not_valid_cache()
        if cycle_idx % _WARMUP_CYCLE_EVERY == 0:
            _warmup_reserve_connections_async()
        if OFFER_MEMORY_DEDUPE:
            maybe_reset_inmem_caches()
        users = _get_users_cached()
        now_ts = time.time()

        completed = 0
        for key, entry in list(inflight.items()):
            fut, _started_ts = entry
            if not fut.done():
                continue
            completed += 1
            inflight.pop(key, None)
            try:
                res = fut.result()
                if res and ATHENA_PRINT_DEBUG:
                    _poll_log(f"✅ {res}")
            except Exception as e:
                _poll_log(f"❌ Poll error ({key[0]}/{key[1]}): {e}")
                _quiet_exc()

        launched = 0
        for user in users:
            key = _user_key(user)
            if key in inflight:
                continue
            inflight[key] = (_user_executor.submit(poll_user, user), now_ts)
            launched += 1

        time.sleep(_sleep_interval())
