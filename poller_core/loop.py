import json
import time
import traceback
from typing import Optional, List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import (
    ALWAYS_POLL_REAL_ORDERS,
    USE_MOCK_P1,
    USE_MOCK_P2,
    ATHENA_PRINT_DEBUG,
    POLL_INTERVAL,
    MAX_WORKERS,
)
from .state import (
    maybe_reset_inmem_caches,
)
from .utils import _normalize_formulas
from .p1_client import get_rides_p1, get_offers_p1
from .p2_client import (
    _map_portal_offer,
    _athena_get_offers,
    _athena_get_rides,
    _ensure_portal_token,
    _filter_rides_by_bl_uuid,
)
from .rides import (
    _dump_rides,
    _extract_intervals_from_rides,
    _rides_snapshot_from_athena_payload,
    _rides_snapshot_from_p1_list,
)
from .processing import debug_print_offers, _process_offers_for_user
from .notify import (
    pin_warning_if_needed,
    unpin_warning_if_any,
    _resolve_bot_token,
    tg_unpin_message,
)
from db import (
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
)

def _quiet_print(*args, **kwargs):
    return None


def _quiet_exc(*args, **kwargs):
    return None


print = _quiet_print


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
    bot_id, telegram_id, token, filters_json, active, bot_admin_active = user

    tz_name = get_user_timezone(bot_id, telegram_id) or "UTC"
    print(
        f"[{datetime.now()}] 🔍 Polling {bot_id}/{telegram_id} "
        f"(active={active}, admin_active={bot_admin_active}) tz={tz_name}"
    )

    if not bot_admin_active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive bot {bot_id} (admin inactive)")
        return
    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    # Load filters + normalize admin formulas once
    filters = json.loads(filters_json) if filters_json else {}
    formulas_raw = get_endtime_formulas(bot_id, telegram_id)
    filters["__endtime_formulas__"] = _normalize_formulas(formulas_raw)

    class_state = get_vehicle_classes_state(bot_id, telegram_id)
    booked_slots = get_booked_slots(bot_id, telegram_id)
    blocked_days = {d["day"] for d in get_blocked_days(bot_id, telegram_id)}

    # ---------- Build busy intervals from Rides (Athena preferred) ----------
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]] = []

    bl_uuid = get_bl_uuid(bot_id, telegram_id)
    email, password = _read_portal_creds(bot_id, telegram_id)
    portal_token = None
    if email and password:
        portal_token = _ensure_portal_token(bot_id, telegram_id, email, password)

    poll_real_orders = ALWAYS_POLL_REAL_ORDERS and not (USE_MOCK_P1 and USE_MOCK_P2)

    def _fetch_p2_rides():
        if not portal_token:
            return [], False
        status_code, payload, _ = _athena_get_rides(portal_token)

        if status_code == 200 and isinstance(payload, dict):
            data_all = (payload or {}).get("data") or []
            data_kept = _filter_rides_by_bl_uuid(data_all, bl_uuid) if bl_uuid else data_all
            filtered_payload = {"data": data_kept, "included": (payload or {}).get("included") or []}
            snap = _rides_snapshot_from_athena_payload(filtered_payload, tz_name)
            _dump_rides(bot_id, telegram_id, snap, "p2")
            intervals_p2 = _extract_intervals_from_rides([
                (r.get("attributes") or {}) | {"starts_at": (r.get("attributes") or {}).get("starts_at")}
                for r in (data_kept or [])
            ])
            print(
                f"[{datetime.now()}] 📚 Loaded {len(intervals_p2)} assigned interval(s) "
                f"(kept {len(data_kept)}/{len(data_all)} rides) from Athena for user {telegram_id}"
            )
            return intervals_p2, True
        if status_code == 304:
            if ATHENA_PRINT_DEBUG:
                print(f"[{datetime.now()}] 📦 Athena rides 304 Not Modified for user {telegram_id}")
            return [], True
        if status_code in (401, 403):
            print(f"[{datetime.now()}] ⚠️ Athena rides unauthorized for user {telegram_id}.")
            return [], False
        if status_code is None:
            print(f"[{datetime.now()}] ⚠️ Athena rides network error for user {telegram_id}")
            return [], False
        print(f"[{datetime.now()}] ⚠️ Athena rides returned status {status_code} for user {telegram_id}")
        return [], False

    def _fetch_p1_rides():
        if not token or not str(token).strip():
            return [], False
        status_code, ride_results = get_rides_p1(token)
        if status_code == 200 and isinstance(ride_results, list):
            kept = _filter_rides_by_bl_uuid(ride_results, bl_uuid) if bl_uuid else ride_results
            snap = _rides_snapshot_from_p1_list(kept, tz_name)
            _dump_rides(bot_id, telegram_id, snap, "p1")
            intervals_p1 = _extract_intervals_from_rides(kept)
            print(
                f"[{datetime.now()}] 📚 Loaded {len(intervals_p1)} assigned interval(s) "
                f"(kept {len(kept)}/{len(ride_results)} rides) from P1 /rides for user {telegram_id}"
            )
            return intervals_p1, True
        if status_code in (401, 403):
            set_token_status(bot_id, telegram_id, "expired")
            existing = get_pinned_warnings(bot_id, telegram_id)
            if not existing["expired_msg_id"]:
                if existing["no_token_msg_id"]:
                    bot_token = _resolve_bot_token(bot_id, telegram_id)
                    tg_unpin_message(bot_token, telegram_id, existing["no_token_msg_id"])
                    clear_pinned_warning(bot_id, telegram_id, "no_token")
                pin_warning_if_needed(bot_id, telegram_id, "expired")
        elif status_code is None:
            print(f"[{datetime.now()}] ⚠️ P1 /rides network error for user {telegram_id}")
        else:
            print(f"[{datetime.now()}] ⚠️ P1 /rides returned status {status_code} for user {telegram_id}")
        return [], False

    if poll_real_orders:
        p1_intervals: List[Tuple[datetime, Optional[datetime]]] = []
        p2_intervals: List[Tuple[datetime, Optional[datetime]]] = []
        tasks = {}
        with ThreadPoolExecutor(max_workers=2) as ride_exec:
            if portal_token:
                tasks["p2"] = ride_exec.submit(_fetch_p2_rides)
            if token and str(token).strip():
                tasks["p1"] = ride_exec.submit(_fetch_p1_rides)
            for name, fut in tasks.items():
                intervals, _ok = fut.result()
                if name == "p1":
                    p1_intervals = intervals
                else:
                    p2_intervals = intervals
        accepted_intervals = p1_intervals + p2_intervals

        if not portal_token and not (token and str(token).strip()):
            existing = get_pinned_warnings(bot_id, telegram_id)
            if not existing["expired_msg_id"] and not existing["no_token_msg_id"]:
                pin_warning_if_needed(bot_id, telegram_id, "no_token")

    # ---------- PLATFORM 1 OFFERS ----------
    offers_p1: List[dict] = []
    if USE_MOCK_P1:
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
        if token and str(token).strip():
            status_code, results = get_offers_p1(token)
            if status_code in (401, 403):
                set_token_status(bot_id, telegram_id, "expired")
                existing = get_pinned_warnings(bot_id, telegram_id)
                if not existing["expired_msg_id"]:
                    if existing["no_token_msg_id"]:
                        bot_token = _resolve_bot_token(bot_id, telegram_id)
                        tg_unpin_message(bot_token, telegram_id, existing["no_token_msg_id"])
                        clear_pinned_warning(bot_id, telegram_id, "no_token")
                    pin_warning_if_needed(bot_id, telegram_id, "expired")
                print(f"[{datetime.now()}] ⚠️ P1 offers returned {status_code} for user {telegram_id}")
                return []
            if status_code == 200:
                set_token_status(bot_id, telegram_id, "valid")
                unpin_warning_if_any(bot_id, telegram_id, "expired")
                unpin_warning_if_any(bot_id, telegram_id, "no_token")
                offers = results or []
                print(f"[{datetime.now()}] 📥 P1 offers for user {telegram_id}: {len(offers)}")
                return offers
            print(f"[{datetime.now()}] ⚠️ P1 offers returned {status_code} for user {telegram_id} | body={results}")
            return []

        existing = get_pinned_warnings(bot_id, telegram_id)
        if not existing["expired_msg_id"] and not existing["no_token_msg_id"]:
            pin_warning_if_needed(bot_id, telegram_id, "no_token")
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
        tok = portal_token
        if not tok:
            return [], tok
        status_code, payload, _ = _athena_get_offers(tok)

        if ATHENA_PRINT_DEBUG:
            print(f"[{datetime.now()}] 🛰️ Athena offers status={status_code} for user {telegram_id}")

        if status_code in (401, 403):
            print(f"[{datetime.now()}] ⚠️ Athena token unauthorized for user {telegram_id}. Re-logging...")
            tok = _ensure_portal_token(bot_id, telegram_id, email, password)
            if tok:
                status_code, payload, _ = _athena_get_offers(tok)
                if ATHENA_PRINT_DEBUG:
                    print(
                        f"[{datetime.now()}] 🛰️ Athena offers (after re-login) status={status_code} for user {telegram_id}"
                    )

        offers: List[dict] = []
        if status_code == 200 and isinstance(payload, dict):
            included = payload.get("included") or []
            for raw in (payload.get("data") or []):
                mapped = _map_portal_offer(raw, included)
                if mapped:
                    offers.append(mapped)
            print(f"[{datetime.now()}] 📥 P2 offers for user {telegram_id}: {len(offers)}")
        elif status_code == 304 and ATHENA_PRINT_DEBUG:
            print(f"[{datetime.now()}] 📦 Athena offers 304 Not Modified for user {telegram_id}")
        return offers, tok

    if not USE_MOCK_P1 or not USE_MOCK_P2:
        if not USE_MOCK_P1 and not USE_MOCK_P2:
            tasks = {}
            with ThreadPoolExecutor(max_workers=2) as offer_exec:
                tasks["p1"] = offer_exec.submit(_fetch_p1_offers_real)
                tasks["p2"] = offer_exec.submit(_fetch_p2_offers_real)
                for name, fut in tasks.items():
                    if name == "p1":
                        offers_p1 = fut.result()
                    else:
                        offers_p2, portal_token = fut.result()
        else:
            if not USE_MOCK_P1:
                offers_p1 = _fetch_p1_offers_real()
            if not USE_MOCK_P2:
                offers_p2, portal_token = _fetch_p2_offers_real()

    # ---------- Combine and process ----------
    all_offers = (offers_p1 or []) + (offers_p2 or [])
    if not all_offers:
        print(f"[{datetime.now()}] ℹ️ No offers for user {telegram_id} this cycle")
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
        p2_token=portal_token,
    )

    return f"Done with user {telegram_id}"


def run():
    print(f"[{datetime.now()}] 🚀 Poller started")
    while True:
        maybe_reset_inmem_caches()
        print(f"[{datetime.now()}] 🔄 Starting polling cycle")
        users = get_all_users_with_bot_admin_active()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(poll_user, u) for u in users]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        print(f"[{datetime.now()}] ✅ {res}")
                except Exception as e:
                    print(f"[{datetime.now()}] ❌ Poll error: {e}")
                    _quiet_exc()
        time.sleep(POLL_INTERVAL)
