# poller.py
import time
import json
import uuid
import re
import sqlite3
import requests
import traceback
from typing import Optional, Iterable, List, Tuple, Dict, Any
from datetime import datetime, timezone, timedelta
from datetime import time as dt_time
from dateutil import parser
from dateutil.tz import gettz
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import threading
import base64
import os

from dotenv import load_dotenv

inmem_lock = threading.Lock()

from db import (
    DB_FILE,
    get_all_users,
    get_booked_slots,
    get_vehicle_classes_state,
    log_offer_decision,
    save_offer_message,
    get_user_timezone,
    get_pinned_warnings,
    save_pinned_warning,
    clear_pinned_warning,
    set_token_status,
    get_blocked_days,
    list_user_custom_filters,
    get_notifications,
    # Portal additions
    get_portal_token,
    update_portal_token,
    get_bl_account_full,
    get_endtime_formulas,
    get_bl_uuid,
)

# =============================================================
#  Poller – production-ready
#  - Prefer Athena rides if portal creds exist; fallback to P1 /rides
#  - Poll offers from both platforms (mock or real)
#  - Normalize to a single shape; compute endsAt; apply filters
#  - Send Telegram messages (now requires explicit 'platform' argument)
#  - Print/dump polled rides (Athena and fallback P1)
# =============================================================

load_dotenv()  # reads .env in project root

# -------- Config --------
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_HOST = "https://chauffeur-app-api.blacklane.com"  # Platform 1 (mobile)
ATHENA_BASE = "https://athena.blacklane.com"          # Platform 2 (Portal)
PORTAL_CLIENT_ID = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
PORTAL_PAGE_SIZE = 50

POLL_INTERVAL = 5
MAX_WORKERS = 10

# Toggle mock data for development (default: real polling)
USE_MOCK_P1 = False      # set True to use mock offers for Platform 1
USE_MOCK_P2 = False      # set True to use mock offers for Platform 2
ALWAYS_POLL_REAL_ORDERS = True  # always poll real /rides (both platforms when available)
# When enabled, accepted offers will be actually reserved via API calls (P1/P2).
AUTO_RESERVE_ENABLED = False

# Diagnostics
DEBUG_PRINT_OFFERS = False   # print raw offers
CF_DEBUG = False             # custom filters debug
ATHENA_PRINT_DEBUG = True   # print portal token and raw payloads
DEBUG_ENDS = False           # log endsAt math for each offer
APPLY_GAP_TO_BUSY_INTERVALS = False  # ← gap will NOT extend busy intervals

# --- Rides visibility ---
DUMP_RIDES_IN_LOGS = True         # print polled rides to stdout
DUMP_RIDES_IN_TELEGRAM = False    # also send a compact snapshot to the user
MAX_RIDES_SHOWN = 20              # cap to avoid spam

# Athena token/etag helpers
ATHENA_RELOGIN_SKEW_S = 3600 * 24 * 1000  # seconds before exp to proactively re-login
_athena_offers_etag: Dict[int, Optional[str]] = {}  # telegram_id -> etag (offers)
_athena_rides_etag: Dict[int, Optional[str]] = {}   # telegram_id -> etag (rides)

# In-memory dedupe for accepted/rejected per user (reset every 24h)
accepted_per_user = defaultdict(set)
rejected_per_user = defaultdict(set)
_CACHE_RESET_INTERVAL = timedelta(hours=24)
_cache_last_reset = datetime.now(timezone.utc)

# =============================================================
#  Utilities – safe parsing & formatters
# =============================================================

def maybe_reset_inmem_caches():
    """Clear accepted/rejected caches every 24h."""
    global _cache_last_reset
    now = datetime.now(timezone.utc)
    if now - _cache_last_reset >= _CACHE_RESET_INTERVAL:
        accepted_per_user.clear()
        rejected_per_user.clear()
        _cache_last_reset = now
        print(f"[{datetime.now()}] 🔁 Cleared in-memory accept/reject caches (24h rotation)")

def _to_str(x):
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x)

def _to_int(x, default=0):
    try:
        if isinstance(x, (int, float)):
            return int(x)
        s = _to_str(x)
        if s is None:
            return default
        m = re.search(r"-?\d+", s)
        return int(m.group(0)) if m else default
    except Exception:
        return default

def _parse_hhmm(s):
    """Parse 'HH:MM' → (h,m) or None. Robust to bytes and junk."""
    try:
        s = _to_str(s)
        parts = (s or "").split(":")
        if len(parts) < 2:
            return None
        hh = _to_int(parts[0], None)
        mm = _to_int(parts[1], None)
        if hh is None or mm is None:
            return None
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        return hh, mm
    except Exception:
        return None

def _time_in_interval(t, start_s, end_s):
    if start_s is None or end_s is None:
        return False
    shsm = _parse_hhmm(start_s); ehm = _parse_hhmm(end_s)
    if not shsm or not ehm:
        return False
    sh, sm = shsm; eh, em = ehm
    cur = (t.hour, t.minute)
    start = (sh, sm); end = (eh, em)
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end  # wraps midnight

def _prio(row):
    return _to_int((row or {}).get("priority", 0), 0)

def _esc(s: Optional[str]) -> str:
    if s is None:
        return "—"
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _fmt_money(price, currency) -> str:
    if price is None:
        return "—"
    try:
        return f"{float(price):.2f} {currency or ''}".strip()
    except Exception:
        return f"{price} {currency or ''}".strip()

def _fmt_km(meters) -> str:
    if meters is None:
        return "—"
    try:
        return f"{float(meters)/1000.0:.3f} km"
    except Exception:
        return str(meters)

def _fmt_minutes(mins) -> str:
    if mins is None:
        return "—"
    try:
        return f"{float(mins):.0f} min"
    except Exception:
        return str(mins)

def _split_chunks(text: str, limit: int = 4096) -> Iterable[str]:
    t = text
    while len(t) > limit:
        cut = t.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        yield t[:cut]
        t = t[cut:]
    if t:
        yield t

def _strip_html_tags(text: str) -> str:
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.I)
    return re.sub(r"</?[^>]+>", "", text)

def _fmt_dt_local(s: str, tz_name: Optional[str]) -> str:
    if not s:
        return "—"
    try:
        dt = parser.isoparse(s)
        tzinfo = gettz(tz_name) if tz_name else None
        if tzinfo:
            return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
        return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            tzinfo = gettz(tz_name) if tz_name else None
            if tzinfo:
                return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
            return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            return s

def _fmt_dt_local_from_dt(dt: datetime, tz_name: Optional[str]) -> str:
    tzinfo = gettz(tz_name) if tz_name else None
    if tzinfo:
        return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
    return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")

# =============================================================
#  Duration helpers & formula normalization
# =============================================================

def _duration_minutes_from_rid(rid: dict) -> Optional[float]:
    """
    Return ride duration in minutes from whatever the API provides.
    Supports minutes or seconds (camelCase/snake_case).
    """
    if not isinstance(rid, dict):
        return None

    # minutes candidates
    for k in (
        "durationMinutes",
        "estimatedDurationMinutes",
        "duration_minutes",
        "estimated_duration_minutes",
    ):
        v = rid.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    # seconds candidates -> convert to minutes
    for k in (
        "estimatedDurationSeconds",
        "durationSeconds",
        "estimated_duration_seconds",
        "duration_seconds",
        "estimated_duration",  # sometimes this is seconds
        "estimatedDuration",
    ):
        v = rid.get(k)
        if v is not None:
            try:
                v = float(v)
                return v / 60.0 if v > 1000 else v
            except Exception:
                pass

    return None

def _normalize_formulas(rows):
    """Sanitize admin formula rows from DB (bytes/strings -> clean types)."""
    out = []
    for r0 in (rows or []):
        r = dict(r0 or {})
        r["start"] = _to_str(r.get("start"))
        r["end"] = _to_str(r.get("end"))
        r["priority"] = _to_int(r.get("priority"), 0)
        try:
            r["speed_kmh"] = float(_to_str(r.get("speed_kmh") or 0) or 0)
        except Exception:
            r["speed_kmh"] = 0.0
        try:
            r["bonus_min"] = float(_to_str(r.get("bonus_min") or 0) or 0)
        except Exception:
            r["bonus_min"] = 0.0
        out.append(r)
    return out

def _pick_formula_for_pickup(filters: dict, pickup_dt: datetime, tz_name: str):
    formulas = filters.get("__endtime_formulas__") or []
    if not formulas:
        return None
    local_t = pickup_dt.astimezone(gettz(tz_name)).time()
    fallback = None

    for row in sorted(formulas, key=_prio):
        if not isinstance(row, dict):
            continue
        st = _to_str(row.get("start"))
        en = _to_str(row.get("end"))
        if st and en:
            if _time_in_interval(local_t, st, en):
                return row
        elif not st and not en:
            fallback = row
    return fallback

def _compute_ends_at(offer: dict, filters: dict, pickup_dt: datetime, tz_name: str):
    """
    Compute endsAt for an offer using either:
      - Hourly: pickup + durationMinutes
      - Transfer: admin formula (speed_kmh, bonus_min) if available, else provided duration
    Returns (ends_at_iso_or_None, details_dict_or_None)
    """
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()

    if otype == "hourly":
        dur_min = _duration_minutes_from_rid(rid)
        if dur_min:
            ends_at = pickup_dt + timedelta(minutes=float(dur_min))
            if DEBUG_ENDS:
                _log(f"ENDSAT[hourly]: pickup={pickup_dt.isoformat()} + {dur_min:.1f}min = {ends_at.isoformat()}")
            return ends_at.isoformat(), {
                "duration_minutes": float(dur_min),
                "formula": "pickup + durationMinutes",
            }
        return None, None

    if otype == "transfer":
        dist_m = rid.get("estimatedDistanceMeters")
        try:
            if dist_m is not None:
                dist_m = float(dist_m)
        except Exception:
            dist_m = None

        # try admin formulas first
        rule = _pick_formula_for_pickup(filters, pickup_dt, tz_name)
        if rule and dist_m is not None:
            try:
                speed = float(rule.get("speed_kmh") or 0.0)
                bonus = float(rule.get("bonus_min") or 0.0)
                dist_km = float(dist_m) / 1000.0
                one_way_min = (dist_km / speed) * 60.0 if speed > 0 else 0.0
                total_min = one_way_min * 2.0 + bonus
                ends_at = pickup_dt + timedelta(minutes=total_min)
                if DEBUG_ENDS:
                    _log(
                        "ENDSAT[transfer:formula]: dist_km={:.3f} speed_kmh={} "
                        "one_way={:.2f}min total={:.2f}min => {}".format(
                            dist_km, speed, one_way_min, total_min, ends_at.isoformat()
                        )
                    )
                return ends_at.isoformat(), {
                    "distance_km": round(dist_km, 3),
                    "speed_kmh": float(speed),
                    "one_way_minutes": round(one_way_min, 2),
                    "bonus_minutes": bonus,
                    "total_minutes": round(total_min, 2),
                    "formula": "((distance_km / speed_kmh) * 60) * 2 + bonus_minutes",
                }
            except Exception:
                pass

        # fallback: use provided duration (minutes or seconds)
        dur = _duration_minutes_from_rid(rid)
        if dur is not None:
            try:
                ends_at = pickup_dt + timedelta(minutes=float(dur))
                if DEBUG_ENDS:
                    _log(f"ENDSAT[transfer:fallback]: pickup={pickup_dt.isoformat()} + {float(dur):.1f}min = {ends_at.isoformat()}")
                return ends_at.isoformat(), {
                    "duration_minutes": float(dur),
                    "formula": "pickupTime + (durationMinutes|estimatedDuration)",
                }
            except Exception:
                pass

        return None, None

    return None, None

# =============================================================
#  Platform helpers (Rides & Offers)
# =============================================================

# ---------- P1: /rides ----------
def get_rides_p1(token: str) -> Tuple[Optional[int], Optional[list]]:
    headers = {
        "Host": API_HOST.replace("https://", ""),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
        "User-Agent": "Chauffeur/14647 CFNetwork/1494.0.7 Darwin/23.4.0",
    }
    try:
        r = requests.get(f"{API_HOST}/rides", headers=headers, timeout=12)
        if 200 <= r.status_code < 300:
            try:
                data = r.json()
            except Exception:
                return 200, []
            if isinstance(data, list):
                return 200, data
            if isinstance(data, dict):
                for key in ("results", "rides", "data", "items"):
                    val = data.get(key)
                    if isinstance(val, list):
                        return 200, val
                return 200, [data] if data else []
            return 200, []
        return r.status_code, None
    except Exception as e:
        print(f"[{datetime.now()}] ❌ P1 /rides exception: {e}")
        return None, None

# ---------- P1: /offers ----------
def get_offers_p1(token: str):
    headers = {
        "Host": API_HOST.replace("https://", ""),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
    }
    try:
        r = requests.get(f"{API_HOST}/offers", headers=headers, timeout=12)
        try:
            body = r.json()
        except Exception:
            body = r.text

        if r.status_code == 200 and isinstance(body, dict):
            results = body.get("results", []) or []
            for it in results:
                try:
                    it["_platform"] = "p1"
                except Exception:
                    pass
            return 200, results

        # return status + body for diagnostics (401/403/etc)
        return r.status_code, body
    except Exception as e:
        print(f"[{datetime.now()}] ❌ P1 /offers exception: {e}")
        return None, None

# ---------- P1: reserve ----------
def reserve_offer_p1(token: str, offer_id: str):
    """
    Accept (reserve) an offer on Platform 1.

    Returns: (status_code, json_or_text)
      200/201 → accepted
      401/403 → token invalid/expired
      409      → conflict / already taken
      422      → cannot accept (validation)
    """
    headers = {
        "Host": API_HOST.replace("https://", ""),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,                 # e.g. "Bearer <JWT>"
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
    }
    payload = {
        "id": offer_id,
        "action": "accept",
        "parameters": []                        # present for symmetry with actions list
    }
    r = requests.post(f"{API_HOST}/offers", headers=headers, json=payload, timeout=12)
    try:
        body = r.json()
    except Exception:
        body = r.text
    return r.status_code, body

# ---------- P2: helpers ----------
def _safe_attr(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur

def _find_included(included: list, typ: str, id_: str):
    for it in (included or []):
        if it.get("type") == typ and str(it.get("id")) == str(id_):
            return it
    return None

def _extract_loc_from_included(included_item: dict) -> dict:
    if not isinstance(included_item, dict):
        return {}
    attrs = included_item.get("attributes") or {}
    address = (
        attrs.get("formatted_address_en")
        or attrs.get("formatted_address_de")
        or attrs.get("formatted_address")
        or ", ".join([x for x in [attrs.get("city"), attrs.get("country_code")] if x])
    )
    name = attrs.get("airport_iata") or attrs.get("city")
    out = {}
    if name:
        out["name"] = str(name)
    if address:
        out["address"] = str(address)
    return out

def _normalize_vclass(name: str) -> str:
    m = {
        "business": "Business",
        "first": "First",
        "suv": "SUV",
        "van": "VAN",
        "electric": "Electric",
        "sprinter": "Sprinter",
    }
    key = (name or "").strip().lower()
    return m.get(key, name or "")

# ---------- P2: /offers ----------
def _map_portal_offer(raw: dict, included: list) -> Optional[dict]:
    """Convert Athena JSON:API offer into the internal shape."""
    if not isinstance(raw, dict):
        return None

    attrs = raw.get("attributes") or {}
    rel   = raw.get("relationships") or {}
    oid   = str(raw.get("id") or "")
    if not oid:
        return None

    pickup_iso = attrs.get("starts_at") or attrs.get("pickup_time") or attrs.get("start_time")
    if not pickup_iso:
        return None

    price_raw = attrs.get("price")
    try:
        price = float(price_raw) if price_raw is not None else None
    except Exception:
        price = price_raw
    currency = attrs.get("currency") or "USD"

    vclass = _normalize_vclass(
        attrs.get("service_class") or attrs.get("vehicle_class") or attrs.get("class") or ""
    )
    rid_type = (attrs.get("booking_type") or "").strip().lower() or "transfer"

    est_dist = attrs.get("distance")
    try:
        if est_dist is not None:
            est_dist = float(est_dist)
    except Exception:
        pass

    est_dur_raw = attrs.get("estimated_duration")
    est_dur_min = None
    try:
        if est_dur_raw is not None:
            val = float(est_dur_raw)
            est_dur_min = val / 60.0 if val > 1000 else val
    except Exception:
        est_dur_min = None

    pu_rel = _safe_attr(rel, "pickup_location", "data")
    do_rel = _safe_attr(rel, "dropoff_location", "data")

    pickUpLocation = {}
    dropOffLocation = None

    if pu_rel and pu_rel.get("id") and pu_rel.get("type"):
        inc = _find_included(included, pu_rel["type"], pu_rel["id"])
        pickUpLocation = _extract_loc_from_included(inc) or {}
    if do_rel and do_rel.get("id") and do_rel.get("type"):
        inc = _find_included(included, do_rel["type"], do_rel["id"])
        dropOffLocation = _extract_loc_from_included(inc) or None

    flight_no = attrs.get("flight_number")
    special_reqs = attrs.get("special_requests")

    ride = {
        "type": rid_type,  # "transfer" | "hourly"
        "pickUpLocation": pickUpLocation,
        "pickupTime": pickup_iso,
    }
    if dropOffLocation:
        ride["dropOffLocation"] = dropOffLocation
    if est_dist is not None:
        ride["estimatedDistanceMeters"] = est_dist
    if est_dur_min is not None:
        ride["estimatedDurationMinutes"] = est_dur_min
        ride["durationMinutes"] = est_dur_min
    if flight_no:
        ride["flight"] = {"number": str(flight_no)}
    if special_reqs:
        ride["guestRequests"] = special_reqs

    mapped = {
        "type": "ride",
        "id": oid,                 # keep real ID (no prefix)
        "price": price,
        "currency": currency,
        "vehicleClass": vclass,
        "rides": [ride],
        "_platform": "p2",         # mark platform explicitly
    }
    return mapped

# ---------- P2: reserve ----------
def reserve_offer_p2(access_token: str, offer_id: str, price: float):
    """
    Place a bid for an offer on Platform 2 (Athena/Portal).

    Returns: (status_code, json_or_text)
    """
    url = f"{ATHENA_BASE}/hades/bids"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "BLPortal/1.0 (+poller)",
    }
    payload = {
        "data": {
            "type": "bids",
            "attributes": {"price": float(price)},
            "relationships": {
                "offer": {"data": {"id": str(offer_id), "type": "offers"}}
            },
        },
        "meta": {},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=15)
    try:
        body = r.json()
    except Exception:
        body = r.text
    return r.status_code, body

def _athena_login(email: str, password: str) -> Tuple[bool, Optional[str], str]:
    url = f"{ATHENA_BASE}/oauth/token"
    payload = {
        "client_id": PORTAL_CLIENT_ID,
        "username": email,
        "password": password,
        "grant_type": "implicit",
        "resource_owner_type": "driver",
    }
    headers = {"Accept": "application/json"}
    try:
        r = requests.post(url, data=payload, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            try:
                j = r.json() or {}
            except Exception:
                return (False, None, "upstream:bad_json")
            token = (j.get("result") or {}).get("access_token") or j.get("access_token")
            if token:
                return (True, token, "ok")
            return (False, None, "upstream:no_token")
        if r.status_code in (401, 403):
            return (False, None, f"unauthorized:{r.status_code}")
        return (False, None, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, None, f"network:{type(e).__name__}")

def _jwt_exp_unverified(token: str) -> Optional[int]:
    """Best-effort read of 'exp' (seconds since epoch) from a JWT without verifying; None if not readable."""
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
        # If we can't tell, assume valid and let 401 drive a re-login
        return False
    now = int(time.time())
    return now >= (exp - ATHENA_RELOGIN_SKEW_S)

def _ensure_portal_token(telegram_id: int, email: str, password: str) -> Optional[str]:
    """Get token from DB and refresh if missing/expired."""
    portal_token = get_portal_token(telegram_id)
    if isinstance(portal_token, (list, tuple)):
        portal_token = portal_token[0] if portal_token else None

    needs_login = _portal_token_expired(portal_token)
    if not portal_token or needs_login:
        ok, new_tok, note = _athena_login(email, password)
        if ok and new_tok:
            update_portal_token(telegram_id, new_tok)
            portal_token = new_tok
            if ATHENA_PRINT_DEBUG:
                print(f"[{datetime.now()}] 🔐 Athena login OK for user {telegram_id}.")
        else:
            print(f"[{datetime.now()}] ❌ Portal login failed for user {telegram_id}: {note}")
            portal_token = None
    return portal_token

# ---------- P2: /offers (ETag-aware) ----------
def _athena_get_offers(access_token: str, page: int = 1, page_size: int = PORTAL_PAGE_SIZE, etag: Optional[str] = None):
    """
    Returns (status_code, payload_or_None, response_etag_or_None).
    On 304, payload is None and etag may be returned by upstream.
    """
    url = (
        f"{ATHENA_BASE}/hades/offers"
        f"?page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
        f"&include=pickup_location%2Cdropoff_location"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.api+json",
        "Connection": "keep-alive",
        "User-Agent": "BLPortal/1.0 (+poller)",
    }
    if etag:
        headers["If-None-Match"] = etag
    try:
        r = requests.get(url, headers=headers, timeout=15)
        new_etag = r.headers.get("etag") or r.headers.get("ETag")
        if r.status_code == 304:
            return 304, None, new_etag
        if 200 <= r.status_code < 300:
            try:
                return r.status_code, r.json(), new_etag
            except Exception:
                return r.status_code, None, new_etag
        return r.status_code, None, new_etag
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ❌ Athena offers error: {e}")
        return None, None, None

# ---------- P2: /rides (YOUR URL) ----------
def _athena_get_rides(access_token: str, page: int = 1, page_size: int = PORTAL_PAGE_SIZE, etag: Optional[str] = None):
    """
    Poll Athena 'planned' rides. Returns (status_code, payload_or_None, response_etag_or_None).
    Endpoint provided by you:
    /hades/rides?page[number]=1&page[size]=30&include=pickup_location%2Cdropoff_location%2Caccepted_by%2Cassigned_driver%2Cassigned_vehicle%2Cavailable_drivers%2Cavailable_vehicles%2Cstatus_updates&filter[group]=planned
    """
    url = (
        f"{ATHENA_BASE}/hades/rides"
        f"?page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
        f"&include=pickup_location%2Cdropoff_location%2Caccepted_by%2Cassigned_driver%2Cassigned_vehicle%2Cavailable_drivers%2Cavailable_vehicles%2Cstatus_updates"
        f"&filter%5Bgroup%5D=planned"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.api+json",
        "Connection": "keep-alive",
        "User-Agent": "BLPortal/1.0 (+poller)",
    }
    if etag:
        headers["If-None-Match"] = etag
    try:
        r = requests.get(url, headers=headers, timeout=15)
        new_etag = r.headers.get("etag") or r.headers.get("ETag")
        if r.status_code == 304:
            return 304, None, new_etag
        if 200 <= r.status_code < 300:
            try:
               
                return r.status_code, r.json(), new_etag
            except Exception:
                return r.status_code, None, new_etag
        return r.status_code, None, new_etag
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ❌ Athena rides error: {e}")
        return None, None, None


def _athena_assigned_driver_id(raw_ride: dict) -> str | None:
    """
    Athena JSON:API ride → relationships.assigned_driver.data.id (or assignedDriver).
    """
    if not isinstance(raw_ride, dict):
        return None
    rel = raw_ride.get("relationships") or {}
    node = (rel.get("assigned_driver") or rel.get("assignedDriver") or {}).get("data")
    if isinstance(node, dict):
        did = node.get("id")
        return str(did) if did else None
    return None


def _filter_rides_by_bl_uuid(raw_items: list, bl_uuid: str) -> list:
    """
    Works for both Athena rides (JSON:API) and Mobile /rides payloads.
    Keeps only rides whose assigned driver id == bl_uuid.
    - Athena: relationships.assigned_driver.data.id
    - Mobile: item['chauffeur']['id']
    """
    if not bl_uuid:
        return raw_items or []

    filtered = []
    for it in (raw_items or []):
        # Athena JSON:API?
        if isinstance(it, dict) and "relationships" in it:
            did = _athena_assigned_driver_id(it)
            if did and str(did) == str(bl_uuid):
                filtered.append(it)
            continue

        # Mobile shape
        ch = (it or {}).get("chauffeur") or {}
        did = ch.get("id")
        if did and str(did) == str(bl_uuid):
            filtered.append(it)

    return filtered

# =============================================================
#  Filters, custom filters, conflicts
# =============================================================

def _get_enabled_filter_slugs(telegram_id: int):
    items = list_user_custom_filters(telegram_id)
    return {it["slug"]: it for it in items if it["global_enabled"] and it["user_enabled"]}

def _filter_pickup_airport_reject(offer: dict) -> Tuple[Optional[str], Optional[str]]:
    rid = (offer.get("rides") or [{}])[0]
    pu = (rid.get("pickUpLocation") or {}).get("address") or (rid.get("pickUpLocation") or {}).get("name") or ""
    text = (pu or "").lower()
    matched = next((k for k in ["airport", "aéroport"] if k in text), None)
    if matched:
        if CF_DEBUG:
            try:
                oid = offer.get("id")
                print(f"[{datetime.now()}] 🧪 CF fired: pickup_airport_reject (match='{matched}') for offer={oid} PU='{pu}'")
            except Exception:
                pass
        return "reject", "pickup contains 'airport'"
    return None, None

def _filter_reject_under_90_between_20_22(offer: dict, tz_name: str, min_price: float = 90.0,
                                          win_from="20:00", win_to="22:00") -> Tuple[Optional[str], Optional[str]]:
    rid = (offer.get("rides") or [{}])[0]
    if not rid.get("pickupTime"):
        return None, None

    fm = _parse_hhmm(_to_str(win_from))
    tm = _parse_hhmm(_to_str(win_to))
    if not fm or not tm:
        return None, None
    fH, fM = fm
    tH, tM = tm

    try:
        pu_dt = parser.isoparse(rid["pickupTime"])
        pu_local = pu_dt.astimezone(gettz(tz_name))
        within = (fH, fM) <= (pu_local.hour, pu_local.minute) <= (tH, tM)
    except Exception:
        within = False

    if not within:
        return None, None

    price = float(offer.get("price") or 0)
    if price < float(min_price):
        return "reject", f"price {price:.0f} < {min_price:.0f} between {_to_str(win_from)}-{_to_str(win_to)}"
    return None, None

def _run_custom_filters(offer: dict, enabled_map: dict, tz_name: str):
    if "pickup_airport_reject" in enabled_map:
        d, r = _filter_pickup_airport_reject(offer)
        if d:
            if CF_DEBUG:
                print(f"[{datetime.now()}] 🔔 Decision from CF 'pickup_airport_reject': {d} – {r}")
            return d, r
    if "reject_under_90_between_20_22" in enabled_map:
        try:
            params = json.loads(enabled_map["reject_under_90_between_20_22"].get("params") or "{}")
        except Exception:
            params = {}
        d, r = _filter_reject_under_90_between_20_22(
            offer, tz_name,
            float(params.get("min_price", 90)),
            params.get("from", "20:00"),
            params.get("to", "22:00"),
        )
        if d:
            return d, r
    return None, None

def _format_filter_summary(results: List[dict]) -> str:
    """
    Build a verbose summary of all filters with green/red markers.
    Each item in results is expected to have: name (str), ok (bool), detail (optional str).
    """
    if not results:
        return ""
    lines = ["<b>🧰 Filters:</b>"]
    for r in results:
        name = r.get("name") or "Filtre"
        detail = r.get("detail")
        icon = "✅" if r.get("ok") else "❌"
        if detail:
            lines.append(f"{icon} <b>{_esc(name)}:</b> {_esc(detail)}")
        else:
            lines.append(f"{icon} <b>{_esc(name)}:</b> {_esc('ok' if r.get('ok') else 'non respecté')}")
    return "\n".join(lines)

# ---------- Conflicts ----------
def _find_conflict(new_start: datetime, new_end_iso: Optional[str], accepted_intervals: List[Tuple[datetime, Optional[datetime]]]) -> Optional[Tuple[datetime, datetime]]:
    new_end = None
    if new_end_iso:
        try:
            new_end = parser.isoparse(new_end_iso)
        except Exception:
            new_end = None
    for a_start, a_end in accepted_intervals:
        if not a_end:
            continue
        if a_start <= new_start <= a_end:
            return (a_start, a_end)
        if new_end and not (new_end <= a_start or new_start >= a_end):
            return (a_start, a_end)
    return None

# =============================================================
#  Telegram helpers (now require platform arg)
# =============================================================

def _platform_icon(offer_or_platform) -> str:
    # accepts offer dict or plain "p1"/"p2" string
    plat = offer_or_platform
    if isinstance(offer_or_platform, dict):
        plat = offer_or_platform.get("_platform", "p1")
    return "💻" if str(plat).lower() == "p2" else "📱"

def _send_one(chat_id: int, text: str, reply_markup: Optional[dict], parse_mode: Optional[str]) -> Optional[int]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_notification": False}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=15)
    if r.status_code >= 400:
        try:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.json()}")
        except Exception:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.text}")
        r.raise_for_status()
    return r.json().get("result", {}).get("message_id")

def tg_send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, disable_notification: bool = False) -> Optional[int]:
    first_id = None
    try:
        chunks = list(_split_chunks(text, 4096))
        for i, ch in enumerate(chunks):
            mid = _send_one(chat_id, ch, reply_markup if i == 0 else None, "HTML")
            if first_id is None:
                first_id = mid
        return first_id
    except requests.HTTPError as e:
        print(f"[{datetime.now()}] ⚠️ Falling back to plain text due to HTML parse error: {e}")
        plain = _strip_html_tags(text)
        first_id = None
        for i, ch in enumerate(_split_chunks(plain, 4096)):
            try:
                mid = _send_one(chat_id, ch, reply_markup if i == 0 else None, None)
                if first_id is None:
                    first_id = mid
            except Exception as e2:
                print(f"[{datetime.now()}] ❌ Telegram fallback send failed: {e2}")
                return first_id
        return first_id
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram sendMessage error for {chat_id}: {e}")
        return None

def tg_pin_message(chat_id: int, message_id: int):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/pinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id, "disable_notification": False},
            timeout=10,
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram pinChatMessage error for {chat_id}: {e}")

def tg_unpin_message(chat_id: int, message_id: int):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/unpinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id},
            timeout=10,
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram unpinChatMessage error for {chat_id}: {e}")

def maybe_send_message(telegram_id: int, kind: str, text: str, platform: str, reply_markup: Optional[dict] = None):
    """
    kind: 'accepted' | 'not_accepted' | 'rejected'
    platform: 'p1' or 'p2' (required)
    Sends Telegram message only if user's notification preference for 'kind' is enabled.
    (You can later extend prefs to be per-platform if needed.)
    """
    prefs = get_notifications(telegram_id)
    if not prefs.get(kind, True):
        return None
    # simple platform-aware header injection (optional)
    icon = _platform_icon(platform)
    text = f"{icon} {text}"
    return tg_send_message(telegram_id, text, reply_markup=reply_markup)

def pin_warning_if_needed(telegram_id: int, kind: str):
    existing = get_pinned_warnings(telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        return
    other = "expired" if kind == "no_token" else "no_token"
    other_id = existing["expired_msg_id"] if kind == "no_token" else existing["no_token_msg_id"]
    if other_id:
        tg_unpin_message(telegram_id, other_id)
        clear_pinned_warning(telegram_id, other)
    if kind == "no_token":
        text = "⚠️ <b>Bot Issue</b>: no mobile session\n\nPlease add your mobile session token."
    else:
        text = "⚠️ <b>Bot Issue</b>: mobile session expired\n\nPlease update your mobile session token."
    markup = {"inline_keyboard": [[{"text": "➕ Add mobile session", "callback_data": "open_mobile_sessions"}]]}
    message_id = tg_send_message(telegram_id, text, reply_markup=markup)
    if message_id:
        tg_pin_message(telegram_id, message_id)
        save_pinned_warning(telegram_id, kind, message_id)

def unpin_warning_if_any(telegram_id: int, kind: str):
    existing = get_pinned_warnings(telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        tg_unpin_message(telegram_id, msg_id)
        clear_pinned_warning(telegram_id, kind)

# =============================================================
#  User message builder
# =============================================================

def _extract_addr(loc: dict) -> str:
    if not loc:
        return "—"
    return loc.get("address") or loc.get("name") or "—"

def _build_user_message(
    offer: dict,
    status: str,
    reason: Optional[str],
    tz_name: Optional[str],
    filters_summary: Optional[str] = None,
    filter_results: Optional[List[dict]] = None,
    platform: Optional[str] = None,
    forced_accept: bool = False,
) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    vclass = (offer.get("vehicleClass") or "")
    typ_disp = "transfer" if otype == "transfer" else ("hourly" if otype == "hourly" else "—")
    price_disp = _fmt_money(offer.get("price"), offer.get("currency"))
    pu_addr = _extract_addr(rid.get("pickUpLocation"))
    do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else None
    pickup_s = rid.get("pickupTime")
    ends_s   = rid.get("endsAt")
    pickup_disp = _fmt_dt_local(pickup_s, tz_name) if pickup_s else "—"
    ends_disp   = _fmt_dt_local(ends_s, tz_name) if ends_s else "—"

    flight_no = None
    if isinstance(rid.get("flight"), dict):
        flight_no = rid.get("flight", {}).get("number")
    guest_reqs = rid.get("guestRequests")
    if isinstance(guest_reqs, list):
        norm = []
        for it in guest_reqs:
            if isinstance(it, str):
                norm.append(it)
            elif isinstance(it, dict):
                for k in ("label", "name", "value", "text"):
                    if k in it and it[k]:
                        norm.append(str(it[k]))
                        break
        guest_reqs = ", ".join(norm) if norm else None
    elif isinstance(guest_reqs, str):
        pass
    else:
        guest_reqs = None
    dist = _fmt_km(rid.get("estimatedDistanceMeters"))
    dur  = _fmt_minutes(_duration_minutes_from_rid(rid))
    lines = [
        f"🚘 <b>Type:</b> {_esc(typ_disp)}",
        f"🚗 <b>Class:</b> {_esc(vclass)}",
        f"💰 <b>Price:</b> {_esc(price_disp)}",
    ]
    if flight_no:
        lines.append(f"✈️ <b>Flight number:</b> {_esc(flight_no)}")
    if guest_reqs:
        lines.append(f"👁️ <b>Special requests:</b> {_esc(guest_reqs)}")
    if dist != "—":
        lines.append(f"📏 <b>Distance:</b> {_esc(dist)}")
    if dur != "—":
        lines.append(f"⏱️ <b>Duration:</b> {_esc(dur)}")
    lines += [
        f"🕒 <b>Starts at:</b> {_esc(pickup_disp)}",
        f"⏳ <b>Ends at:</b> {_esc(ends_disp)}",
        "",
        f"⬆️ <b>Pickup:</b>\n{_esc(pu_addr)}",
    ]
    if do_addr:
        lines += ["", f"⬇️ <b>Dropoff:</b>\n{_esc(do_addr)}"]

    status_icon = "🟢" if status == "accepted" else "🔴"
    plat_icon = _platform_icon(platform or "p1")
    status_word = "valid" if status == "accepted" else "not valid"
    if forced_accept and status == "accepted":
        status_word = "valid (override)"
    header = f"🔥 New offer - {price_disp} - {status_icon} {status_word} {plat_icon}"
    body = "\n".join(lines)
    filters_block = _format_filter_summary(filter_results or []) if filter_results else (filters_summary or "")
    parts = [header, body]
    if filters_block:
        parts.append(filters_block)
    return "\n\n".join(parts)

def _build_offer_header_line(
    offer: dict,
    status: str,
    platform: Optional[str],
    forced_accept: bool = False,
) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    price_disp = _fmt_money(offer.get("price"), offer.get("currency"))
    status_icon = "🟢" if status == "accepted" else "🔴"
    plat_icon = _platform_icon(platform or "p1")
    status_word = "valid" if status == "accepted" else "not valid"
    if forced_accept and status == "accepted":
        status_word = "valid (override)"
    return f"🔥 New offer - {price_disp} - {status_icon} {status_word} {plat_icon}"
    return "\n".join(lines)

def _log(msg: str):
    print(f"[{datetime.now()}] {msg}")

def _fmt_local_iso(iso_or_none: Optional[str], tz_name: str) -> str:
    if not iso_or_none:
        return "—"
    try:
        dt = parser.isoparse(iso_or_none)
        return _fmt_dt_local_from_dt(dt, tz_name)
    except Exception:
        return iso_or_none

# =============================================================
#  Core offer processing pipeline per user
# =============================================================

def _parse_user_slot_local(dt_str: str, tz_name: str) -> Optional[datetime]:
    if not dt_str:
        return None
    dt_str = _to_str(dt_str).strip()
    tzinfo = gettz(tz_name)
    fmts = ["%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M"]
    for fmt in fmts:
        try:
            naive = datetime.strptime(dt_str, fmt)
            return naive.replace(tzinfo=tzinfo)
        except Exception:
            pass
    try:
        dt = parser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        else:
            dt = dt.astimezone(tzinfo)
        return dt
    except Exception:
        return None

def debug_print_offers(telegram_id: int, offers: list):
    if not DEBUG_PRINT_OFFERS:
        return
    print(f"[{datetime.now()}] 📥 Received {len(offers)} offer(s) for user {telegram_id}")
    for idx, offer in enumerate(offers, start=1):
        rid = (offer.get("rides") or [{}])[0]
        oid = offer.get("id")
        otype = (rid.get("type") or "—")
        vclass = (offer.get("vehicleClass") or "—")
        price = offer.get("price")
        currency = offer.get("currency") or ""
        pickup = rid.get("pickupTime")
        pu = _extract_addr(rid.get("pickUpLocation"))
        do = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else "—"
        plat = offer.get("_platform")
        print(
            f"[{datetime.now()}] 🧾 Offer {idx} [{plat}]: id={oid} • type={otype} • class={vclass} • "
            f"price={price} {currency} • pickup={pickup} • PU='{pu}' • DO='{do}'"
        )
        try:
            print(json.dumps(offer, indent=2, ensure_ascii=False))
        except Exception:
            print(str(offer))

def _process_offers_for_user(
    telegram_id: int,
    offers: List[Dict[str, Any]],
    filters: dict,
    class_state: dict,
    booked_slots: List[dict],
    blocked_days: set,
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]],
    tz_name: str,
    p1_token: Optional[str] = None,
    p2_token: Optional[str] = None,
):
    user_cfilters = _get_enabled_filter_slugs(telegram_id)

    for offer in offers:
        oid = offer.get("id")
        platform = offer.get("_platform", "p1")

        # Skip already processed (in-mem)
        if oid in accepted_per_user[telegram_id] or oid in rejected_per_user[telegram_id]:
            print(f"[{datetime.now()}] ⏭️ Skipping offer {oid} for user {telegram_id} – already processed (memory).")
            continue

        rid      = (offer.get("rides") or [{}])[0]
        price    = float(offer.get("price", 0) or 0)
        otype    = (rid.get("type") or "").lower()
        raw_vc   = offer.get("vehicleClass", "")
        pickup_s = rid.get("pickupTime")
        if not pickup_s:
            continue
        try:
            pickup   = parser.isoparse(pickup_s)  # aware
        except Exception:
            continue

        # Compute endsAt using formulas or duration
        ends_at_iso, end_calc = _compute_ends_at(offer, filters, pickup, tz_name)
        if ends_at_iso:
            rid["endsAt"] = ends_at_iso
        if end_calc:
            rid["_endsAtCalc"] = end_calc

        if DEBUG_ENDS:
            pid  = offer.get("id")
            kind = (rid.get("type") or "").lower()
            pu   = rid.get("pickupTime")
            end  = rid.get("endsAt")
            fstr = (end_calc or {}).get("formula") or "—"
            _log(
                f"OFFER[{pid}] type={kind} | pickup={_fmt_local_iso(pu, tz_name)} "
                f"| endsAt={_fmt_local_iso(end, tz_name)} | formula={fstr}"
            )

        # --- 0) Working hours & blocked days (user timezone) ---
        filter_results: List[dict] = []
        accept_override = False

        def record_result(name: str, ok: bool, detail: Optional[str] = None):
            filter_results.append({"name": name, "ok": bool(ok), "detail": detail})

        pickup_local = pickup.astimezone(gettz(tz_name))
        pickup_t = pickup_local.time()

        ws = filters.get("work_start")
        we = filters.get("work_end")
        if ws and we:
            ws_hm = _parse_hhmm(_to_str(ws))
            we_hm = _parse_hhmm(_to_str(we))
            if ws_hm and we_hm:
                start_t = dt_time(ws_hm[0], ws_hm[1])
                end_t   = dt_time(we_hm[0], we_hm[1])
                if not (start_t <= pickup_t <= end_t):
                    reason = f"heure pickup {pickup_t.strftime('%H:%M')} hors plage {ws}–{we}"
                    record_result("Horaires", False, reason)
                else:
                    record_result("Horaires", True, f"{pickup_t.strftime('%H:%M')} dans {ws}–{we}")

        day_key = pickup_local.strftime("%d/%m/%Y")
        if day_key in blocked_days:
            record_result("Jours bloqués", False, f"jour {day_key} bloqué (Schedule)")
        elif blocked_days:
            record_result("Jours bloqués", True, f"{day_key} autorisé")

        # 1) Minimal gap before pickup vs current time (UTC base)
        gap_min_now = filters.get("gap", 0)
        if gap_min_now:
            now_utc = datetime.now(timezone.utc)
            mins_left = max(0, (pickup - now_utc).total_seconds() / 60)
            if pickup < now_utc + timedelta(minutes=float(gap_min_now)):
                record_result(
                    "Délai minimal",
                    False,
                    f"{mins_left:.0f} min restants < seuil {gap_min_now} min",
                )
            else:
                record_result("Délai minimal", True, f"{mins_left:.0f} min restants")

        # 1.5) Enforce min/max hourly duration (filters in HOURS)
        dur_min_est = _duration_minutes_from_rid(rid)
        if otype == "hourly":
            min_hours = filters.get("min_hourly_hours", filters.get("min_duration", 0)) or 0
            max_hours = filters.get("max_hourly_hours", filters.get("max_duration"))  # may be None
            try:
                min_minutes = float(min_hours) * 60.0
            except Exception:
                min_minutes = 0.0
            try:
                max_minutes = float(max_hours) * 60.0 if max_hours is not None else None
            except Exception:
                max_minutes = None

            if min_minutes:
                ok = not (dur_min_est is None or dur_min_est < min_minutes)
                record_result(
                    "Durée horaire min",
                    ok,
                    None if ok else f"{0 if dur_min_est is None else dur_min_est:.0f} min < min {min_minutes:.0f} min",
                )
            if max_minutes is not None and dur_min_est is not None:
                ok = dur_min_est <= max_minutes
                record_result(
                    "Durée horaire max",
                    ok,
                    None if ok else f"{dur_min_est:.0f} min > max {max_minutes:.0f} min",
                )

        # Custom filters (user-defined)
        decision, reason_txt = _run_custom_filters(offer, user_cfilters, tz_name)
        if decision == "reject":
            if CF_DEBUG:
                print(f"[{datetime.now()}] ⛔ Custom filter rejected offer {oid}: {reason_txt}")
            record_result("Filtres personnalisés", False, reason_txt or "rejeté")
        elif decision == "accept":
            if CF_DEBUG:
                print(f"[{datetime.now()}] ✅ Custom filter accepted offer {oid}: {reason_txt or 'custom filter'}")
            accept_override = True
            record_result("Filtres personnalisés", True, reason_txt or "accepté")
        elif user_cfilters:
            record_result("Filtres personnalisés", True, "ok")

        # 2) Price filter
        min_p = float(filters.get("price_min", 0) or 0)
        max_p = float(filters.get("price_max", float("inf")))
        if min_p:
            ok = price >= min_p
            record_result("Prix min", ok, None if ok else f"prix {price} < minimum {min_p}")
        if max_p != float("inf"):
            ok = price <= max_p
            record_result("Prix max", ok, None if ok else f"prix {price} > maximum {max_p}")

        # 2.5) Distance filters
        if otype == "transfer":
            dist_m = rid.get("estimatedDistanceMeters")
            if dist_m is not None:
                try:
                    dist_m = float(dist_m)
                except Exception:
                    dist_m = None
            if dist_m is not None:
                min_km = float(filters.get("min_km", 0) or 0)
                max_km = float(filters.get("max_km", float("inf")) or float("inf"))
                min_m = min_km * 1000.0
                max_m = max_km * 1000.0
                dist_km = dist_m / 1000.0

                if min_km:
                    ok = dist_m >= min_m
                    record_result("Distance min", ok, None if ok else f"distance {dist_km:.1f} km < {min_km:g} km")
                if max_km != float("inf"):
                    ok = dist_m <= max_m
                    record_result("Distance max", ok, None if ok else f"distance {dist_km:.1f} km > {max_km:g} km")
        elif otype == "hourly":
            # Optional hourly km constraints if provided
            km_inc = rid.get("kmIncluded")
            try:
                km_inc = float(km_inc) if km_inc is not None else None
            except Exception:
                km_inc = None
            if km_inc is not None:
                h_min_km = filters.get("min_hourly_km")
                h_max_km = filters.get("max_hourly_km")
                if h_min_km is not None:
                    ok = km_inc >= float(h_min_km)
                    record_result("Km inclus min", ok, None if ok else f"{km_inc:g} < {float(h_min_km):g}")
                if h_max_km is not None:
                    ok = km_inc <= float(h_max_km)
                    record_result("Km inclus max", ok, None if ok else f"{km_inc:g} > {float(h_max_km):g}")

        # 3) Blacklists
        pickup_terms  = (filters.get("pickup_blacklist")  or [])
        dropoff_terms = (filters.get("dropoff_blacklist") or [])
        pu_addr = _extract_addr(rid.get("pickUpLocation"))
        do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else ""

        def _first_blacklist_hit(text: str, terms):
            if not text or not terms:
                return None
            low = text.lower()
            for term in terms:
                if term and term.strip() and term.lower() in low:
                    return term
            return None

        hit_pu = _first_blacklist_hit(pu_addr, pickup_terms)
        if pickup_terms:
            record_result("Pickup blacklist", hit_pu is None, None if hit_pu is None else f"pickup contient «{hit_pu}»")

        hit_do = _first_blacklist_hit(do_addr, dropoff_terms) if do_addr else None
        if dropoff_terms and do_addr:
            record_result("Dropoff blacklist", hit_do is None, None if hit_do is None else f"dropoff contient «{hit_do}»")

        # 4) Class filter
        otype_dict = class_state.get(otype, {})
        matched_vc = next((cls for cls in otype_dict.keys() if cls.lower() == raw_vc.lower()), None)
        enabled = otype_dict.get(matched_vc, 0) if matched_vc else 0
        record_result("Classe véhicule", bool(enabled), f"{otype} '{raw_vc}' désactivé" if not enabled else None)

        # 5) Booked-slots (user tz) – overlap using start & end
        ends_at_iso = rid.get("endsAt")
        offer_end_local = None
        if ends_at_iso:
            try:
                offer_end_local = parser.isoparse(ends_at_iso).astimezone(gettz(tz_name))
            except Exception:
                offer_end_local = None

        conflict_reason = None
        for slot in booked_slots:
            start_local = _parse_user_slot_local(slot.get("from"), tz_name)
            end_local   = _parse_user_slot_local(slot.get("to"), tz_name)
            if not start_local or not end_local:
                continue
            if end_local < start_local:
                start_local, end_local = end_local, start_local

            overlap = False
            if offer_end_local:
                if not (offer_end_local <= start_local or pickup_local >= end_local):
                    overlap = True
            else:
                if start_local <= pickup_local <= end_local:
                    overlap = True

            if overlap:
                slot_name = slot.get("name") or "Sans nom"
                conflict_reason = (
                    f"tombe dans créneau bloqué «{slot_name}» "
                    f"({start_local.strftime('%Y-%m-%d %H:%M')} → {end_local.strftime('%Y-%m-%d %H:%M')})"
                )
                break
        if booked_slots:
            record_result("Créneaux bloqués", conflict_reason is None, conflict_reason)

        # 5.5) Conflict with already accepted offers (busy intervals)
        conflict_with = _find_conflict(pickup, ends_at_iso, accepted_intervals)
        if conflict_with:
            a_start, a_end = conflict_with
            conflict_text = (
                "conflit avec une course acceptée "
                f"({_fmt_dt_local_from_dt(a_start, tz_name)} – {_fmt_dt_local_from_dt(a_end, tz_name)})"
            )
            record_result("Conflit trajets acceptés", False, conflict_text)
        elif accepted_intervals:
            record_result("Conflit trajets acceptés", True, "aucun conflit")

        # --- Final decision based on accumulated filters ---
        failed_filters = [fr for fr in filter_results if not fr["ok"]]
        summary_text = _format_filter_summary(filter_results)
        base_reason = "; ".join([fr["detail"] or fr["name"] for fr in failed_filters]) if failed_filters else None

        forced_accept_reason = None
        if accept_override and failed_filters:
            forced_accept_reason = f"accepté (filtre personnalisé) malgré: {base_reason}"

        is_rejected = bool(failed_filters) and not accept_override
        reason_for_log = forced_accept_reason or base_reason

        if is_rejected:
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {base_reason or 'filtres non respectés'}")
            log_offer_decision(telegram_id, offer, "rejected", reason_for_log or "filtres non respectés")
            full_text = _build_user_message(
                offer,
                "rejected",
                None,
                tz_name,
                summary_text,
                filter_results=filter_results,
                platform=platform,
                forced_accept=False,
            )
            header_line = _build_offer_header_line(offer, "rejected", platform, forced_accept=False)
            details_key = uuid.uuid4().hex[:16]
            save_offer_message(telegram_id, details_key, header_line, full_text)
            kb = {"inline_keyboard": [[{"text": "Show details", "callback_data": f"show_offer:{details_key}"}]]}
            maybe_send_message(
                telegram_id,
                "rejected",
                header_line,
                platform,
                reply_markup=kb,
            )
            rejected_per_user[telegram_id].add(oid)
            continue

        # Accept (either all filters OK or overridden by custom filter)
        print(f"[{datetime.now()}] ✅ Accepted {oid} [{platform}]")
        offer_to_log = deepcopy(offer)
        log_offer_decision(telegram_id, offer_to_log, "accepted", reason_for_log)
        full_text = _build_user_message(
            offer_to_log,
            "accepted",
            None,
            tz_name,
            summary_text,
            filter_results=filter_results,
            platform=platform,
            forced_accept=bool(accept_override and failed_filters),
        )
        header_line = _build_offer_header_line(offer_to_log, "accepted", platform, forced_accept=bool(accept_override and failed_filters))
        details_key = uuid.uuid4().hex[:16]
        save_offer_message(telegram_id, details_key, header_line, full_text)
        kb = {"inline_keyboard": [[{"text": "Show details", "callback_data": f"show_offer:{details_key}"}]]}
        maybe_send_message(
            telegram_id,
            "accepted",
            header_line,
            platform,
            reply_markup=kb,
        )
        accepted_per_user[telegram_id].add(oid)

        # Optionally auto-reserve the offer upstream
        if AUTO_RESERVE_ENABLED:
            try:
                if platform == "p1":
                    if p1_token:
                        rs, rb = reserve_offer_p1(p1_token, oid)
                        print(f"[{datetime.now()}] 🎯 P1 reserve {oid} -> {rs} | {rb}")
                    else:
                        print(f"[{datetime.now()}] ⚠️ P1 reserve skipped (no token) for user {telegram_id}")
                else:  # p2
                    if p2_token:
                        bid_price = offer_to_log.get("price")
                        if bid_price is None:
                            print(f"[{datetime.now()}] ⚠️ P2 reserve skipped (no price) for {oid}")
                        else:
                            rs, rb = reserve_offer_p2(p2_token, oid, float(bid_price))
                            print(f"[{datetime.now()}] 🎯 P2 reserve {oid} -> {rs} | {rb}")
                    else:
                        print(f"[{datetime.now()}] ⚠️ P2 reserve skipped (no portal token) for user {telegram_id}")
            except Exception as e:
                print(f"[{datetime.now()}] ❌ Auto-reserve error for {oid}: {e}")

        try:
            new_end_dt = parser.isoparse(offer_to_log["rides"][0].get("endsAt")) if offer_to_log["rides"][0].get("endsAt") else None
        except Exception:
            new_end_dt = None
        accepted_intervals.append((pickup, new_end_dt))

# =============================================================
#  Rides utilities (printing + intervals)
# =============================================================

def _extract_intervals_from_rides(rides: list) -> List[Tuple[datetime, Optional[datetime]]]:
    out: List[Tuple[datetime, Optional[datetime]]] = []
    for it in (rides or []):
        rid = it if isinstance(it, dict) else {}

        # start time (accept common keys)
        start_s = (
            rid.get("pickupTime") or rid.get("pickup_time") or
            rid.get("starts_at")  or rid.get("start_time")  or
            rid.get("pickup")     or rid.get("start")
        )
        if not start_s:
            continue
        try:
            start_dt = parser.isoparse(start_s)
        except Exception:
            continue

        # end time or duration
        end_dt = None
        end_s = rid.get("endsAt") or rid.get("ends_at") or rid.get("end_time")
        if end_s:
            try:
                end_dt = parser.isoparse(end_s)
            except Exception:
                end_dt = None
        if not end_dt:
            dur_min = _duration_minutes_from_rid(rid)
            if dur_min is not None:
                try:
                    end_dt = start_dt + timedelta(minutes=float(dur_min))
                except Exception:
                    end_dt = None

        out.append((start_dt, end_dt))
    return out

def _rides_snapshot_from_athena_payload(payload: dict, tz_name: str) -> str:
    data = (payload or {}).get("data") or []
    inc = (payload or {}).get("included") or []
    lines = [f"🛰️ Athena rides (planned) – showing {min(len(data), MAX_RIDES_SHOWN)}/{len(data)}"]
    for raw in data[:MAX_RIDES_SHOWN]:
        attrs = raw.get("attributes") or {}
        rel   = raw.get("relationships") or {}
        rid   = str(raw.get("id") or "—")
        starts_at = attrs.get("starts_at")
        booking_type = (attrs.get("booking_type") or "—").lower()
        est_dur = attrs.get("estimated_duration")
        try:
            dur_min = float(est_dur)/60.0 if est_dur is not None else None
        except Exception:
            dur_min = None
        distance = attrs.get("distance")

        pu_rel = _safe_attr(rel, "pickup_location", "data")
        do_rel = _safe_attr(rel, "dropoff_location", "data")

        pu = do = {}
        if pu_rel and pu_rel.get("id") and pu_rel.get("type"):
            inc_pu = _find_included(inc, pu_rel["type"], pu_rel["id"])
            pu = _extract_loc_from_included(inc_pu)
        if do_rel and do_rel.get("id") and do_rel.get("type"):
            inc_do = _find_included(inc, do_rel["type"], do_rel["id"])
            do = _extract_loc_from_included(inc_do)

        lines.append(
            "• <b>{typ}</b> · 🕒 {when}\n"
            "  ⬆️ {pu}\n"
            "  ⬇️ {do}\n"
            "  ⏱️ {dur} · 📏 {dist}\n"
            "  id: <code>{rid}</code>".format(
                typ=_esc(booking_type),
                when=_esc(_fmt_dt_local(starts_at, tz_name)),
                pu=_esc(pu.get("address") or pu.get("name") or "—"),
                do=_esc(do.get("address") or do.get("name") or "—"),
                dur=_esc(_fmt_minutes(dur_min)),
                dist=_esc(_fmt_km(distance)),
                rid=_esc(rid),
            )
        )
    return "\n".join(lines)

def _rides_snapshot_from_p1_list(rides: list, tz_name: str) -> str:
    lines = [f"📱 Mobile rides – showing {min(len(rides), MAX_RIDES_SHOWN)}/{len(rides)}"]
    for raw in rides[:MAX_RIDES_SHOWN]:
        starts = raw.get("pickupTime") or raw.get("pickup_time") or raw.get("start") or raw.get("starts_at")
        dur = _duration_minutes_from_rid(raw)
        pu = _extract_addr((raw.get("pickUpLocation") or {}))
        do = _extract_addr((raw.get("dropOffLocation") or {}))
        lines.append(
            "• 🕒 {when}\n  ⬆️ {pu}\n  ⬇️ {do}\n  ⏱️ {dur}".format(
                when=_esc(_fmt_dt_local(starts, tz_name)), pu=_esc(pu), do=_esc(do), dur=_esc(_fmt_minutes(dur))
            )
        )
    return "\n".join(lines)

def _dump_rides(telegram_id: int, text: str, platform: str):
    if DUMP_RIDES_IN_LOGS:
        # strip tags for logs
        print(f"[{datetime.now()}] {text.replace('<b>','').replace('</b>','').replace('<code>','').replace('</code>','')}")
    if DUMP_RIDES_IN_TELEGRAM:
        maybe_send_message(telegram_id, "accepted", text, platform)

# =============================================================
#  Per-user polling
# =============================================================

def _read_portal_creds(telegram_id: int) -> Tuple[Optional[str], Optional[str]]:
    try:
        creds = get_bl_account_full(telegram_id)
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
    telegram_id, token, filters_json, active = user

    tz_name = get_user_timezone(telegram_id) or "UTC"
    print(f"[{datetime.now()}] 🔍 Polling user {telegram_id} (active={active}) tz={tz_name}")

    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    # Load filters + normalize admin formulas once
    filters = json.loads(filters_json) if filters_json else {}
    formulas_raw = get_endtime_formulas(telegram_id)
    filters["__endtime_formulas__"] = _normalize_formulas(formulas_raw)

    class_state  = get_vehicle_classes_state(telegram_id)
    booked_slots = get_booked_slots(telegram_id)
    blocked_days = {d["day"] for d in get_blocked_days(telegram_id)}

    # ---------- Build busy intervals from Rides (Athena preferred) ----------
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]] = []
    bl_uuid = get_bl_uuid(telegram_id)
    email, password = _read_portal_creds(telegram_id)
    portal_token = None
    if email and password:
        portal_token = _ensure_portal_token(telegram_id, email, password)
        
    if ALWAYS_POLL_REAL_ORDERS:
        # Platform 2 rides (Athena)
        if portal_token:
            prev_etag = _athena_rides_etag.get(telegram_id)
            status_code, payload, new_etag = _athena_get_rides(portal_token, etag=prev_etag)
            if new_etag is not None:
                _athena_rides_etag[telegram_id] = new_etag

            if status_code == 200 and isinstance(payload, dict):
                data_all = (payload or {}).get("data") or []
                data_kept = _filter_rides_by_bl_uuid(data_all, bl_uuid) if bl_uuid else data_all
                filtered_payload = {"data": data_kept, "included": (payload or {}).get("included") or []}
                snap = _rides_snapshot_from_athena_payload(filtered_payload, tz_name)
                _dump_rides(telegram_id, snap, "p2")
                intervals_p2 = _extract_intervals_from_rides([
                    (r.get("attributes") or {}) | {"starts_at": (r.get("attributes") or {}).get("starts_at")}
                    for r in (data_kept or [])
                ])
                accepted_intervals.extend(intervals_p2)
                print(
                    f"[{datetime.now()}] 📚 Loaded {len(intervals_p2)} assigned interval(s) "
                    f"(kept {len(data_kept)}/{len(data_all)} rides) from Athena for user {telegram_id}"
                )
            elif status_code == 304:
                if ATHENA_PRINT_DEBUG:
                    print(f"[{datetime.now()}] 📦 Athena rides 304 Not Modified for user {telegram_id} (etag hit)")
            elif status_code in (401, 403):
                print(f"[{datetime.now()}] ⚠️ Athena rides unauthorized for user {telegram_id}.")
            elif status_code is None:
                print(f"[{datetime.now()}] ⚠️ Athena rides network error for user {telegram_id}")
            else:
                print(f"[{datetime.now()}] ⚠️ Athena rides returned status {status_code} for user {telegram_id}")

        # Platform 1 rides (mobile) — also fetch even if portal is present
        if token and str(token).strip():
            status_code, ride_results = get_rides_p1(token)
            if status_code == 200 and isinstance(ride_results, list):
                kept = _filter_rides_by_bl_uuid(ride_results, bl_uuid) if bl_uuid else ride_results
                snap = _rides_snapshot_from_p1_list(kept, tz_name)
                _dump_rides(telegram_id, snap, "p1")
                intervals_p1 = _extract_intervals_from_rides(kept)
                accepted_intervals.extend(intervals_p1)
                print(
                    f"[{datetime.now()}] 📚 Loaded {len(intervals_p1)} assigned interval(s) "
                    f"(kept {len(kept)}/{len(ride_results)} rides) from P1 /rides for user {telegram_id}"
                )

            elif status_code in (401, 403):
                set_token_status(telegram_id, "expired")
                existing = get_pinned_warnings(telegram_id)
                if not existing["expired_msg_id"]:
                    if existing["no_token_msg_id"]:
                        tg_unpin_message(telegram_id, existing["no_token_msg_id"])
                        clear_pinned_warning(telegram_id, "no_token")
                    pin_warning_if_needed(telegram_id, "expired")
            elif status_code is None:
                print(f"[{datetime.now()}] ⚠️ P1 /rides network error for user {telegram_id}")
            else:
                print(f"[{datetime.now()}] ⚠️ P1 /rides returned status {status_code} for user {telegram_id}")
        elif not portal_token:
            existing = get_pinned_warnings(telegram_id)
            if not existing["expired_msg_id"] and not existing["no_token_msg_id"]:
                pin_warning_if_needed(telegram_id, "no_token")

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
    else:
        if token and str(token).strip():
            status_code, results = get_offers_p1(token)
            if status_code in (401, 403):
                set_token_status(telegram_id, "expired")
                existing = get_pinned_warnings(telegram_id)
                if not existing["expired_msg_id"]:
                    if existing["no_token_msg_id"]:
                        tg_unpin_message(telegram_id, existing["no_token_msg_id"])
                        clear_pinned_warning(telegram_id, "no_token")
                    pin_warning_if_needed(telegram_id, "expired")
                print(f"[{datetime.now()}] ⚠️ P1 offers returned {status_code} for user {telegram_id}")
            elif status_code == 200:
                set_token_status(telegram_id, "valid")
                unpin_warning_if_any(telegram_id, "expired")
                unpin_warning_if_any(telegram_id, "no_token")
                # results already tagged with _platform='p1' in get_offers_p1
                offers_p1 = results or []
                print(f"[{datetime.now()}] 📥 P1 offers for user {telegram_id}: {len(offers_p1)}")
            else:
                print(f"[{datetime.now()}] ⚠️ P1 offers returned {status_code} for user {telegram_id} | body={results}")
        else:
            existing = get_pinned_warnings(telegram_id)
            if not existing["expired_msg_id"] and not existing["no_token_msg_id"]:
                pin_warning_if_needed(telegram_id, "no_token")

    # ---------- PLATFORM 2 OFFERS (Portal/Athena) ----------
    offers_p2: List[dict] = []
    if USE_MOCK_P2:
        portal_sample = {
            "data": [
                {
                    "id": "2254f2w94e-ba06-4b5ddb-aec3-25e0x9bddfwddwfdfww21ecdf05f",
                    "type": "offers",
                    "attributes": {
                        "starts_at": "2025-12-25T15:30:00-04:00",
                        "price": "236.39",
                        "currency": "USD",
                        "distance": 166560,
                        "service_class": "business",
                        "booking_type": "transfer",
                        "estimated_duration": 6680,
                    },
                    "relationships": {
                        "dropoff_location": {"data": {"id": "28714255", "type": "locations"}},
                        "pickup_location":  {"data": {"id": "28714254", "type": "locations"}},
                    },
                }
            ],
            "included": [
                {
                    "id": "28714254",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "Burlington International Airport (BTV), Airport Drive 1200, 05403 South Burlington, VT",
                        "city": "South Burlington",
                        "country_code": "US",
                        "airport_iata": "BTV",
                    },
                },
                {
                    "id": "28714255",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "Montreal-Trudeau (YUL), QC H4Y 1H1 Montreal, Dorval",
                        "city": "Montreal",
                        "country_code": "CA",
                        "airport_iata": "YUL",
                    },
                },
            ],
        }
        included = portal_sample.get("included") or []
        for raw in (portal_sample.get("data") or []):
            mapped = _map_portal_offer(raw, included)
            if mapped:
                offers_p2.append(mapped)
    else:
        if portal_token:
            prev_etag = _athena_offers_etag.get(telegram_id)
            status_code, payload, new_etag = _athena_get_offers(portal_token, etag=prev_etag)
            if new_etag is not None:
                _athena_offers_etag[telegram_id] = new_etag

            if ATHENA_PRINT_DEBUG:
                print(f"[{datetime.now()}] 🛰️ Athena offers status={status_code} for user {telegram_id}")

            if status_code in (401, 403):
                print(f"[{datetime.now()}] ⚠️ Athena token unauthorized for user {telegram_id}. Re-logging…")
                portal_token = _ensure_portal_token(telegram_id, email, password)
                if portal_token:
                    status_code, payload, new_etag = _athena_get_offers(portal_token, etag=_athena_offers_etag.get(telegram_id))
                    if new_etag is not None:
                        _athena_offers_etag[telegram_id] = new_etag
                    if ATHENA_PRINT_DEBUG:
                        print(f"[{datetime.now()}] 🛰️ Athena offers (after re-login) status={status_code} for user {telegram_id}")

            if status_code == 200 and isinstance(payload, dict):
                included = payload.get("included") or []
                for raw in (payload.get("data") or []):
                    mapped = _map_portal_offer(raw, included)
                    if mapped:
                        offers_p2.append(mapped)
                print(f"[{datetime.now()}] 📥 P2 offers for user {telegram_id}: {len(offers_p2)}")
            elif status_code == 304 and ATHENA_PRINT_DEBUG:
                print(f"[{datetime.now()}] 📦 Athena offers 304 Not Modified for user {telegram_id}")

    # ---------- Combine and process ----------
    all_offers = (offers_p1 or []) + (offers_p2 or [])
    if not all_offers:
        print(f"[{datetime.now()}] ℹ️ No offers for user {telegram_id} this cycle")
        return f"Done with user {telegram_id}"

    debug_print_offers(telegram_id, all_offers)

    _process_offers_for_user(
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

# =============================================================
#  Main loop – one thread per user
# =============================================================

if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 Poller started")
    while True:
        maybe_reset_inmem_caches()
        print(f"[{datetime.now()}] 🔄 Starting polling cycle")
        users = get_all_users()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(poll_user, u) for u in users]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        print(f"[{datetime.now()}] ✅ {res}")
                except Exception as e:
                    print(f"[{datetime.now()}] ❌ Poll error: {e}")
                    traceback.print_exc()
        time.sleep(POLL_INTERVAL)
