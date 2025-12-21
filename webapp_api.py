# webapp_api.py
import os, hmac, hashlib, json, urllib.parse, time, sqlite3, logging, uuid, re, sys
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any

# Pydantic v1 isn't yet Python 3.13-compatible: ForwardRef._evaluate now requires
# a keyword-only recursive_guard. Patch pydantic's helper before FastAPI imports.
if sys.version_info >= (3, 13):
    import pydantic.typing as _pydantic_typing

    def _evaluate_forwardref_py313(type_: Any, globalns: Any, localns: Any) -> Any:
        return type_._evaluate(globalns, localns, recursive_guard=set())

    _pydantic_typing.evaluate_forwardref = _evaluate_forwardref_py313

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from dateutil import parser
from dateutil.tz import gettz

# ----------------- DB helpers (existing) -----------------
from db import (
    DB_FILE, init_db,

    # slots/days
    get_booked_slots, add_booked_slot,
    get_blocked_days, add_blocked_day, delete_blocked_day,

    # filters
    list_all_custom_filters, create_custom_filter, update_custom_filter,
    assign_custom_filter, unassign_custom_filter,
    list_user_custom_filters, toggle_user_custom_filter,

    # accounts/tokens/profile
    set_bl_account, get_bl_account,           # may be sanitized (no password)
    get_portal_token, update_portal_token,
    get_bl_uuid, get_user_timezone, get_endtime_formulas,
    replace_endtime_formulas, add_endtime_formula, delete_endtime_formula,set_bl_uuid,
)

try:
    from db import delete_booked_slot
except Exception:
    delete_booked_slot = None

# ----------------- Env -----------------
load_dotenv()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
BOT_TOKEN   = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

API_HOST = "https://chauffeur-app-api.blacklane.com"  # mobile fallback
ATHENA_BASE = "https://athena.blacklane.com"
PORTAL_CLIENT_ID = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
PARTNER_PORTAL_API = os.getenv("PARTNER_PORTAL_API", "https://partner-portal-api.blacklane.com")

PORTAL_PAGE_SIZE = 50

# ----------------- Logging -----------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

# ----------------- App & CORS -----------------
app = FastAPI(title="MiniApp API")

ALLOWED_ORIGINS = [
    "http://localhost:3000",
    os.environ.get("NEXT_PUBLIC_SITE_URL", ""),
    os.environ.get("NEXT_PUBLIC_MINI_APP_ORIGIN", ""),
]
ALLOWED_ORIGINS = [o for o in ALLOWED_ORIGINS if o]

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------- Startup -----------------
@app.on_event("startup")
async def _startup():
    init_db()
    logging.info("🚀 FastAPI starting; DB_FILE=%s", DB_FILE)
    if ALLOWED_ORIGINS:
        logging.info("🌐 CORS allow_origins=%s", ", ".join(ALLOWED_ORIGINS))
    else:
        logging.info("🌐 CORS allow_origins=* (dev)")

# ----------------- Request logging -----------------
@app.middleware("http")
async def _log_requests(request: Request, call_next):
    rid = str(uuid.uuid4())[:8]
    start = time.time()
    client_ip = request.headers.get("x-forwarded-for") or (request.client.host if request.client else "?")
    method = request.method
    path   = request.url.path
    query  = request.url.query or ""
    ua     = (request.headers.get("user-agent") or "?")[:160]
    has_auth = "authorization" in {k.lower(): None for k in request.headers.keys()}
    logging.info("➡️  [%s] %s %s%s from %s | UA=%s | auth=%s", rid, method, path, f"?{query}" if query else "", client_ip, ua, has_auth)
    try:
        resp = await call_next(request)
        took = int((time.time() - start) * 1000)
        logging.info("⬅️  [%s] %s %s -> %s in %dms", rid, method, path, resp.status_code, took)
        resp.headers["x-req-id"] = rid
        return resp
    except Exception as e:
        took = int((time.time() - start) * 1000)
        logging.exception("💥 [%s] %s %s failed after %dms: %s", rid, method, path, took, e)
        raise

# ----------------- Auth helpers -----------------
def _validate_init_data(init_data_raw: str, max_age_sec: int = 600) -> dict:
    try:
        qs = urllib.parse.parse_qs(init_data_raw, strict_parsing=True)
    except Exception:
        logging.warning("initData parse error")
        raise HTTPException(401, "Bad initData")

    data = {k: v[0] for k, v in qs.items()}
    tg_hash = data.pop("hash", None)
    if not tg_hash:
        logging.warning("initData missing hash")
        raise HTTPException(401, "Missing hash")

    data_check_string = "\n".join(f"{k}={data[k]}" for k in sorted(data.keys()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    check = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(check, tg_hash):
        logging.warning("initData HMAC mismatch")
        raise HTTPException(401, "Invalid initData")

    if max_age_sec and "auth_date" in data:
        try:
            age = time.time() - int(data["auth_date"])
            if age > max_age_sec:
                logging.warning("initData expired: age=%.1fs", age)
                raise HTTPException(401, "initData expired")
        except Exception:
            logging.warning("initData bad auth_date")
            raise HTTPException(401, "Bad auth_date")

    user = json.loads(data.get("user", "{}")) if "user" in data else {}
    if not user.get("id"):
        logging.warning("initData has no user.id")
        raise HTTPException(401, "No user in initData")
    return user

def _require_user(auth_header: Optional[str]) -> int:
    if not auth_header or not auth_header.startswith("tma "):
        raise HTTPException(401, "Use header: Authorization: tma <initData>")
    init_data_raw = auth_header[4:]
    user = _validate_init_data(init_data_raw)
    uid = int(user["id"])
    logging.info("🔑 Auth OK for user_id=%s", uid)
    return uid

def _require_user_from_any(auth_header: Optional[str], tma_qs: Optional[str]) -> int:
    if auth_header and auth_header.startswith("tma "):
        return _require_user(auth_header)
    if tma_qs:
        user = _validate_init_data(tma_qs)
        uid = int(user["id"])
        logging.info("🔑 Auth OK (query) uid=%s", uid)
        return uid
    raise HTTPException(401, "Provide Authorization: tma <initData> or ?tma=<initData>")

def _require_admin(Authorization: Optional[str]):
    if not Authorization or not Authorization.startswith("admin "):
        raise HTTPException(401, "Use Authorization: admin <token>")
    token = Authorization[6:].strip()
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")

# ----------------- Date/time utils -----------------
ACCEPTED_FORMATS = (
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
)

def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ACCEPTED_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _fmt_ddmmyyyy(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y %H:%M")

def _parse_day_ddmmyyyy(s: str) -> datetime | None:
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y")
    except Exception:
        return None

def _fmt_day_ddmmyyyy(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y")

# ----------------- Helpers for formulas/endsAt -----------------
def _to_str(x):
    if x is None: return None
    if isinstance(x, (bytes, bytearray)):
        try: return x.decode("utf-8", "ignore")
        except Exception: return str(x)
    return str(x)

def _to_int(x, default=0):
    try:
        if isinstance(x, (int, float)): return int(x)
        s = _to_str(x)
        if s is None: return default
        m = re.search(r"-?\d+", s)
        return int(m.group(0)) if m else default
    except Exception:
        return default

def _parse_hhmm(s):
    try:
        s = _to_str(s); parts = (s or "").split(":")
        if len(parts) < 2: return None
        hh = _to_int(parts[0], None); mm = _to_int(parts[1], None)
        if hh is None or mm is None: return None
        if not (0 <= hh <= 23 and 0 <= mm <= 59): return None
        return hh, mm
    except Exception:
        return None

def _time_in_interval(t, start_s, end_s):
    if start_s is None or end_s is None: return False
    shsm = _parse_hhmm(start_s); ehm = _parse_hhmm(end_s)
    if not shsm or not ehm: return False
    sh, sm = shsm; eh, em = ehm
    cur = (t.hour, t.minute); start = (sh, sm); end = (eh, em)
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end  # wraps midnight

def _normalize_formulas(rows):
    out = []
    for r0 in (rows or []):
        r = dict(r0 or {})
        r["start"] = _to_str(r.get("start"))
        r["end"] = _to_str(r.get("end"))
        r["priority"] = _to_int(r.get("priority"), 0)
        try: r["speed_kmh"] = float(_to_str(r.get("speed_kmh") or 0) or 0)
        except Exception: r["speed_kmh"] = 0.0
        try: r["bonus_min"] = float(_to_str(r.get("bonus_min") or 0) or 0)
        except Exception: r["bonus_min"] = 0.0
        out.append(r)
    return out

def _pick_formula_for_pickup(formulas: list, pickup_dt: datetime, tz_name: str):
    if not formulas: return None
    local_t = pickup_dt.astimezone(gettz(tz_name)).time()
    fallback = None
    for row in sorted(formulas, key=lambda r: _to_int((r or {}).get("priority"), 0)):
        if not isinstance(row, dict): continue
        st = _to_str(row.get("start")); en = _to_str(row.get("end"))
        if st and en:
            if _time_in_interval(local_t, st, en):
                return row
        elif not st and not en:
            fallback = row
    return fallback

def _duration_minutes_from_rid(rid: dict) -> Optional[float]:
    if not isinstance(rid, dict): return None
    for k in ("durationMinutes","estimatedDurationMinutes","duration_minutes","estimated_duration_minutes"):
        v = rid.get(k)
        if v is not None:
            try: return float(v)
            except Exception: pass
    for k in ("estimatedDurationSeconds","durationSeconds","estimated_duration_seconds","duration_seconds","estimated_duration","estimatedDuration"):
        v = rid.get(k)
        if v is not None:
            try:
                v = float(v)
                return v/60.0 if v > 1000 else v
            except Exception:
                pass
    return None

def _compute_ends_at_for_ride(ride: dict, formulas: list, pickup_dt: datetime, tz_name: str) -> Optional[str]:
    otype = (ride.get("type") or ride.get("rideType") or "").lower()
    if otype == "hourly":
        dur_min = _duration_minutes_from_rid(ride)
        if dur_min:
            return (pickup_dt + timedelta(minutes=float(dur_min))).isoformat()
        return None

    if otype == "transfer":
        dist_m = ride.get("estimatedDistanceMeters") or ride.get("distance")
        try:
            if dist_m is not None: dist_m = float(dist_m)
        except Exception:
            dist_m = None

        rule = _pick_formula_for_pickup(formulas, pickup_dt, tz_name)
        if rule and dist_m is not None:
            try:
                speed = float(rule.get("speed_kmh") or 0.0)
                bonus = float(rule.get("bonus_min") or 0.0)
                dist_km = float(dist_m) / 1000.0
                one_way_min = (dist_km / speed) * 60.0 if speed > 0 else 0.0
                total_min = one_way_min * 2.0 + bonus
                return (pickup_dt + timedelta(minutes=total_min)).isoformat()
            except Exception:
                pass

        dur = _duration_minutes_from_rid(ride)
        if dur is not None:
            return (pickup_dt + timedelta(minutes=float(dur))).isoformat()
        return None
    return None

# ----------------- Internal creds helper (fetch real password) -----------------
def _get_bl_creds_from_db(uid: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Try a few likely places to find email+password.
    Adjust the SQL to your actual schema if needed.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()

        # Try users table first
        try:
            cur.execute("SELECT bl_email, bl_password FROM users WHERE telegram_id = ?", (uid,))
            row = cur.fetchone()
            if row and (row[0] or row[1]):
                email = (row[0] or "").strip()
                password = (row[1] or "").strip()
                conn.close()
                logging.info("🔎 creds(users): email=%s, len(password)=%d", email, len(password or ""))
                return (email or None), (password or None)
        except Exception:
            pass

        # Try dedicated bl_accounts table
        try:
            cur.execute("SELECT email, password FROM bl_accounts WHERE telegram_id = ?", (uid,))
            row = cur.fetchone()
            if row and (row[0] or row[1]):
                email = (row[0] or "").strip()
                password = (row[1] or "").strip()
                conn.close()
                logging.info("🔎 creds(bl_accounts): email=%s, len(password)=%d", email, len(password or ""))
                return (email or None), (password or None)
        except Exception:
            pass

        # Try generic accounts table
        try:
            cur.execute("SELECT email, password FROM accounts WHERE telegram_id = ?", (uid,))
            row = cur.fetchone()
            if row and (row[0] or row[1]):
                email = (row[0] or "").strip()
                password = (row[1] or "").strip()
                conn.close()
                logging.info("🔎 creds(accounts): email=%s, len(password)=%d", email, len(password or ""))
                return (email or None), (password or None)
        except Exception:
            pass

        conn.close()
    except Exception as e:
        logging.warning("get_bl_creds_from_db failed: %s", e)

    logging.info("🔸 No raw creds found in DB for uid=%s", uid)
    return None, None

# ----------------- Athena/Hades helpers -----------------
def _athena_login(email: str, password: str) -> Tuple[bool, Optional[str], str]:
    url = f"{ATHENA_BASE}/oauth/token"
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
            j = r.json() or {}
            tok = (j.get("result") or {}).get("access_token") or j.get("access_token")
            if tok:
                return True, tok, "ok"
            return False, None, "no_token"
        if r.status_code in (401, 403):
            return False, None, f"unauthorized:{r.status_code}"
        return False, None, f"upstream:{r.status_code}"
    except requests.exceptions.RequestException as e:
        return False, None, f"network:{type(e).__name__}"
    
def _portal_get_me(access_token: str) -> Tuple[Optional[int], Optional[dict]]:
    url = f"{PARTNER_PORTAL_API}/me"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "BLPortal/uuid-fetch (+miniapp)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, None
        return r.status_code, None
    except requests.exceptions.RequestException:
        return None, None


def _hades_fetch_plain(token: str, page: int, page_size: int) -> Tuple[int, Optional[dict]]:
    url = (
        f"{ATHENA_BASE}/hades/rides"
        f"?page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
        f"&include=pickup_location%2Cdropoff_location%2Caccepted_by%2Cassigned_driver%2Cassigned_vehicle%2Cavailable_drivers%2Cavailable_vehicles%2Cstatus_updates"
        f"&filter%5Bgroup%5D=planned"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.api+json",
        "Connection": "keep-alive",
        "User-Agent": "BLPortal/1.0 (+miniapp)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        try:
            body = r.json()
        except Exception:
            body = None
        logging.info("🌐 Hades GET (plain) -> %s", r.status_code)
        return r.status_code, body
    except requests.exceptions.RequestException:
        logging.warning("Hades network error")
        return 0, None

def _fetch_hades_with_login_flow(uid: int, page: int, page_size: int) -> Tuple[int, Optional[dict]]:
    """
    Steps:
    1) If email+password and NO portal_token -> login, store, fetch.
    2) If email+password and portal_token -> use it; on 401/403 -> login, store, retry.
    3) If no email+password -> let caller fallback to mobile.
    """
    email, password = _get_bl_creds_from_db(uid)
    if not (email and password):
        logging.info("🔸 No portal credentials for uid=%s", uid)
        return 0, None  # caller falls back to mobile

    tok = get_portal_token(uid)
    if isinstance(tok, (list, tuple)):
        tok = tok[0] if tok else None
    logging.info("🔑 Current portal_token: %s", "present" if tok else "not present")

    # A) creds present, no token -> login
    if not tok:
        logging.info("🔑 No portal_token in DB; logging in (uid=%s)", uid)
        ok, new_tok, note = _athena_login(email, password)
        if not ok or not new_tok:
            logging.warning("❌ Athena login failed (uid=%s): %s", uid, note)
            return 401, None
        update_portal_token(uid, new_tok)
        tok = new_tok
        sc, body = _hades_fetch_plain(tok, page, page_size)
        return sc, body

    # B) creds present + token present -> use token
    logging.info("🟢 Using existing portal_token for uid=%s", uid)
    sc, body = _hades_fetch_plain(tok, page, page_size)
    if sc in (401, 403):
        logging.info("🔁 Token rejected (%s). Logging in again (uid=%s).", sc, uid)
        ok, new_tok, note = _athena_login(email, password)
        if ok and new_tok:
            update_portal_token(uid, new_tok)
            sc, body = _hades_fetch_plain(new_tok, page, page_size)
        else:
            logging.warning("❌ Re-login failed (uid=%s): %s", uid, note)
    return sc, body

# ----------------- Mapping/filtering for Hades -----------------
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

def _athena_assigned_driver_id(raw_ride: dict) -> Optional[str]:
    if not isinstance(raw_ride, dict):
        return None
    rel = raw_ride.get("relationships") or {}
    node = (rel.get("assigned_driver") or rel.get("assignedDriver") or {}).get("data")
    if isinstance(node, dict):
        did = node.get("id")
        return str(did) if did else None
    return None

def _filter_rides_by_bl_uuid(raw_items: list, bl_uuid: str) -> list:
    if not bl_uuid:
        return raw_items or []
    out = []
    for it in (raw_items or []):
        if isinstance(it, dict) and "relationships" in it:
            did = _athena_assigned_driver_id(it)
            if did and str(did) == str(bl_uuid):
                out.append(it); continue
        ch = (it or {}).get("chauffeur") or {}
        did = ch.get("id")
        if did and str(did) == str(bl_uuid):
            out.append(it)
    return out

def _map_athena_ride_to_ui(raw: dict, included: list, tz_name: str, formulas: list) -> dict:
    attrs = raw.get("attributes") or {}
    rel   = raw.get("relationships") or {}
    rid   = str(raw.get("id") or "")

    booking_type  = (attrs.get("booking_type") or "").lower() or "transfer"
    service_class = attrs.get("service_class") or attrs.get("vehicle_class") or ""
    starts_at     = attrs.get("starts_at") or attrs.get("pickup_time") or attrs.get("start_time")
    est_dur       = attrs.get("estimated_duration")
    try:
        est_dur_min = float(est_dur)/60.0 if est_dur is not None else None
    except Exception:
        est_dur_min = None
    distance = attrs.get("distance")

    pu_rel = _safe_attr(rel, "pickup_location", "data")
    do_rel = _safe_attr(rel, "dropoff_location", "data")

    pu = do = {}
    if pu_rel and pu_rel.get("id") and pu_rel.get("type"):
        inc_pu = _find_included(included, pu_rel["type"], pu_rel["id"])
        pu = _extract_loc_from_included(inc_pu)
    if do_rel and do_rel.get("id") and do_rel.get("type"):
        inc_do = _find_included(included, do_rel["type"], do_rel["id"])
        do = _extract_loc_from_included(inc_do)

    item = {
        "id": rid,
        "bookingNumber": rid,
        "rideType": booking_type,
        "vehicleClass": service_class,
        "pickupTime": starts_at,
        "estimatedDurationMinutes": est_dur_min,
        "estimatedDuration": est_dur,
        "estimatedDistanceMeters": distance,
        "pickupLocation": pu,
        "dropoffLocation": do or None,
        "rideStatus": attrs.get("status") or attrs.get("ride_status"),
        "flight": {"number": attrs.get("flight_number")} if attrs.get("flight_number") else None,
        "_platform": "p2",
    }

    if starts_at:
        try:
            pickup_dt = parser.isoparse(starts_at)
            ends_at_iso = _compute_ends_at_for_ride(
                {
                    "type": booking_type,
                    "pickupTime": starts_at,
                    "estimatedDurationMinutes": est_dur_min,
                    "estimatedDuration": est_dur,
                    "estimatedDistanceMeters": distance,
                },
                formulas, pickup_dt, tz_name
            )
            if ends_at_iso:
                item["endsAt"] = ends_at_iso
        except Exception:
            pass
    return item

# ----------------- Pydantic models -----------------
class CreateSlotIn(BaseModel):
    start: str
    end: str
    name: Optional[str] = None

class ToggleDayIn(BaseModel):
    day: str  # dd/mm/YYYY

class AdminCreateCF(BaseModel):
    slug: str
    name: str
    description: Optional[str] = ""
    params: Optional[dict] = None
    global_enabled: bool = True
    rule_kind: Optional[str] = "generic"
    rule_code: Optional[str] = None

class AdminPatchCF(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    params: Optional[dict] = None
    global_enabled: Optional[bool] = None
    rule_kind: Optional[str] = None
    rule_code: Optional[str] = None

class AdminToggleUserCF(BaseModel):
    enabled: bool

class ToggleMyCF(BaseModel):
    enabled: bool

class BLAccountIn(BaseModel):
    email: str
    password: str

class EndtimeFormulaIn(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None
    speed_kmh: float
    bonus_min: float = 0
    priority: int = 0
    @validator("start", "end")
    def _valid_hhmm(cls, v):
        if v is None: return v
        parts = v.split(":")
        if len(parts) != 2: raise ValueError("HH:MM")
        h, m = parts
        h = int(h); m = int(m)
        if not (0 <= h <= 23 and 0 <= m <= 59): raise ValueError("HH:MM")
        return f"{h:02d}:{m:02d}"

# ----------------- Utility: mobile token -----------------
def _get_mobile_token(uid: int) -> Optional[str]:
    try:
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("SELECT token FROM users WHERE telegram_id = ?", (uid,))
        row = cur.fetchone(); conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None

# ----------------- Endpoints -----------------
@app.get("/debug/ping")
def ping():
    return {"ok": True, "ts": int(time.time())}

@app.get("/debug/whoami")
def whoami(Authorization: Optional[str] = Header(default=None)):
    try:
        uid = _require_user(Authorization)
        return {"ok": True, "user_id": uid}
    except HTTPException as e:
        return {"ok": False, "error": e.detail}

# --- Slots ---
@app.get("/webapp/slots")
def list_slots(Authorization: Optional[str] = Header(default=None), tma: Optional[str] = Query(default=None)):
    uid = _require_user_from_any(Authorization, tma)
    rows = get_booked_slots(uid)
    logging.info("📦 list_slots user=%s -> %d rows", uid, len(rows or []))
    return {"slots": rows}

@app.post("/webapp/slots")
def create_slot(payload: CreateSlotIn, Authorization: Optional[str] = Header(default=None)):
    uid = _require_user(Authorization)
    dt_start = _parse_dt(payload.start); dt_end = _parse_dt(payload.end)
    if not (dt_start and dt_end):
        raise HTTPException(400, detail={
            "error": "bad_date_format",
            "received": {"start": payload.start, "end": payload.end},
            "accepted": list(ACCEPTED_FORMATS),
            "hint": "Use HTML5 datetime-local or dd/mm/yyyy HH:MM",
        })
    add_booked_slot(uid, _fmt_ddmmyyyy(dt_start), _fmt_ddmmyyyy(dt_end), payload.name or None)
    return {"ok": True}

@app.delete("/webapp/slots/{slot_id}")
def delete_slot(slot_id: int, Authorization: Optional[str] = Header(default=None)):
    uid = _require_user(Authorization)
    if delete_booked_slot:
        try:
            delete_booked_slot(slot_id)
            return {"ok": True}
        except Exception as e:
            logging.warning("custom deleter failed: %s", e)
    try:
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("DELETE FROM booked_slots WHERE id = ? AND telegram_id = ?", (slot_id, uid))
        if cur.rowcount == 0:
            cur.execute("DELETE FROM booked_slots WHERE id = ?", (slot_id,))
        conn.commit(); conn.close()
        return {"ok": True}
    except Exception as e:
        logging.exception("delete_slot SQL error: %s", e)
        raise HTTPException(500, "Could not delete")

# --- Days ---
@app.get("/webapp/days")
def list_blocked_days(Authorization: Optional[str] = Header(default=None), tma: Optional[str] = Query(default=None)):
    uid = _require_user_from_any(Authorization, tma)
    rows = get_blocked_days(uid)
    return {"days": [r["day"] for r in rows]}

@app.post("/webapp/days/toggle")
def toggle_blocked_day(payload: ToggleDayIn, Authorization: Optional[str] = Header(default=None)):
    uid = _require_user(Authorization)
    dt = _parse_day_ddmmyyyy((payload.day or "").strip())
    if not dt:
        raise HTTPException(400, detail={"error": "bad_day_format", "expected": "dd/mm/YYYY", "got": payload.day})
    day = _fmt_day_ddmmyyyy(dt)
    existing = {d["day"] for d in get_blocked_days(uid)}
    if day in existing:
        for d in get_blocked_days(uid):
            if d["day"] == day:
                delete_blocked_day(d["id"])
                return {"ok": True, "blocked": False, "day": day}
        raise HTTPException(404, "Not found")
    else:
        add_blocked_day(uid, day)
        return {"ok": True, "blocked": True, "day": day}

# --- Rides (Hades first per your steps; fallback to mobile ONLY if no creds) ---
@app.get("/webapp/rides")
def list_user_rides(
    Authorization: Optional[str] = Header(default=None),
    page: int = Query(1, ge=1),
    limit: int = Query(30, ge=1, le=200),
    status: Optional[str] = Query(default=None),
):
    uid = _require_user(Authorization)

    tz_name = get_user_timezone(uid) or "UTC"
    formulas = _normalize_formulas(get_endtime_formulas(uid))

    normalized: List[dict] = []
    used_platform = None

    # Try Athena/Hades if raw creds exist
    email, password = _get_bl_creds_from_db(uid)
    have_creds = bool((email or "").strip() and (password or "").strip())
    logging.info("👤 have_creds=%s (email=%s, len(password)=%d)", have_creds, (email or ""), len(password or ""))

    if have_creds:
        sc, payload = _fetch_hades_with_login_flow(uid, page=page, page_size=limit)
        logging.info("Hades flow result status=%s", sc)
        if sc and 200 <= sc < 300:
            data_all = (payload or {}).get("data") if isinstance(payload, dict) else []
            included = (payload or {}).get("included") if isinstance(payload, dict) else []
            bl_uuid = get_bl_uuid(uid)
            kept = _filter_rides_by_bl_uuid(data_all or [], bl_uuid) if bl_uuid else (data_all or [])

            for raw in kept:
                try:
                    item = _map_athena_ride_to_ui(raw, included or [], tz_name, formulas)
                    if item:
                        normalized.append(item)
                except Exception as e:
                    logging.warning("map athena ride failed: %s", e)
            used_platform = "p2"

            # SUCCESS even if zero rides
            def _start_ts(x):
                s = x.get("pickupTime") or x.get("starts_at")
                try:
                    return parser.isoparse(s).timestamp() if s else float("inf")
                except Exception:
                    return float("inf")
            normalized.sort(key=_start_ts)
            return JSONResponse(status_code=200, content={"results": normalized, "platform": used_platform})

        # creds exist but Hades failed → per your spec, DO NOT fallback to mobile; surface error
        if sc in (401, 403):
            raise HTTPException(403, detail="upstream_error")
        elif sc:
            raise HTTPException(502, detail="upstream_error")
        else:
            raise HTTPException(502, detail="upstream_error")

    # No creds → fallback to mobile /rides
    token = _get_mobile_token(uid)
    if not token:
        raise HTTPException(401, detail={"error": "no_token", "hint": "Add your mobile session token or portal account."})

    headers = {
        "Host": API_HOST.replace("https://", ""),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
    }

    try:
        r = requests.get(f"{API_HOST}/rides", headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            try:
                data = r.json()
            except Exception:
                data = []

            if isinstance(data, list):
                raw_list = data
            elif isinstance(data, dict):
                raw_list = data.get("results") or data.get("rides") or data.get("data") or data.get("items") or []
            else:
                raw_list = []

            bl_uuid = get_bl_uuid(uid)
            kept = _filter_rides_by_bl_uuid(raw_list, bl_uuid) if bl_uuid else raw_list

            for it in kept:
                try:
                    starts = it.get("pickupTime") or it.get("pickup_time") or it.get("start") or it.get("starts_at")
                    if starts:
                        pickup_dt = parser.isoparse(starts)
                        rid = {
                            "type": (it.get("rideType") or it.get("type") or "").lower(),
                            "pickupTime": starts,
                            "estimatedDurationMinutes": it.get("estimatedDurationMinutes"),
                            "estimatedDuration": it.get("estimatedDuration"),
                            "estimatedDistanceMeters": it.get("estimatedDistanceMeters") or it.get("distance"),
                        }
                        ends_at = _compute_ends_at_for_ride(rid, formulas, pickup_dt, tz_name)
                        if ends_at:
                            it["endsAt"] = ends_at
                    it["_platform"] = "p1"
                    normalized.append(it)
                except Exception as e:
                    logging.warning("normalize p1 ride failed: %s", e)

            used_platform = "p1"

            def _start_ts(x):
                s = x.get("pickupTime") or x.get("starts_at")
                try:
                    return parser.isoparse(s).timestamp() if s else float("inf")
                except Exception:
                    return float("inf")
            normalized.sort(key=_start_ts)
            return JSONResponse(status_code=200, content={"results": normalized, "platform": used_platform})
        else:
            logging.warning("Mobile /rides upstream status=%s", r.status_code)
            raise HTTPException(r.status_code, detail="upstream_error")
    except requests.exceptions.RequestException:
        raise HTTPException(502, detail="upstream_error")

# --- Filters UI (mini-app) ---
@app.get("/webapp/custom-filters")
def webapp_list_my_filters(Authorization: Optional[str] = Header(default=None), tma: Optional[str] = Query(default=None)):
    uid = _require_user_from_any(Authorization, tma)
    rows = list_user_custom_filters(uid)
    items = []
    for r in rows:
        effective_enabled = bool(r.get("user_enabled")) and bool(r.get("global_enabled", 1))
        items.append({"slug": r["slug"], "name": r["name"], "enabled": effective_enabled})
    return {"filters": items}

@app.post("/webapp/custom-filters/{slug}/toggle")
def webapp_toggle_my_filter(slug: str, payload: ToggleMyCF, Authorization: Optional[str] = Header(default=None)):
    uid = _require_user(Authorization)
    toggle_user_custom_filter(uid, slug, bool(payload.enabled))
    return {"ok": True}

# --- BL portal account (mini-app) ---
@app.get("/webapp/bl-account")
def webapp_get_bl_account(Authorization: Optional[str] = Header(default=None), tma: Optional[str] = Query(default=None)):
    uid = _require_user_from_any(Authorization, tma)
    acc = get_bl_account(uid) or {}
    return {"email": acc.get("email")}

@app.post("/webapp/bl-account")
def webapp_save_bl_account(
    payload: BLAccountIn,
    Authorization: Optional[str] = Header(default=None),
    tma: Optional[str] = Query(default=None),
):
    uid = _require_user_from_any(Authorization, tma)

    email = (payload.email or "").strip()
    password = (payload.password or "").strip()
    if not email or not password:
        raise HTTPException(400, detail={"error": "missing_fields"})

    # 1) Save credentials
    set_bl_account(uid, email, password)

    # 2) Login to Athena
    ok, token, note = _athena_login(email, password)
    if not ok or not token:
        if str(note).startswith("unauthorized:"):
            # exact per your ask
            raise HTTPException(status_code=401, detail={"error": "invalid_credentials"})
        raise HTTPException(status_code=502, detail={"error": "portal_login_failed", "note": note})

    # 3) Persist portal token
    update_portal_token(uid, token)

    # 4) Fetch UUID from Partner Portal /me
    status, me = _portal_get_me(token)
    if not status:
        raise HTTPException(status_code=502, detail={"error": "portal_me_failed", "note": "network"})
    if not (200 <= status < 300) or not isinstance(me, dict):
        raise HTTPException(status_code=502, detail={"error": "portal_me_failed", "status": status})

    bl_id = (me or {}).get("id")
    if isinstance(bl_id, str) and bl_id.strip():
        set_bl_uuid(uid, bl_id.strip())
        return {"ok": True, "uuid": bl_id}

    # Token worked but response lacked id
    raise HTTPException(status_code=502, detail={"error": "portal_me_no_id"})


# --- Admin: users listing ---
@app.get("/admin/users")
def admin_users(Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("""
        SELECT
          telegram_id, active, bl_email,
          tg_first_name, tg_last_name, tg_username, tg_lang, tg_is_premium,
          tg_last_seen, tg_first_seen, tg_chat_type, tg_chat_id, tg_chat_title
        FROM users
        ORDER BY telegram_id ASC
    """)
    rows = cur.fetchall(); conn.close()

    users = []
    for r in rows:
        (uid, active, email,
         first, last, uname, lang, is_prem,
         last_seen, first_seen, chat_type, chat_id, chat_title) = r
        users.append({
            "telegram_id": uid,
            "active": bool(active),
            "email": email or "",
            "tg": {
                "first_name": first or "",
                "last_name":  last or "",
                "username":   uname or "",
                "language_code": lang or "",
                "is_premium": bool(is_prem or 0),
                "first_seen": first_seen or "",
                "last_seen":  last_seen or "",
                "chat_type":  chat_type or "",
                "chat_id":    chat_id,
                "chat_title": chat_title or "",
            },
        })
    return {"users": users}

# --- Admin: list all filters ---
@app.get("/admin/custom-filters")
def admin_list_filters(Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    return {"filters": list_all_custom_filters()}

@app.post("/admin/custom-filters")
def admin_create_filter(payload: AdminCreateCF, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    slug = payload.slug.strip().lower()
    if not slug or not all(ch.isalnum() or ch in "-_." for ch in slug):
        raise HTTPException(400, "Bad slug")
    create_custom_filter(
        slug,
        payload.name.strip(),
        payload.description or "",
        payload.params or {},
        payload.global_enabled,
        rule_kind=(payload.rule_kind or "generic"),
        rule_code=payload.rule_code,
    )
    return {"ok": True}

@app.patch("/admin/custom-filters/{slug}")
def admin_update_filter(slug: str, payload: AdminPatchCF, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    update_custom_filter(slug, **{k:v for k,v in payload.dict().items() if v is not None})
    return {"ok": True}

# --- Admin: assign/unassign/toggle for a user ---
@app.get("/admin/users/{telegram_id}/custom-filters")
def admin_user_filters(telegram_id: int, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    assigned = list_user_custom_filters(telegram_id)
    return {"assigned": assigned, "all": list_all_custom_filters()}

@app.post("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_assign_cf(telegram_id: int, slug: str, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    assign_custom_filter(telegram_id, slug, True)
    return {"ok": True}

@app.delete("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_unassign_cf(telegram_id: int, slug: str, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    unassign_custom_filter(telegram_id, slug)
    return {"ok": True}

@app.patch("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_toggle_user_cf(telegram_id: int, slug: str, payload: AdminToggleUserCF, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    toggle_user_custom_filter(telegram_id, slug, payload.enabled)
    return {"ok": True}

# --- Admin: endtime formulas for a user ---
@app.get("/admin/users/{telegram_id}/endtime-formulas")
def admin_list_formulas(telegram_id: int, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    return {"user": telegram_id, "formulas": get_endtime_formulas(telegram_id)}

@app.put("/admin/users/{telegram_id}/endtime-formulas")
def admin_replace_user_formulas(telegram_id: int, payload: List[EndtimeFormulaIn], Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    items = [p.dict() for p in payload]
    replace_endtime_formulas(telegram_id, items)
    return {"ok": True, "count": len(items)}

@app.post("/admin/users/{telegram_id}/endtime-formulas")
def admin_add_user_formula(telegram_id: int, payload: EndtimeFormulaIn, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    p = payload.dict()
    add_endtime_formula(
        telegram_id,
        p.get("start"), p.get("end"),
        p["speed_kmh"], p.get("bonus_min", 0),
        p.get("priority", 0),
    )
    return {"ok": True}

@app.delete("/admin/users/{telegram_id}/endtime-formulas/{formula_id}")
def admin_delete_user_formula(telegram_id: int, formula_id: int, Authorization: Optional[str] = Header(default=None)):
    _require_admin(Authorization)
    delete_endtime_formula(telegram_id, formula_id)
    return {"ok": True}
