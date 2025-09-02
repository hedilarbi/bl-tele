# webapp_api.py
import os, hmac, hashlib, json, urllib.parse, time, sqlite3, logging
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi import Query
import requests
import requests
import uuid
from fastapi import Header, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from typing import Optional
from db import DB_FILE, get_booked_slots, add_booked_slot, list_user_custom_filters, toggle_user_custom_filter
from db import get_blocked_days, add_blocked_day, delete_blocked_day 
from db import (
   
    get_all_users,
    list_all_custom_filters, create_custom_filter, update_custom_filter,
     assign_custom_filter, unassign_custom_filter
)
import sqlite3, uuid
from fastapi import Query, Header, HTTPException


# ---------- env ----------
# Load .env from CWD or parents (like your bot.py does)
load_dotenv()

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Accept both EU, US and HTML5 datetime-local (with/without seconds)
ACCEPTED_FORMATS = (
    "%d/%m/%Y %H:%M",      # 29/08/2025 20:00
    "%m/%d/%Y %H:%M",      # 08/29/2025 20:00
    "%Y-%m-%d %H:%M",      # 2025-08-29 20:00
    "%Y-%m-%d %H:%M:%S",   # 2025-08-29 20:00:00
    "%Y-%m-%dT%H:%M",      # 2025-08-29T20:00 (HTML5 datetime-local)
    "%Y-%m-%dT%H:%M:%S",   # 2025-08-29T20:00:00
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
    # Normalize to your DB display format
    return dt.strftime("%d/%m/%Y %H:%M")


BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")

# Dev shortcuts (optional)
DEV_SKIP_TMA = os.getenv("DEV_SKIP_TMA", "0") == "1"
DEV_FAKE_USER_ID = int(os.getenv("DEV_FAKE_USER_ID", "0"))

# ---------- logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

# ---------- DB hooks ----------

try:
    from db import delete_booked_slot
except Exception:
    delete_booked_slot = None

# ---------- app ----------
app = FastAPI(title="MiniApp API (verbose)")

API_HOST = "https://chauffeur-app-api.blacklane.com"


def _require_admin(Authorization: str | None):
    if not Authorization or not Authorization.startswith("admin "):
        raise HTTPException(401, "Use Authorization: admin <token>")
    token = Authorization[6:].strip()
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid admin token")
    
from db import init_db  # add this import at top

@app.on_event("startup")
async def _startup():
    init_db()  # <-- run schema/migrations here
    logging.info("🚀 FastAPI starting")
    logging.info("🔐 DEV_SKIP_TMA=%s DEV_FAKE_USER_ID=%s", DEV_SKIP_TMA, DEV_FAKE_USER_ID or "—")
    logging.info("🗄️  DB_FILE=%s", DB_FILE)
    if ALLOWED_ORIGINS:
        logging.info("🌐 CORS allow_origins=%s", ", ".join(ALLOWED_ORIGINS))
    else:
        logging.info("🌐 CORS allow_origins=* (dev)")

    
@app.get("/admin/users")
def admin_list_users(Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT telegram_id, active FROM users ORDER BY telegram_id ASC")
    rows = cur.fetchall(); conn.close()
    users = [{"id": r[0], "telegram_id": r[0], "active": bool(r[1])} for r in rows]
    return {"users": users}

def _get_user_token(uid: int) -> str | None:
    # Your existing way to fetch the user’s mobile session token from DB.
    # If you already have a helper in db.py, use that instead.
    import sqlite3
    from db import DB_FILE
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT token FROM users WHERE telegram_id = ?", (uid,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def _parse_day_ddmmyyyy(s: str) -> datetime | None:
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y")
    except Exception:
        return None

def _fmt_day_ddmmyyyy(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y")

ALLOWED_ORIGINS = [
    "http://localhost:3000",
    os.environ.get("NEXT_PUBLIC_SITE_URL", ""),         # e.g. https://your-vercel-app.vercel.app
    os.environ.get("NEXT_PUBLIC_MINI_APP_ORIGIN", ""),  # optional extra
]
ALLOWED_ORIGINS = [o for o in ALLOWED_ORIGINS if o]

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=".*",   # allow ANY origin for now
    allow_credentials=False,   # leave False so wildcard works cleanly
    allow_methods=["*"],
    allow_headers=["*"],
)


def _require_user_from_any(auth_header: str | None, tma_qs: str | None) -> int:
    # dev bypass still works
    if DEV_SKIP_TMA and not (auth_header or tma_qs):
        if not DEV_FAKE_USER_ID:
            raise HTTPException(401, "DEV mode missing DEV_FAKE_USER_ID")
        logging.warning("⚠️ DEV mode, using DEV_FAKE_USER_ID=%s", DEV_FAKE_USER_ID)
        return DEV_FAKE_USER_ID

    if auth_header and auth_header.startswith("tma "):
        user = _validate_init_data(auth_header[4:])
        uid = int(user["id"])
        logging.info("🔑 Auth OK (header) uid=%s", uid)
        return uid

    if tma_qs:
        user = _validate_init_data(tma_qs)
        uid = int(user["id"])
        logging.info("🔑 Auth OK (query) uid=%s", uid)
        return uid

    raise HTTPException(401, "Provide Authorization: tma <initData> or ?tma=<initData>")

@app.on_event("startup")
async def _startup():
    logging.info("🚀 FastAPI starting")
    logging.info("🔐 DEV_SKIP_TMA=%s DEV_FAKE_USER_ID=%s", DEV_SKIP_TMA, DEV_FAKE_USER_ID or "—")
    logging.info("🗄️  DB_FILE=%s", DB_FILE)
    if ALLOWED_ORIGINS:
        logging.info("🌐 CORS allow_origins=%s", ", ".join(ALLOWED_ORIGINS))
    else:
        logging.info("🌐 CORS allow_origins=* (dev)")

# Per-request logger
@app.middleware("http")
async def _log_requests(request: Request, call_next):
    start = time.time()
    client = request.client.host if request.client else "?"
    path   = request.url.path
    method = request.method
    ua     = request.headers.get("user-agent", "?")
    xff    = request.headers.get("x-forwarded-for", "")
    has_auth = "Authorization" in request.headers
    logging.info("➡️  %s %s from %s | UA=%s | XFF=%s | auth=%s",
                 method, path, client, ua[:60], xff, has_auth)
    try:
        response = await call_next(request)
        took = int((time.time() - start) * 1000)
        logging.info("⬅️  %s %s -> %s in %dms", method, path, response.status_code, took)
        return response
    except Exception as e:
        took = int((time.time() - start) * 1000)
        logging.exception("💥 %s %s failed after %dms: %s", method, path, took, e)
        raise

# ---------- auth helpers ----------
def _validate_init_data(init_data_raw: str, max_age_sec: int = 600) -> dict:
    """Verify Telegram WebApp initData (HMAC)."""
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

def _require_user(auth_header: str | None) -> int:
    """Read Authorization: tma <initData> and return Telegram user id."""
    if DEV_SKIP_TMA and not auth_header:
        if not DEV_FAKE_USER_ID:
            logging.error("DEV_SKIP_TMA=1 but DEV_FAKE_USER_ID missing")
            raise HTTPException(401, "DEV_SKIP_TMA=1 but DEV_FAKE_USER_ID missing")
        logging.warning("⚠️  Using DEV_FAKE_USER_ID=%s (no Authorization header)", DEV_FAKE_USER_ID)
        return DEV_FAKE_USER_ID

    if not auth_header or not auth_header.startswith("tma "):
        logging.warning("Authorization header missing or malformed")
        raise HTTPException(401, "Use header: Authorization: tma <initData>")

    init_data_raw = auth_header[4:]
    user = _validate_init_data(init_data_raw)
    uid = int(user["id"])
    logging.info("🔑 Auth OK for user_id=%s", uid)
    return uid

# ---------- models ----------
class CreateSlotIn(BaseModel):
    start: str  # dd/mm/yyyy HH:MM
    end: str    # dd/mm/yyyy HH:MM
    name: str | None = None

def _valid_dt(s: str) -> bool:
    try:
        datetime.strptime(s, "%d/%m/%Y %H:%M"); return True
    except Exception:
        return False

# ---------- endpoints ----------
@app.get("/debug/ping")
def ping():
    logging.info("🏓 /debug/ping")
    return {"ok": True, "ts": int(time.time())}

@app.get("/debug/whoami")
def whoami(Authorization: str | None = Header(default=None)):
    try:
        uid = _require_user(Authorization)
        return {"ok": True, "user_id": uid, "dev_mode": DEV_SKIP_TMA}
    except HTTPException as e:
        return {"ok": False, "error": e.detail}
    
@app.get("/webapp/slots")
def list_slots(
    Authorization: str | None = Header(default=None),
    tma: str | None = Query(default=None)
):
    uid = _require_user_from_any(Authorization, tma)
    rows = get_booked_slots(uid)
    logging.info("📦 list_slots user=%s -> %d rows", uid, len(rows or []))
    return {"slots": rows}

@app.post("/webapp/slots")
def create_slot(payload: CreateSlotIn, Authorization: str | None = Header(default=None)):
    uid = _require_user(Authorization)
    logging.info("➕ create_slot user=%s start=%s end=%s name=%s", uid, payload.start, payload.end, payload.name)

    dt_start = _parse_dt(payload.start)
    dt_end   = _parse_dt(payload.end)
    if not (dt_start and dt_end):
        logging.warning("create_slot invalid format")
        # Always JSON so the client doesn't try to parse HTML
        raise HTTPException(
            status_code=400,
            detail={
                "error": "bad_date_format",
                "received": {"start": payload.start, "end": payload.end},
                "accepted": list(ACCEPTED_FORMATS),
                "hint": "Use HTML5 datetime-local or dd/mm/yyyy HH:MM",
            },
        )

    start_str = _fmt_ddmmyyyy(dt_start)
    end_str   = _fmt_ddmmyyyy(dt_end)

    add_booked_slot(uid, start_str, end_str, payload.name or None)
    logging.info("✅ slot created for user=%s (%s → %s)", uid, start_str, end_str)
    return {"ok": True, "saved": {"from": start_str, "to": end_str}}




@app.delete("/webapp/slots/{slot_id}")
def delete_slot(slot_id: int, Authorization: str | None = Header(default=None)):
    uid = _require_user(Authorization)
    logging.info("🗑️  delete_slot user=%s id=%s", uid, slot_id)
    # try custom deleter
    if delete_booked_slot:
        try:
            delete_booked_slot(slot_id)
            logging.info("✅ slot deleted via custom deleter")
            return {"ok": True}
        except Exception as e:
            logging.warning("custom deleter failed: %s", e)

    # fallback raw SQL (ensure ownership where possible)
    try:
        conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
        cur.execute("DELETE FROM booked_slots WHERE id = ? AND telegram_id = ?", (slot_id, uid))
        if cur.rowcount == 0:
            cur.execute("DELETE FROM booked_slots WHERE id = ?", (slot_id,))
        conn.commit(); conn.close()
        logging.info("✅ slot deleted via fallback SQL (rowcount may vary)")
        return {"ok": True}
    except Exception as e:
        logging.exception("delete_slot SQL error: %s", e)
        raise HTTPException(500, "Could not delete")
@app.get("/webapp/days")
def list_blocked_days(
    Authorization: str | None = Header(default=None),
    tma: str | None = Query(default=None),
):
    logging.info("📅 list_blocked_days tma=%s", tma)
    uid = _require_user_from_any(Authorization, tma)
    rows = get_blocked_days(uid)  # [{"id":..., "day":"dd/mm/YYYY"}, ...]
    days = [r["day"] for r in rows]
    logging.info("📅 list_blocked_days uid=%s -> %d", uid, len(days))
    return {"days": days}

class ToggleDayIn(BaseModel):
    day: str  # "dd/mm/YYYY"

@app.post("/webapp/days/toggle")
def toggle_blocked_day(payload: ToggleDayIn, Authorization: str | None = Header(default=None)):
    uid = _require_user(Authorization)
    day_raw = (payload.day or "").strip()
    dt = _parse_day_ddmmyyyy(day_raw)
    if not dt:
        raise HTTPException(400, detail={"error": "bad_day_format", "expected": "dd/mm/YYYY", "got": day_raw})
    day = _fmt_day_ddmmyyyy(dt)
    existing = {d["day"] for d in get_blocked_days(uid)}
    if day in existing:
        # unblock
        # find ID (cheap loop)
        for d in get_blocked_days(uid):
            if d["day"] == day:
                delete_blocked_day(d["id"])
                logging.info("🟢 unblocked %s for uid=%s", day, uid)
                return {"ok": True, "blocked": False, "day": day}
        raise HTTPException(404, "Not found")
    else:
        add_blocked_day(uid, day)
        logging.info("🛑 blocked %s for uid=%s", day, uid)
        return {"ok": True, "blocked": True, "day": day}
    
@app.get("/webapp/rides")
def list_user_rides(
    Authorization: str | None = Header(default=None),
    page: int = Query(0),
    limit: int = Query(30),
    status: str | None = Query(default=None),
):
    uid = _require_user(Authorization)
    token = _get_user_token(uid)
    if not token:
        raise HTTPException(401, detail={"error": "no_token", "hint": "Add your mobile session token."})

    headers = {
        "Host": API_HOST.replace("https://", ""),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,               # SAME as poller
        "X-Request-ID": str(uuid.uuid4()),
        "X-Correlation-ID": str(uuid.uuid4()),
    }
    

    try:
        r = requests.get(f"{API_HOST}/rides", headers=headers, timeout=15)
        # Return upstream status + body as JSON
        data = r.json()
        return JSONResponse(status_code=r.status_code, content=data)
    except requests.exceptions.RequestException as e:
        raise HTTPException(502, detail="upstream_error")
@app.get("/admin/users")
def admin_users(Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    rows = get_all_users()
    return {"users": [{"telegram_id": r[0], "active": bool(r[3])} for r in rows]}


@app.get("/admin/custom-filters")
def admin_list_filters(Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    return {"filters": list_all_custom_filters()}

class AdminCreateCF(BaseModel):
    slug: str
    name: str
    description: str | None = ""
    params: dict | None = None
    global_enabled: bool = True
    rule_kind: str | None = "generic"
    rule_code: str | None = None

@app.post("/admin/custom-filters")
def admin_create_filter(payload: AdminCreateCF, Authorization: str | None = Header(default=None)):
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

class AdminPatchCF(BaseModel):
    name: str | None = None
    description: str | None = None
    params: dict | None = None
    global_enabled: bool | None = None
    rule_kind: str | None = None
    rule_code: str | None = None



@app.patch("/admin/custom-filters/{slug}")
def admin_update_filter(slug: str, payload: AdminPatchCF, Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    update_custom_filter(slug, **{k:v for k,v in payload.dict().items() if v is not None})
    return {"ok": True}

# --- Admin: assign/unassign/toggle for a user ---
@app.get("/admin/users/{telegram_id}/custom-filters")
def admin_user_filters(telegram_id: int, Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    assigned = list_user_custom_filters(telegram_id)
    return {"assigned": assigned, "all": list_all_custom_filters()}

@app.post("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_assign_cf(telegram_id: int, slug: str, Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    assign_custom_filter(telegram_id, slug, True)
    return {"ok": True}

@app.delete("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_unassign_cf(telegram_id: int, slug: str, Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    unassign_custom_filter(telegram_id, slug)
    return {"ok": True}

class AdminToggleUserCF(BaseModel):
    enabled: bool

@app.patch("/admin/users/{telegram_id}/custom-filters/{slug}")
def admin_toggle_user_cf(telegram_id: int, slug: str, payload: AdminToggleUserCF, Authorization: str | None = Header(default=None)):
    _require_admin(Authorization)
    toggle_user_custom_filter(telegram_id, slug, payload.enabled)
    return {"ok": True}
# ---- WebApp: list/toggle my custom filters ----
class ToggleMyCF(BaseModel):
    enabled: bool

@app.get("/webapp/custom-filters")
def webapp_list_my_filters(
    Authorization: str | None = Header(default=None),
    tma: str | None = Query(default=None),
):
    uid = _require_user_from_any(Authorization, tma)
    rows = list_user_custom_filters(uid)  # [{slug, name, description, global_enabled, user_enabled, params}]
    # Only show assigned filters; expose a simple shape the UI expects
    items = []
    for r in rows:
        effective_enabled = bool(r.get("user_enabled")) and bool(r.get("global_enabled", 1))
        items.append({
            "slug": r["slug"],
            "name": r["name"],
            "enabled": effective_enabled,  # UI reads it.enabled
        })
    return {"filters": items}

@app.post("/webapp/custom-filters/{slug}/toggle")
def webapp_toggle_my_filter(
    slug: str,
    payload: ToggleMyCF,
    Authorization: str | None = Header(default=None),
):
    uid = _require_user(Authorization)
    # assign (insert-or-update) and set enabled state
    toggle_user_custom_filter(uid, slug, bool(payload.enabled))
    return {"ok": True}




