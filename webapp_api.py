# webapp_api.py
import os, hmac, hashlib, json, urllib.parse, time, sqlite3, logging
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi import Query
# ---------- env ----------
# Load .env from CWD or parents (like your bot.py does)
load_dotenv()

from datetime import datetime
from typing import Optional

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
from db import DB_FILE, get_booked_slots, add_booked_slot
from db import get_blocked_days, add_blocked_day, delete_blocked_day
try:
    from db import delete_booked_slot
except Exception:
    delete_booked_slot = None

# ---------- app ----------
app = FastAPI(title="MiniApp API (verbose)")

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
from fastapi import Query, Header, HTTPException

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