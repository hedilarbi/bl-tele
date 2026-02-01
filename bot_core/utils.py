import base64
import json
import re
from datetime import datetime
from typing import Optional
from dateutil.tz import gettz
import requests

from .config import API_HOST


def normalize_token(s: str) -> str:
    """
    Canonicalize to: 'Bearer <JWT>'.
    Accepts:
      - 'Bearer <JWT>'
      - 'authorization: Bearer <JWT>'
      - full HTTP request dumps (extracts Authorization header)
      - raw '<JWT>' (xxx.yyy.zzz)
      - quoted / multiline pastes
    """
    if not s:
        return ""
    raw = str(s).strip()

    # If a full HTTP request was pasted, extract the Authorization header line.
    auth_match = re.search(r"(?im)^\s*authorization\s*:\s*(.+)$", raw)
    s = auth_match.group(1).strip() if auth_match else raw

    # remove surrounding quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # collapse whitespace/newlines
    s = " ".join(s.replace("\r", "\n").split())

    # drop leading 'authorization:' if present
    if s.lower().startswith("authorization:"):
        s = s.split(":", 1)[1].strip()

    # already Bearer? keep but normalize capitalization/spacing
    if s.lower().startswith("bearer "):
        tok = s[7:].strip().replace(" ", "")
        return f"Bearer {tok}"

    # plain JWT pattern?
    if re.match(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", s):
        return f"Bearer {s}"

    # If the token was wrapped across lines in a HTTP dump, recover from raw text.
    compact = re.sub(r"\s+", "", raw)
    jwt_match = re.search(r"[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+", compact)
    if jwt_match:
        return f"Bearer {jwt_match.group(0)}"

    # fallback: return as-is (some exotic formats)
    return s


def parse_mobile_session_dump(raw: str) -> tuple[str, dict]:
    """
    Parse a full HTTP dump and return (token, headers).
    Authorization is returned separately; headers excludes Authorization.
    """
    token = normalize_token(raw)
    headers: dict = {}
    if not raw:
        return token, headers
    for line in str(raw).splitlines():
        line = line.strip()
        if not line:
            continue
        if re.match(r"^[A-Z]+\s+\S+\s+HTTP/[\d.]+$", line):
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip()
        v = v.strip()
        if not k or k.lower() == "authorization":
            continue
        headers[k] = v
    return token, headers


def mask_secret(s: str, keep: int = 4) -> str:
    if not s:
        return "—"
    s = str(s)
    if len(s) <= keep * 2:
        return s[:keep] + "…"
    return f"{s[:keep]}…{s[-keep:]}"


def _http_ok(status: int) -> bool:
    return 200 <= status < 300


def validate_mobile_session(token: str, headers: Optional[dict] = None) -> tuple[bool, str]:
    """
    Quick upstream probe. Token should already be normalized
    (i.e., 'Bearer <JWT>').
    """
    if not token:
        return (False, "empty_token")
    merged = {"Authorization": token, "Accept": "application/json"}
    if headers:
        merged = dict(headers)
        if not any(k.lower() == "accept" for k in merged):
            merged["Accept"] = "application/json"
        merged["Authorization"] = token
    try:
        r = requests.get(f"{API_HOST}/rides?limit=1", headers=merged, timeout=12)
        if _http_ok(r.status_code):
            return (True, "ok")
        if r.status_code in (401, 403):
            return (False, f"unauthorized:{r.status_code}")
        return (False, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, f"network:{type(e).__name__}")


def mask_email(email: str | None) -> str:
    if not email:
        return "—"
    try:
        local, domain = str(email).split("@", 1)
    except ValueError:
        return str(email)
    if len(local) <= 4:
        return f"{local}*****@{domain}"
    head = local[:4]
    tail = local[-4:] if len(local) > 8 else ""
    return f"{head}*****{tail}@{domain}"


def fmt_money(price, currency):
    if price is None:
        return "—"
    try:
        return f"{float(price):.2f} {currency or ''}".strip()
    except Exception:
        return f"{price} {currency or ''}".strip()


def fmt_km(meters):
    if meters is None:
        return "—"
    try:
        return f"{float(meters)/1000.0:.1f} km"
    except Exception:
        return str(meters)


def fmt_minutes(mins):
    if mins is None:
        return "—"
    try:
        return f"{float(mins):.0f} min"
    except Exception:
        return str(mins)


def fmt_dt_local(s, tz_name=None):
    if not s:
        return "—"
    try:
        iso = s.replace("Z", "+00:00")
        if "T" in iso or "+" in iso:
            dt = datetime.fromisoformat(iso)
        else:
            dt = datetime.strptime(iso, "%Y-%m-%d %H:%M:%S")
        tzinfo = gettz(tz_name) if tz_name else None
        if tzinfo:
            return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M")
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception:
        return s


def status_emoji(status):
    return "✅" if status == "accepted" else ("❌" if status == "rejected" else "ℹ️")


def safe(v, fallback="—"):
    return fallback if v in (None, "", []) else v


def _esc(s):
    if s is None:
        return "—"
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _norm_guest_requests(val):
    """
    Accepts:
      - JSON string like '["A","B"]'
      - list[str]/list[dict]
      - plain string
    Returns a displayable comma-separated string or None.
    """
    if not val:
        return None
    try:
        if isinstance(val, str):
            # try json decode first
            parsed = json.loads(val)
            val = parsed
    except Exception:
        # keep as plain string
        return str(val)

    if isinstance(val, list):
        out = []
        for it in val:
            if isinstance(it, str):
                out.append(it)
            elif isinstance(it, dict):
                for k in ("label", "name", "value", "text"):
                    if it.get(k):
                        out.append(str(it[k]))
                        break
        return ", ".join(out) if out else None

    return str(val)


def validate_datetime(text: str):
    try:
        return datetime.strptime(text, "%d/%m/%Y %H:%M")
    except ValueError:
        return None


def validate_day(text: str):
    try:
        return datetime.strptime(text, "%d/%m/%Y")
    except ValueError:
        return None
