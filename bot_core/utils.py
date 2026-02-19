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
    token = ""
    headers: dict = {}
    if not raw:
        return token, headers
    auth_match = re.search(r"(?im)^\s*authorization\s*:\s*(.+)$", str(raw))
    if auth_match:
        token = normalize_token(auth_match.group(1).strip())
    for line in str(raw).splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(("{", "}", "[", "]", '"', "'")):
            continue
        if re.match(r"^[A-Z]+\s+\S+\s+HTTP/[\d.]+$", line):
            continue
        if not re.match(r"^[A-Za-z0-9_-]+\s*:\s*.+$", line):
            continue
        k, v = line.split(":", 1)
        k = k.strip()
        v = v.strip()
        if not k or k.lower() == "authorization":
            continue
        headers[k] = v
    return token, headers


def parse_mobile_auth_material(raw: str) -> dict:
    """
    Extract auth material from flexible input shapes.

    Returns optional keys:
      - token          (normalized Bearer JWT)
      - refresh_token
      - client_id
    """
    out: dict = {}
    if not raw:
        return out

    s = str(raw).strip()

    # 1) JSON payload/response
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            root = parsed.get("result") if isinstance(parsed.get("result"), dict) else parsed
            access = root.get("access_token") or parsed.get("access_token")
            refresh = root.get("refresh_token") or parsed.get("refresh_token")
            client_id = root.get("client_id") or parsed.get("client_id")
            if isinstance(access, str) and access.strip():
                out["token"] = normalize_token(access.strip())
            if isinstance(refresh, str) and refresh.strip():
                out["refresh_token"] = refresh.strip()
            if isinstance(client_id, str) and client_id.strip():
                out["client_id"] = client_id.strip()
    except Exception:
        pass

    # 2) key/value fallback
    access_kv = _extract_auth_value(s, "access_token")
    refresh_kv = _extract_auth_value(s, "refresh_token")
    client_kv = _extract_auth_value(s, "client_id")
    auth_kv = _extract_auth_value(s, "authorization")
    if auth_kv and "token" not in out:
        out["token"] = normalize_token(auth_kv)
    if access_kv and "token" not in out:
        out["token"] = normalize_token(access_kv)
    if refresh_kv and "refresh_token" not in out:
        out["refresh_token"] = refresh_kv
    if client_kv and "client_id" not in out:
        out["client_id"] = client_kv

    # 3) bare value fallback (single-line paste)
    if "\n" not in s and "\r" not in s:
        bare = s.strip().strip('"').strip("'")
        if bare:
            if bare.lower().startswith("bearer ") and "token" not in out:
                out["token"] = normalize_token(bare)
            elif re.match(r"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", bare) and "token" not in out:
                out["token"] = normalize_token(bare)
            elif bare.startswith("v1.") and "refresh_token" not in out:
                out["refresh_token"] = bare
            elif re.match(r"^[A-Za-z0-9_-]{12,128}$", bare) and ("client_id" not in out) and ("refresh_token" not in out):
                out["client_id"] = bare

    return out


def _extract_auth_value(raw: str, key: str) -> Optional[str]:
    if not raw:
        return None
    patterns = [
        rf'"{re.escape(key)}"\s*:\s*"([^"]+)"',
        rf"{re.escape(key)}\s*[:=]\s*['\"]?([^\s\"',&}}]+)",
    ]
    for pat in patterns:
        m = re.search(pat, str(raw), flags=re.IGNORECASE)
        if m and m.group(1):
            return m.group(1).strip()
    return None


def parse_mobile_auth_meta(raw: str, headers: Optional[dict] = None) -> dict:
    """
    Best-effort extraction of OAuth refresh material from a pasted dump.

    Returned dict may include:
      - refresh_token
      - client_id
      - oauth_headers (subset useful for /oauth/token refresh call)
    """
    out: dict = {}
    if not raw and not headers:
        return out

    refresh_token = _extract_auth_value(raw or "", "refresh_token")
    client_id = _extract_auth_value(raw or "", "client_id")
    if refresh_token:
        out["refresh_token"] = refresh_token
    if client_id:
        out["client_id"] = client_id

    src_headers = headers if isinstance(headers, dict) else {}
    if not src_headers and raw:
        _, parsed_headers = parse_mobile_session_dump(raw)
        src_headers = parsed_headers or {}

    if src_headers:
        wanted = {
            "auth0-client",
            "user-agent",
            "accept",
            "accept-language",
            "accept-encoding",
            "connection",
            "cookie",
            "content-type",
            "host",
        }
        oauth_headers = {
            k: v
            for k, v in src_headers.items()
            if k and v is not None and k.lower() in wanted
        }
        if oauth_headers:
            out["oauth_headers"] = oauth_headers

    return out


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
