import base64
import hashlib
import json
import re
import threading
import time
import builtins as _builtins
from datetime import datetime
from typing import Optional, Tuple

import requests

from .config import MOBILE_AUTH_BASE, MOBILE_CLIENT_ID, P1_POLL_TIMEOUT_S, P1_REFRESH_SKEW_S
from db import get_mobile_auth, update_token, set_token_status

_thread_local = threading.local()


def _get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.session = sess
    return sess


def _session_request(method: str, url: str, **kwargs):
    sess = _get_session()
    # Keep connection pooling but avoid cross-user cookie bleed on shared worker threads.
    try:
        sess.cookies.clear()
    except Exception:
        pass
    return sess.request(method=method, url=url, **kwargs)


def _normalize_bearer(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    val = str(token).strip()
    if not val:
        return None
    if val.lower().startswith("bearer "):
        return "Bearer " + val[7:].strip()
    return val


def _header_get(headers: Optional[dict], name: str) -> Optional[str]:
    if not headers:
        return None
    lname = name.lower()
    for k, v in headers.items():
        if str(k).lower() == lname:
            return v
    return None


def _header_drop(headers: dict, name: str):
    lname = name.lower()
    for k in list(headers.keys()):
        if str(k).lower() == lname:
            headers.pop(k, None)


def _jwt_exp_unverified(token: str) -> Optional[int]:
    try:
        raw = token[7:].strip() if str(token).lower().startswith("bearer ") else token
        parts = (raw or "").split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "==="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
        exp = payload.get("exp")
        return int(exp) if isinstance(exp, (int, float)) else None
    except Exception:
        return None


def _needs_refresh(token: Optional[str]) -> bool:
    if not token:
        return True
    exp = _jwt_exp_unverified(token)
    if exp is None:
        return False
    now = int(time.time())
    return now >= (exp - P1_REFRESH_SKEW_S)


def _build_oauth_headers(mobile_headers: Optional[dict], oauth_headers: Optional[dict]) -> dict:
    allowed = {
        "accept",
        "accept-language",
        "accept-encoding",
        "connection",
        "user-agent",
        "auth0-client",
        "cookie",
        "content-type",
        "host",
    }
    headers: dict = {}
    if mobile_headers:
        for k, v in mobile_headers.items():
            if v is not None and str(k).lower() in allowed:
                headers[k] = v
    if oauth_headers:
        for k, v in oauth_headers.items():
            if v is not None and str(k).lower() in allowed:
                headers[k] = v

    _header_drop(headers, "Authorization")
    _header_drop(headers, "Content-Length")

    auth_host = MOBILE_AUTH_BASE.replace("https://", "").replace("http://", "")
    current_host = _header_get(headers, "Host")
    if current_host and current_host.strip() != auth_host:
        _header_drop(headers, "Host")

    if not _header_get(headers, "Accept"):
        headers["Accept"] = "*/*"
    if not _header_get(headers, "Content-Type"):
        headers["Content-Type"] = "application/json"
    if not _header_get(headers, "Accept-Language"):
        headers["Accept-Language"] = "en-CA,en-US;q=0.9,en;q=0.8"
    if not _header_get(headers, "Accept-Encoding"):
        headers["Accept-Encoding"] = "gzip, deflate, br"
    if not _header_get(headers, "Connection"):
        headers["Connection"] = "keep-alive"
    if not _header_get(headers, "User-Agent"):
        headers["User-Agent"] = "Chauffeur/20104 CFNetwork/3860.300.31 Darwin/25.2.0"
    return headers


def _redact_sensitive_text(raw: str, max_len: int = 800) -> str:
    text = str(raw or "")
    # JSON-like token fields
    text = re.sub(
        r'("?(?:access_token|refresh_token|id_token)"?\s*:\s*")([^"]+)(")',
        r"\1***REDACTED***\3",
        text,
        flags=re.IGNORECASE,
    )
    # Bearer values
    text = re.sub(
        r"(Bearer\s+)[A-Za-z0-9\-\._~\+/=]+",
        r"\1***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )
    # Cookie header/value
    text = re.sub(
        r'("?(?:cookie|set-cookie)"?\s*:\s*")([^"]+)(")',
        r"\1***REDACTED***\3",
        text,
        flags=re.IGNORECASE,
    )
    if len(text) > max_len:
        return text[:max_len] + "...(truncated)"
    return text


def _mask_value(val: Optional[str], keep: int = 4) -> str:
    s = str(val or "").strip()
    if not s:
        return "—"
    if len(s) <= keep * 2:
        return s[:keep] + "..." if len(s) > keep else s
    return f"{s[:keep]}...{s[-keep:]}"


def _fp8(val: Optional[str]) -> str:
    s = str(val or "").strip()
    if not s:
        return "—"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]


def refresh_p1_access_token(
    refresh_token: str,
    client_id: str,
    oauth_headers: Optional[dict] = None,
) -> Tuple[bool, Optional[str], Optional[str], str]:
    if not refresh_token:
        return False, None, None, "missing_refresh_token"
    if not client_id:
        return False, None, None, "missing_client_id"

    url = f"{MOBILE_AUTH_BASE}/oauth/token"
    payload = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "grant_type": "refresh_token",
    }
    headers = _build_oauth_headers(None, oauth_headers)
    timeout_s = max(5, int(P1_POLL_TIMEOUT_S))
    _builtins.print(
        f"[{datetime.now()}] 🔐 P1 refresh call -> POST {url} "
        f"(client_id={_mask_value(client_id, keep=5)} fp={_fp8(client_id)}, "
        f"refresh_token={_mask_value(refresh_token, keep=6)} len={len(str(refresh_token or ''))} fp={_fp8(refresh_token)})"
    )
    try:
        r = _session_request("POST", url, headers=headers, json=payload, timeout=timeout_s)
        _builtins.print(
            f"[{datetime.now()}] 🧾 P1 refresh response status={r.status_code} "
            f"body={_redact_sensitive_text(r.text)}"
        )
        if 200 <= r.status_code < 300:
            try:
                j = r.json() or {}
            except Exception:
                return False, None, None, "upstream:bad_json"
            root = j.get("result") if isinstance(j.get("result"), dict) else j
            access = root.get("access_token")
            new_refresh = root.get("refresh_token") or refresh_token
            if access:
                return True, _normalize_bearer(access), str(new_refresh), "ok"
            return False, None, None, "upstream:no_access_token"
        if r.status_code in (400, 401, 403):
            return False, None, None, f"unauthorized:{r.status_code}"
        return False, None, None, f"upstream:{r.status_code}"
    except requests.exceptions.RequestException as e:
        _builtins.print(f"[{datetime.now()}] ❌ P1 refresh network error: {type(e).__name__}: {e}")
        return False, None, None, f"network:{type(e).__name__}"


def maybe_refresh_p1_session(
    bot_id: str,
    telegram_id: int,
    token: Optional[str],
    mobile_headers: Optional[dict],
    force: bool = False,
    trigger: str = "unspecified",
) -> Tuple[Optional[str], Optional[dict], bool, str]:
    current_token = _normalize_bearer(token)
    if not force and current_token and not _needs_refresh(current_token):
        return current_token, mobile_headers, False, "not_expiring"

    auth_meta = get_mobile_auth(bot_id, telegram_id) or {}
    refresh_token = auth_meta.get("refresh_token")
    client_id = auth_meta.get("client_id") or MOBILE_CLIENT_ID
    oauth_headers = auth_meta.get("oauth_headers") if isinstance(auth_meta.get("oauth_headers"), dict) else None

    if not refresh_token or not client_id:
        if force:
            _builtins.print(
                f"[{datetime.now()}] ⚠️ P1 refresh skipped for {bot_id}/{telegram_id} "
                f"trigger={trigger} reason=missing_refresh_material "
                f"(refresh_token={'yes' if refresh_token else 'no'}, client_id={'yes' if client_id else 'no'})"
            )
        return current_token, mobile_headers, False, "missing_refresh_material"

    _builtins.print(
        f"[{datetime.now()}] 🔄 P1 refresh attempt for {bot_id}/{telegram_id} "
        f"trigger={trigger} force={force} token_present={'yes' if current_token else 'no'} "
        f"client_id={_mask_value(client_id, keep=5)} fp={_fp8(client_id)} "
        f"refresh_fp={_fp8(refresh_token)}"
    )
    req_headers = _build_oauth_headers(mobile_headers, oauth_headers)
    cookie_raw = _header_get(req_headers, "Cookie") or _header_get(req_headers, "cookie") or ""
    _builtins.print(
        f"[{datetime.now()}] 🍪 P1 refresh headers for {bot_id}/{telegram_id} "
        f"cookie_present={'yes' if bool(cookie_raw) else 'no'} "
        f"cookie_len={len(str(cookie_raw)) if cookie_raw else 0} "
        f"cookie_fp={_fp8(cookie_raw)} "
        f"auth0_client_present={'yes' if bool(_header_get(req_headers, 'Auth0-Client')) else 'no'} "
        f"user_agent_present={'yes' if bool(_header_get(req_headers, 'User-Agent')) else 'no'}"
    )
    ok, new_token, new_refresh, note = refresh_p1_access_token(
        refresh_token=str(refresh_token),
        client_id=str(client_id),
        oauth_headers=req_headers,
    )
    if not ok or not new_token:
        _builtins.print(
            f"[{datetime.now()}] ⚠️ P1 refresh failed for {bot_id}/{telegram_id} "
            f"trigger={trigger} note={note}"
        )
        return current_token, mobile_headers, False, note

    next_headers = dict(mobile_headers or {})
    _header_drop(next_headers, "Authorization")

    next_auth_meta = dict(auth_meta)
    next_auth_meta["client_id"] = str(client_id)
    next_auth_meta["refresh_token"] = str(new_refresh or refresh_token)
    if oauth_headers:
        next_auth_meta["oauth_headers"] = oauth_headers

    headers_to_save = next_headers if mobile_headers is not None else None
    update_token(
        bot_id,
        telegram_id,
        new_token,
        headers=headers_to_save,
        auth_meta=next_auth_meta,
    )
    set_token_status(bot_id, telegram_id, "valid")
    _builtins.print(
        f"[{datetime.now()}] ✅ P1 refresh success for {bot_id}/{telegram_id} "
        f"trigger={trigger} refresh_rotated={'yes' if new_refresh and str(new_refresh) != str(refresh_token) else 'no'}"
    )

    runtime_headers = next_headers if next_headers else None
    return new_token, runtime_headers, True, "ok"
