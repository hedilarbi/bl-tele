import base64
import json
import time
from typing import Optional, Tuple
import requests

from .config import PORTAL_CLIENT_ID, PORTAL_AUTH_BASE, PARTNER_PORTAL_API, P1_API_BASE


def _athena_login(email: str, password: str) -> tuple[bool, Optional[str], str]:
    url = f"{PORTAL_AUTH_BASE}/oauth/token"
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
            try:
                j = r.json() or {}
            except Exception:
                return (False, None, "upstream:bad_json")
            tok = (j.get("result") or {}).get("access_token") or j.get("access_token")
            return (True, tok, "ok") if tok else (False, None, "upstream:no_token")
        if r.status_code in (401, 403):
            return (False, None, f"unauthorized:{r.status_code}")
        return (False, None, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, None, f"network:{type(e).__name__}")


def _portal_get_me(access_token: str) -> tuple[Optional[int], Optional[dict]]:
    url = f"{PARTNER_PORTAL_API}/me"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "BLPortal/uuid-fetch (+bot)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            return r.status_code, r.json()
        return r.status_code, None
    except requests.exceptions.RequestException:
        return None, None


def _p1_get_me_profile(token: str) -> tuple[Optional[int], Optional[dict]]:
    url = f"{P1_API_BASE}/api/v1/me/profile"
    headers = {
        "Authorization": token,  # 'Bearer <JWT>'
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Chauffeur/uuid-fetch (+bot)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            return r.status_code, r.json()
        return r.status_code, None
    except requests.exceptions.RequestException:
        return None, None


def _jwt_exp_unverified(token: str) -> Optional[int]:
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
        # If we can't parse, assume valid and let 401 drive re-login.
        return False
    return int(time.time()) >= (exp - 60)  # refresh ~1min early
