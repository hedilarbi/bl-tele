import uuid
import requests
from typing import Optional, Tuple
from datetime import datetime

from .config import API_HOST

def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print


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
