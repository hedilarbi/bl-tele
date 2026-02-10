import uuid
import threading
import requests
import builtins as _builtins
from typing import Optional, Tuple
from datetime import datetime

from .config import API_HOST, P1_POLL_TIMEOUT_S, P1_RESERVE_TIMEOUT_S, LOG_RAW_API_RESPONSES


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print


def _log_poll_response(label: str, status: int, body: str):
    return None


_thread_local = threading.local()


def _get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.session = sess
    return sess


def _has_header(headers: dict, name: str) -> bool:
    lname = name.lower()
    return any(k.lower() == lname for k in headers.keys())


def _merge_headers(token: str, base_headers: Optional[dict] = None) -> dict:
    if base_headers:
        headers = {k: v for k, v in base_headers.items() if v is not None}
        if not _has_header(headers, "Host"):
            headers["Host"] = API_HOST.replace("https://", "")
        if not _has_header(headers, "Accept"):
            headers["Accept"] = "*/*"
        if not _has_header(headers, "Accept-Language"):
            headers["Accept-Language"] = "en-CA,en-US;q=0.9,en;q=0.8"
        if not _has_header(headers, "Accept-Encoding"):
            headers["Accept-Encoding"] = "gzip, deflate, br"
        if not _has_header(headers, "Content-Type"):
            headers["Content-Type"] = "application/json"
        if not _has_header(headers, "X-Operating-System"):
            headers["X-Operating-System"] = "iOS"
        if not _has_header(headers, "User-Agent"):
            headers["User-Agent"] = "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0"
        if not _has_header(headers, "Connection"):
            headers["Connection"] = "keep-alive"
        if not _has_header(headers, "X-Request-ID"):
            headers["X-Request-ID"] = str(uuid.uuid4())
        if not _has_header(headers, "X-Correlation-ID"):
            headers["X-Correlation-ID"] = str(uuid.uuid4())
    else:
        headers = {
            "Host": API_HOST.replace("https://", ""),
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Request-ID": str(uuid.uuid4()),
            "X-Correlation-ID": str(uuid.uuid4()),
            "X-Operating-System": "iOS",
            "User-Agent": "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0",
            "Connection": "keep-alive",
        }
    headers["Authorization"] = token
    return headers


def get_rides_p1(token: str, headers: Optional[dict] = None) -> Tuple[Optional[int], Optional[list]]:
    headers = _merge_headers(token, headers)
    try:
        r = _get_session().get(f"{API_HOST}/rides", headers=headers, timeout=P1_POLL_TIMEOUT_S)
        raw_text = r.text
        _log_poll_response("P1 poll /rides", r.status_code, raw_text)
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
        err = f"{type(e).__name__}: {e}"
        # silence poll logs
        return None, {"error": err}


def get_offers_p1(token: str, headers: Optional[dict] = None):
    headers = _merge_headers(token, headers)
    try:
        r = _get_session().get(f"{API_HOST}/offers", headers=headers, timeout=P1_POLL_TIMEOUT_S)
        raw_text = r.text
        _log_poll_response("P1 poll /offers", r.status_code, raw_text)
        try:
            body = r.json()
        except Exception:
            body = r.text

        if r.status_code == 200 and isinstance(body, dict):
            results = body.get("results", []) or []
            if results and LOG_RAW_API_RESPONSES:
                _builtins.print(f"[{datetime.now()}] 🛰️ P1 poll /offers full response -> {raw_text}")
            for it in results:
                try:
                    it["_platform"] = "p1"
                except Exception:
                    pass
            return 200, results

        # return status + body for diagnostics (401/403/etc)
        return r.status_code, body
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        # silence poll logs
        return None, {"error": err}


def reserve_offer_p1(token: str, offer_id: str, price: Optional[float] = None, headers: Optional[dict] = None):
    """
    Accept (reserve) an offer on Platform 1.

    Returns: (status_code, json_or_text)
      200/201 → accepted
      401/403 → token invalid/expired
      409      → conflict / already taken
      422      → cannot accept (validation)
    """
    headers = _merge_headers(token, headers)
    payload = {
        "id": offer_id,
        "action": "accept",
    }
    if price is not None:
        try:
            payload["price"] = float(price)
        except Exception:
            payload["price"] = price
    try:
        r = _get_session().post(f"{API_HOST}/offers", headers=headers, json=payload, timeout=P1_RESERVE_TIMEOUT_S)
        try:
            body = r.json()
        except Exception:
            body = r.text
        return r.status_code, body
    except requests.exceptions.RequestException as e:
        return None, {"error": f"{type(e).__name__}: {e}"}
