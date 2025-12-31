import json
from typing import Optional, Tuple, List
from datetime import datetime
from dateutil import parser
from dateutil.tz import gettz

from .config import CF_DEBUG
from .utils import _parse_hhmm, _to_str, _esc
from db import list_user_custom_filters


def _get_enabled_filter_slugs(bot_id: str, telegram_id: int):
    items = list_user_custom_filters(bot_id, telegram_id)
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


def _filter_reject_under_90_between_20_22(
    offer: dict,
    tz_name: str,
    min_price: float = 90.0,
    win_from="20:00",
    win_to="22:00",
) -> Tuple[Optional[str], Optional[str]]:
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
            offer,
            tz_name,
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


def _find_conflict(
    new_start: datetime,
    new_end_iso: Optional[str],
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]],
) -> Optional[Tuple[datetime, datetime]]:
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
