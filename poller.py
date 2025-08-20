# poller.py
import time
import json
import uuid
import re
import sqlite3
import requests
from typing import Optional, Iterable, List, Tuple
from datetime import datetime, timezone, timedelta
from dateutil import parser
from dateutil.tz import gettz
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from db import (
    DB_FILE,
    get_all_users,
    get_booked_slots,
    get_vehicle_classes_state,
    log_offer_decision,
    get_processed_offer_ids,
    get_user_timezone,
    get_pinned_warnings,
    save_pinned_warning,
    clear_pinned_warning,
    set_token_status,
)

BOT_TOKEN     = "8132945480:AAF3iXB6JzZp_cFclqA5LHvniUW5AlXdnpU"
API_HOST      = "https://chauffeur-app-api.blacklane.com"
POLL_INTERVAL = 2
MAX_WORKERS   = 10

# Toggle mock data for development
USE_MOCK = False  # set True to use mock offers instead of live /offers

accepted_per_user = defaultdict(set)
rejected_per_user = defaultdict(set)

# ------------ small helpers ------------
def _esc(s: Optional[str]) -> str:
    """Escape for Telegram HTML parse_mode."""
    if s is None:
        return "—"
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def _fmt_money(price, currency) -> str:
    if price is None:
        return "—"
    try:
        return f"{float(price):.2f} {currency or ''}".strip()
    except Exception:
        return f"{price} {currency or ''}".strip()

def _fmt_km(meters) -> str:
    if meters is None:
        return "—"
    try:
        return f"{float(meters)/1000.0:.3f} km"
    except Exception:
        return str(meters)

def _fmt_minutes(mins) -> str:
    if mins is None:
        return "—"
    try:
        return f"{float(mins):.0f} min"
    except Exception:
        return str(mins)

def _split_chunks(text: str, limit: int = 4096) -> Iterable[str]:
    """Split text into Telegram-safe chunks."""
    t = text
    while len(t) > limit:
        cut = t.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        yield t[:cut]
        t = t[cut:]
    if t:
        yield t

def _strip_html_tags(text: str) -> str:
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.I)
    return re.sub(r"</?[^>]+>", "", text)

def _fmt_dt_local(s: str, tz_name: Optional[str]) -> str:
    if not s:
        return "—"
    try:
        dt = parser.isoparse(s)
        tzinfo = gettz(tz_name) if tz_name else None
        if tzinfo:
            return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
        return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            tzinfo = gettz(tz_name) if tz_name else None
            if tzinfo:
                return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
            return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            return s

def _fmt_dt_local_from_dt(dt: datetime, tz_name: Optional[str]) -> str:
    tzinfo = gettz(tz_name) if tz_name else None
    if tzinfo:
        return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
    return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")

# ------------- Telegram helpers -------------
def _send_one(
    chat_id: int,
    text: str,
    reply_markup: Optional[dict],
    parse_mode: Optional[str],
) -> Optional[int]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_notification": False}
    if reply_markup and parse_mode is not None:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=15)
    if r.status_code >= 400:
        try:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.json()}")
        except Exception:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.text}")
        r.raise_for_status()
    return r.json().get("result", {}).get("message_id")

def tg_send_message(
    chat_id: int,
    text: str,
    reply_markup: Optional[dict] = None,
    disable_notification: bool = False
) -> Optional[int]:
    first_id = None
    try:
        chunks = list(_split_chunks(text, 4096))
        for i, ch in enumerate(chunks):
            mid = _send_one(chat_id, ch, reply_markup if i == 0 else None, "HTML")
            if first_id is None:
                first_id = mid
        return first_id
    except requests.HTTPError as e:
        print(f"[{datetime.now()}] ⚠️ Falling back to plain text due to HTML parse error: {e}")
        plain = _strip_html_tags(text)
        first_id = None
        for i, ch in enumerate(_split_chunks(plain, 4096)):
            try:
                mid = _send_one(chat_id, ch, reply_markup if i == 0 else None, None)
                if first_id is None:
                    first_id = mid
            except Exception as e2:
                print(f"[{datetime.now()}] ❌ Telegram fallback send failed: {e2}")
                return first_id
        return first_id
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram sendMessage error for {chat_id}: {e}")
        return None

def tg_pin_message(chat_id: int, message_id: int):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/pinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id, "disable_notification": False},
            timeout=10
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram pinChatMessage error for {chat_id}: {e}")

def tg_unpin_message(chat_id: int, message_id: int):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/unpinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id},
            timeout=10
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram unpinChatMessage error for {chat_id}: {e}")

def pin_warning_if_needed(telegram_id: int, kind: str):
    existing = get_pinned_warnings(telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        return

    other = "expired" if kind == "no_token" else "no_token"
    other_id = existing["expired_msg_id"] if kind == "no_token" else existing["no_token_msg_id"]
    if other_id:
        tg_unpin_message(telegram_id, other_id)
        clear_pinned_warning(telegram_id, other)

    if kind == "no_token":
        text = "⚠️ <b>Bot Issue</b>: no mobile session\n\nPlease add your mobile session token."
    else:
        text = "⚠️ <b>Bot Issue</b>: mobile session expired\n\nPlease update your mobile session token."

    markup = {"inline_keyboard": [[{"text": "➕ Add mobile session", "callback_data": "open_mobile_sessions"}]]}
    message_id = tg_send_message(telegram_id, text, reply_markup=markup)
    if message_id:
        tg_pin_message(telegram_id, message_id)
        save_pinned_warning(telegram_id, kind, message_id)

def unpin_warning_if_any(telegram_id: int, kind: str):
    existing = get_pinned_warnings(telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        tg_unpin_message(telegram_id, msg_id)
        clear_pinned_warning(telegram_id, kind)

# ------------- Blacklane API -------------
def get_offers(token: str):
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
        if r.status_code == 200:
            return 200, r.json().get("results", [])
        else:
            return r.status_code, None
    except Exception as e:
        print(f"[{datetime.now()}] ❌ API exception: {e}")
        return None, None

# ------------- Offer helpers -------------
def _first_blacklist_hit(text: str, terms):
    if not text or not terms:
        return None
    low = text.lower()
    for term in terms:
        if term and term.strip() and term.lower() in low:
            return term
    return None

def _extract_addr(loc: dict) -> str:
    if not loc:
        return "—"
    return loc.get("address") or loc.get("name") or "—"

def _compute_ends_at(offer: dict, filters: dict, pickup_dt):
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()

    if otype == "hourly":
        dur_min = rid.get("durationMinutes")
        if dur_min:
            ends_at = pickup_dt + timedelta(minutes=float(dur_min))
            return ends_at.isoformat(), {
                "duration_minutes": float(dur_min),
                "formula": "pickupTime + durationMinutes"
            }
        return None, None

    if otype == "transfer":
        dist_m = rid.get("estimatedDistanceMeters")
        speed  = filters.get("avg_speed_kmh")
        bonus  = float(filters.get("bonus_time_min", 0) or 0)
        if dist_m and speed:
            dist_km = float(dist_m) / 1000.0
            one_way_min = (dist_km / float(speed)) * 60.0
            total_min   = one_way_min * 2.0 + bonus
            ends_at     = pickup_dt + timedelta(minutes=total_min)
            return ends_at.isoformat(), {
                "distance_km": round(dist_km, 3),
                "speed_kmh": float(speed),
                "one_way_minutes": round(one_way_min, 2),
                "bonus_minutes": bonus,
                "total_minutes": round(total_min, 2),
                "formula": "((distance_km / speed_kmh) * 60) * 2 + bonus_minutes"
            }
        else:
            return None, None

    return None, None

def _build_user_message(offer: dict, status: str, reason: Optional[str], tz_name: Optional[str]) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    vclass = (offer.get("vehicleClass") or "")
    typ_disp = "transfer" if otype == "transfer" else ("hourly" if otype == "hourly" else "—")

    price_disp = _fmt_money(offer.get("price"), offer.get("currency"))
    pu_addr = _extract_addr(rid.get("pickUpLocation"))
    do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else None

    pickup_s = rid.get("pickupTime")
    ends_s   = rid.get("endsAt")
    pickup_disp = _fmt_dt_local(pickup_s, tz_name) if pickup_s else "—"
    ends_disp   = _fmt_dt_local(ends_s, tz_name) if ends_s else "—"

    # extra: flight + special requests
    flight_no = None
    if isinstance(rid.get("flight"), dict):
        flight_no = rid.get("flight", {}).get("number")

    guest_reqs = rid.get("guestRequests")
    if isinstance(guest_reqs, list):
        norm = []
        for it in guest_reqs:
            if isinstance(it, str):
                norm.append(it)
            elif isinstance(it, dict):
                for k in ("label", "name", "value", "text"):
                    if k in it and it[k]:
                        norm.append(str(it[k]))
                        break
        guest_reqs = ", ".join(norm) if norm else None
    elif isinstance(guest_reqs, str):
        pass
    else:
        guest_reqs = None

    dist = _fmt_km(rid.get("estimatedDistanceMeters"))
    dur  = _fmt_minutes(rid.get("estimatedDurationMinutes") or rid.get("durationMinutes"))

    header = "✅ <b>Offer accepted</b>" if status == "accepted" else "⛔ <b>Offer rejected</b>"
    lines = [header]
    if status == "rejected" and reason:
        lines.append(f"<i>Reason:</i> {_esc(reason)}")

    lines += [
        f"🚘 <b>Type:</b> {_esc(typ_disp)}",
        f"🚗 <b>Class:</b> {_esc(vclass)}",
        f"💰 <b>Price:</b> {_esc(price_disp)}",
    ]
    if flight_no:
        lines.append(f"✈️ <b>Flight number:</b> {_esc(flight_no)}")
    if guest_reqs:
        lines.append(f"👁️ <b>Special requests:</b> {_esc(guest_reqs)}")
    if dist != "—":
        lines.append(f"📏 <b>Distance:</b> {_esc(dist)}")
    if dur != "—":
        lines.append(f"⏱️ <b>Duration:</b> {_esc(dur)}")
    lines += [
        f"🕒 <b>Starts at:</b> {_esc(pickup_disp)}",
        f"⏳ <b>Ends at:</b> {_esc(ends_disp)}",
        "",
        f"⬆️ <b>Pickup:</b>\n{_esc(pu_addr)}",
    ]
    if do_addr:
        lines += ["", f"⬇️ <b>Dropoff:</b>\n{_esc(do_addr)}"]

    return "\n".join(lines)

# ---------- DEBUG PRINT ----------
def debug_print_offers(telegram_id: int, offers: list):
    print(f"[{datetime.now()}] 📥 Received {len(offers)} offer(s) for user {telegram_id}")
    for idx, offer in enumerate(offers, start=1):
        rid = (offer.get("rides") or [{}])[0]
        oid = offer.get("id")
        otype = (rid.get("type") or "—")
        vclass = (offer.get("vehicleClass") or "—")
        price = offer.get("price")
        currency = offer.get("currency") or ""
        pickup = rid.get("pickupTime")
        pu = _extract_addr(rid.get("pickUpLocation"))
        do = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else "—"
        print(
            f"[{datetime.now()}] 🧾 Offer {idx}: id={oid} • type={otype} • class={vclass} • "
            f"price={price} {currency} • pickup={pickup} • PU='{pu}' • DO='{do}'"
        )
        try:
            print(json.dumps(offer, indent=2, ensure_ascii=False))
        except Exception:
            print(str(offer))

# ---------- Accepted intervals (busy) ----------
def _load_accepted_intervals(telegram_id: int) -> List[Tuple[datetime, Optional[datetime]]]:
    """
    Read previously accepted rides from DB and return list of (start_dt, end_dt).
    We only keep rows with a parseable pickup_time; end_dt may be None.
    """
    rows: List[Tuple[datetime, Optional[datetime]]] = []
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            SELECT pickup_time, ends_at
            FROM offer_logs
            WHERE telegram_id = ? AND status = 'accepted'
        """, (telegram_id,))
        for pu_s, end_s in c.fetchall():
            try:
                start_dt = parser.isoparse(pu_s.replace(" ", "T")) if pu_s else None
            except Exception:
                start_dt = None
            try:
                end_dt = parser.isoparse(end_s.replace(" ", "T")) if end_s else None
            except Exception:
                end_dt = None
            if start_dt:
                rows.append((start_dt, end_dt))
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ Could not load accepted intervals: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return rows

def _find_conflict(
    new_start: datetime,
    new_end_iso: Optional[str],
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]],
) -> Optional[Tuple[datetime, datetime]]:
    """
    Returns the first conflicting (start,end) interval if:
      - new_start is inside an accepted interval, OR
      - new interval overlaps any accepted interval (if new_end is known).
    """
    new_end = None
    if new_end_iso:
        try:
            new_end = parser.isoparse(new_end_iso)
        except Exception:
            new_end = None

    for a_start, a_end in accepted_intervals:
        if not a_end:
            continue
        # Case 1: pickup falls inside an accepted interval
        if a_start <= new_start <= a_end:
            return (a_start, a_end)
        # Case 2: the new window overlaps the accepted window (if we know new_end)
        if new_end and not (new_end <= a_start or new_start >= a_end):
            return (a_start, a_end)
    return None

# ------------- Poll one user -------------
def poll_user(user):
    telegram_id, token, filters_json, active = user

    tz_name = get_user_timezone(telegram_id) or "UTC"
    print(f"[{datetime.now()}] 🔍 Polling user {telegram_id} (active={active}) tz={tz_name}")

    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    # ---- MOCK DATA (development mode) ----
    if USE_MOCK:
        offers = [
            {
                "type": "ride", "id": "mock-06102355aab-68d230-4384-a5ac-7814e2e62202af52",
                "price": 120.9, "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "van",
                "rides": [{
                    "type": "hourly",
                    "createdAt": "2025-08-03T19:40:19Z",
                    "pickUpLocation": {
                        "name": "la Vie en Rose Quartiers Dix 30",
                        "address": "la Vie en Rose Quartiers Dix 30, Avenue des Lumières 1600, J4Y 0A5 Brossard, Québec"
                    },
                    "pickupTime": "2025-08-19T08:45:00-04:00",
                    "kmIncluded": 80,
                    "durationMinutes": 120,
                    "guestRequests": ["Baby seat", "VIP pickup"],
                    "flight": {"number": "EK 001"}
                }]
            },
            {
                "type": "ride", "id": "mock-9aeq7a39ef-e4622d12-4f2e1-ab3a-a3398x3d14af79ca",
                "price": 96.05, "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "business",
                "rides": [{
                    "type": "transfer",
                    "createdAt": "2025-08-03T19:34:08Z",
                    "pickUpLocation": {
                        "name": "Centropolis",
                        "address": "Centropolis, Avenue Pierre-Péladeau 1799, H7T 2Y5 Laval, Québec"
                    },
                    "dropOffLocation": {
                        "name": "CF Carrefour Laval",
                        "address": "CF Carrefour Laval, Boulevard le Carrefour 3003, H7T 1C7 Laval, Québec"
                    },
                    "pickupTime": "2025-08-19T20:18:00-04:00",
                    "estimatedDurationMinutes": 32,
                    "estimatedDistanceMeters": 22266,
                    "guestRequests": ["EK Complimentary", "2 Guest(s)", "[6432 40E AVENUE H1T 2V7 MONTREAL]"],
                    "flight": {"number": "EK 243"}
                }]
            }
        ]
    else:
        # ---- LIVE MODE ----
        if not token or not str(token).strip():
            pin_warning_if_needed(telegram_id, "no_token")
            return

        status_code, offers = get_offers(token)

        if status_code == 403:
            set_token_status(telegram_id, "expired")
            pin_warning_if_needed(telegram_id, "expired")
            return
        elif status_code == 200:
            set_token_status(telegram_id, "valid")
            unpin_warning_if_any(telegram_id, "expired")
            unpin_warning_if_any(telegram_id, "no_token")
        else:
            return

        if not offers:
            print(f"[{datetime.now()}] ℹ️ No offers for user {telegram_id}")
            return

    # 🔊 Print offers for debugging
    debug_print_offers(telegram_id, offers)

    filters        = json.loads(filters_json) if filters_json else {}
    class_state    = get_vehicle_classes_state(telegram_id)
    booked_slots   = get_booked_slots(telegram_id)
    processed_ids  = get_processed_offer_ids(telegram_id)
    # Load already accepted intervals from DB
    accepted_intervals = _load_accepted_intervals(telegram_id)

    for offer in offers:
        oid = offer.get("id")

        # Skip already processed
        if oid in processed_ids:
            print(f"[{datetime.now()}] ⏭️ Skipping offer {oid} for user {telegram_id} – already processed (DB).")
            continue
        if oid in accepted_per_user[telegram_id] or oid in rejected_per_user[telegram_id]:
            print(f"[{datetime.now()}] ⏭️ Skipping offer {oid} for user {telegram_id} – already processed (memory).")
            continue

        rid      = (offer.get("rides") or [{}])[0]
        price    = float(offer.get("price", 0) or 0)
        otype    = (rid.get("type") or "").lower()
        raw_vc   = offer.get("vehicleClass", "")
        pickup_s = rid.get("pickupTime")
        if not pickup_s:
            continue
        pickup   = parser.isoparse(pickup_s)  # aware

        pu_addr = _extract_addr(rid.get("pickUpLocation"))
        do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else ""

        # Compute endsAt (for transfer) / ensure hourly has it too
        ends_at_iso, end_calc = _compute_ends_at(offer, filters, pickup)
        if ends_at_iso:
            offer["rides"][0]["endsAt"] = ends_at_iso
            if end_calc:
                offer["rides"][0]["_endCalc"] = end_calc

        # 0) Working hours (user timezone)
        ws = filters.get("work_start")
        we = filters.get("work_end")
        if ws and we:
            pickup_local = pickup.astimezone(gettz(tz_name))
            pickup_t = pickup_local.time()
            start_t  = datetime.strptime(ws, "%H:%M").time()
            end_t    = datetime.strptime(we, "%H:%M").time()
            if not (start_t <= pickup_t <= end_t):
                reason = f"heure pickup {pickup_t.strftime('%H:%M')} hors plage {ws}–{we}"
                print(f"[{datetime.now()}] ⛔ Rejected {oid} – outside work hours {ws}-{we} (user tz {tz_name})")
                log_offer_decision(telegram_id, offer, "rejected", reason)
                tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
                rejected_per_user[telegram_id].add(oid)
                processed_ids.add(oid)
                continue

        # 1) Gap filter (UTC base)
        gap_min = filters.get("gap", 0)
        if gap_min:
            now_utc = datetime.now(timezone.utc)
            if pickup < now_utc + timedelta(minutes=gap_min):
                mins_left = max(0, (pickup - now_utc).total_seconds() / 60)
                reason = f"délai minimal {gap_min} min non respecté ({mins_left:.0f} min restants)"
                print(f"[{datetime.now()}] ⛔ Rejected {oid} – gap {gap_min} min; pickup in {mins_left:.0f} min")
                log_offer_decision(telegram_id, offer, "rejected", reason)
                tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
                rejected_per_user[telegram_id].add(oid)
                processed_ids.add(oid)
                continue

        # 2) Price filter
        min_p = filters.get("price_min", 0)
        max_p = filters.get("price_max", float("inf"))
        if price < min_p:
            reason = f"prix {price} < minimum {min_p}"
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {reason}")
            log_offer_decision(telegram_id, offer, "rejected", reason)
            tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
            rejected_per_user[telegram_id].add(oid)
            processed_ids.add(oid)
            continue
        if price > max_p:
            reason = f"prix {price} > maximum {max_p}"
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {reason}")
            log_offer_decision(telegram_id, offer, "rejected", reason)
            tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
            rejected_per_user[telegram_id].add(oid)
            processed_ids.add(oid)
            continue

        # 3) Blacklists
        pickup_terms  = (filters.get("pickup_blacklist")  or [])
        dropoff_terms = (filters.get("dropoff_blacklist") or [])

        hit_pu = _first_blacklist_hit(pu_addr, pickup_terms)
        if hit_pu:
            reason = f"pickup contient «{hit_pu}»"
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – pickup blacklist term '{hit_pu}'")
            log_offer_decision(telegram_id, offer, "rejected", reason)
            tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
            rejected_per_user[telegram_id].add(oid)
            processed_ids.add(oid)
            continue

        if do_addr:
            hit_do = _first_blacklist_hit(do_addr, dropoff_terms)
            if hit_do:
                reason = f"dropoff contient «{hit_do}»"
                print(f"[{datetime.now()}] ⛔ Rejected {oid} – dropoff blacklist term '{hit_do}'")
                log_offer_decision(telegram_id, offer, "rejected", reason)
                tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
                rejected_per_user[telegram_id].add(oid)
                processed_ids.add(oid)
                continue

        # 4) Class filter
        otype_dict = class_state.get(otype, {})
        matched_vc = next((cls for cls in otype_dict.keys() if cls.lower() == raw_vc.lower()), None)
        enabled = otype_dict.get(matched_vc, 0) if matched_vc else 0
        if not enabled:
            reason = f"{otype} '{raw_vc}' désactivé"
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {reason} (matched='{matched_vc}')")
            log_offer_decision(telegram_id, offer, "rejected", reason)
            tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
            rejected_per_user[telegram_id].add(oid)
            processed_ids.add(oid)
            continue

        # 5) Booked-slots (user tz)
        conflict = False
        pickup_local = pickup.astimezone(gettz(tz_name))
        for slot in booked_slots:
            try:
                start_naive = datetime.strptime(slot["from"], "%d/%m/%Y %H:%M")
                end_naive   = datetime.strptime(slot["to"],   "%d/%m/%Y %H:%M")
            except Exception:
                continue
            start_local = start_naive.replace(tzinfo=gettz(tz_name))
            end_local   = end_naive.replace(tzinfo=gettz(tz_name))
            if start_local <= pickup_local <= end_local:
                reason = f"tombe dans créneau bloqué «{slot.get('name') or 'Sans nom'}»"
                print(f"[{datetime.now()}] ⛔ Rejected {oid} – in booked slot (user tz {tz_name})")
                log_offer_decision(telegram_id, offer, "rejected", reason)
                tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
                rejected_per_user[telegram_id].add(oid)
                processed_ids.add(oid)
                conflict = True
                break
        if conflict:
            continue

        # 5.5) Conflict with already accepted offers (busy)
        conflict_with = _find_conflict(pickup, ends_at_iso, accepted_intervals)
        if conflict_with:
            a_start, a_end = conflict_with
            reason = (
                "conflit avec une course acceptée "
                f"({_fmt_dt_local_from_dt(a_start, tz_name)} – {_fmt_dt_local_from_dt(a_end, tz_name)})"
            )
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {reason}")
            log_offer_decision(telegram_id, offer, "rejected", reason)
            tg_send_message(telegram_id, _build_user_message(offer, "rejected", reason, tz_name))
            rejected_per_user[telegram_id].add(oid)
            processed_ids.add(oid)
            continue

        # 6) Accept
        print(f"[{datetime.now()}] ✅ Accepted {oid}")
        offer_to_log = deepcopy(offer)
        log_offer_decision(telegram_id, offer_to_log, "accepted", None)
        tg_send_message(telegram_id, _build_user_message(offer_to_log, "accepted", None, tz_name))
        accepted_per_user[telegram_id].add(oid)
        processed_ids.add(oid)

        # add this newly accepted interval to in-memory 'busy' cache for subsequent offers in the same cycle
        try:
            new_end_dt = parser.isoparse(offer_to_log["rides"][0].get("endsAt")) if offer_to_log["rides"][0].get("endsAt") else None
        except Exception:
            new_end_dt = None
        accepted_intervals.append((pickup, new_end_dt))

    return f"Done with user {telegram_id}"

if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 Poller started")
    while True:
        print(f"[{datetime.now()}] 🔄 Starting polling cycle")
        users = get_all_users()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(poll_user, u) for u in users]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        print(f"[{datetime.now()}] ✅ {res}")
                except Exception as e:
                    print(f"[{datetime.now()}] ❌ Poll error: {e}")
        time.sleep(POLL_INTERVAL)
