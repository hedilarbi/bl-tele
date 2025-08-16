# poller.py
import time
import json
import uuid
import requests
from datetime import datetime, timezone, timedelta
from dateutil import parser
from dateutil.tz import gettz
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from db import (
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
POLL_INTERVAL = 5
MAX_WORKERS   = 10

accepted_per_user = defaultdict(set)
rejected_per_user = defaultdict(set)


# ------------- Telegram helpers -------------
def tg_send_message(chat_id: int, text: str, reply_markup: dict | None = None, disable_notification: bool = False):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_notification": disable_notification}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram sendMessage error for {chat_id}: {e}")
        return None


def tg_pin_message(chat_id: int, message_id: int):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/pinChatMessage"
    payload = {"chat_id": chat_id, "message_id": message_id, "disable_notification": False}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram pinChatMessage error for {chat_id}: {e}")


def tg_unpin_message(chat_id: int, message_id: int):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/unpinChatMessage"
    payload = {"chat_id": chat_id, "message_id": message_id}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram unpinChatMessage error for {chat_id}: {e}")


def pin_warning_if_needed(telegram_id: int, kind: str):
    """
    kind: "no_token" | "expired"
    """
    existing = get_pinned_warnings(telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        # already pinned
        return

    # Unpin the other kind to avoid conflicts
    other = "expired" if kind == "no_token" else "no_token"
    other_id = existing["expired_msg_id"] if kind == "no_token" else existing["no_token_msg_id"]
    if other_id:
        tg_unpin_message(telegram_id, other_id)
        clear_pinned_warning(telegram_id, other)

    if kind == "no_token":
        text = "⚠️ *Bot Issue*: no mobile session\n\nPlease add your mobile session token."
    else:
        text = "⚠️ *Bot Issue*: mobile session expired\n\nPlease update your mobile session token."

    markup = {
        "inline_keyboard": [
            [{"text": "➕ Add mobile session", "callback_data": "open_mobile_sessions"}]
        ]
    }
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


def _fmt_dt_local(s: str, tz_name: str | None) -> str:
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


def _build_user_message(offer: dict, status: str, reason: str | None, tz_name: str | None) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    vclass = (offer.get("vehicleClass") or "")
    vclass_disp = vclass.capitalize() if vclass else "—"

    price = offer.get("price")
    currency = offer.get("currency") or ""
    price_disp = f"{float(price):.2f} {currency}".strip() if price is not None else "—"

    pu_addr = _extract_addr(rid.get("pickUpLocation"))
    do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else None

    pickup_s = rid.get("pickupTime")
    ends_s   = rid.get("endsAt")

    pickup_disp = _fmt_dt_local(pickup_s, tz_name) if pickup_s else "—"
    ends_disp   = _fmt_dt_local(ends_s, tz_name) if ends_s else "—"

    header = "✅ Offre acceptée" if status == "accepted" else "⛔ Offre rejetée"
    typ_disp = "Transfer" if otype == "transfer" else ("Hourly" if otype == "hourly" else "—")

    lines = [
        f"{header}",
        f"💰 Prix : {price_disp}",
        f"🚗 Type : {typ_disp}  •  Classe : {vclass_disp}",
        f"📍 Pickup : {pu_addr}",
    ]
    if otype == "transfer" and do_addr:
        lines.append(f"🏁 Dropoff : {do_addr}")

    lines.append(f"🕒 PickupTime : {pickup_disp}")
    lines.append(f"⏱️ Fin estimée : {ends_disp}")

    if otype == "hourly":
        dur_min = rid.get("durationMinutes")
        if dur_min is not None:
            try:
                lines.append(f"⌛ Durée : {float(dur_min):.0f} min")
            except Exception:
                lines.append(f"⌛ Durée : {dur_min} min")

    if status == "rejected" and reason:
        lines.append(f"🛑 Raison : {reason}")

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
        # Full JSON (pretty)
        try:
            print(json.dumps(offer, indent=2, ensure_ascii=False))
        except Exception:
            print(str(offer))


# ------------- Poll one user -------------
def poll_user(user):
    telegram_id, token, filters_json, active = user

    tz_name = get_user_timezone(telegram_id) or "UTC"
    user_tz = gettz(tz_name) or gettz("UTC")

    print(f"[{datetime.now()}] 🔍 Polling user {telegram_id} (active={active}) tz={tz_name}")

    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    # Token checks
    if not token or not str(token).strip():
        pin_warning_if_needed(telegram_id, "no_token")
        return

    # Live call
    status_code, offers = get_offers(token)

    if status_code == 403:
        # Token expired
        set_token_status(telegram_id, "expired")
        pin_warning_if_needed(telegram_id, "expired")
        return
    elif status_code == 200:
        # Valid token now — clear warnings
        set_token_status(telegram_id, "valid")
        unpin_warning_if_any(telegram_id, "expired")
        unpin_warning_if_any(telegram_id, "no_token")
    else:
        # Network or API error; skip quietly
        return

    if not offers:
        print(f"[{datetime.now()}] ℹ️ No offers for user {telegram_id}")
        return

    # 🔊 Print offers for debugging
    debug_print_offers(telegram_id, offers)

    filters      = json.loads(filters_json) if filters_json else {}
    class_state  = get_vehicle_classes_state(telegram_id)
    booked_slots = get_booked_slots(telegram_id)
    processed_ids = get_processed_offer_ids(telegram_id)

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

        # Compute endsAt
        ends_at_iso, end_calc = _compute_ends_at(offer, filters, pickup)
        if ends_at_iso:
            offer["rides"][0]["endsAt"] = ends_at_iso
            if end_calc:
                offer["rides"][0]["_endCalc"] = end_calc

        # 0) Working hours (user timezone)
        ws = filters.get("work_start")
        we = filters.get("work_end")
        if ws and we:
            pickup_local = pickup.astimezone(user_tz)
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
        pickup_local = pickup.astimezone(user_tz)
        for slot in booked_slots:
            try:
                start_naive = datetime.strptime(slot["from"], "%d/%m/%Y %H:%M")
                end_naive   = datetime.strptime(slot["to"],   "%d/%m/%Y %H:%M")
            except Exception:
                continue
            start_local = start_naive.replace(tzinfo=user_tz)
            end_local   = end_naive.replace(tzinfo=user_tz)
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

        # 6) Accept
        print(f"[{datetime.now()}] ✅ Accepted {oid}")
        offer_to_log = deepcopy(offer)
        log_offer_decision(telegram_id, offer_to_log, "accepted", None)
        tg_send_message(telegram_id, _build_user_message(offer_to_log, "accepted", None, tz_name))
        accepted_per_user[telegram_id].add(oid)
        processed_ids.add(oid)

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
