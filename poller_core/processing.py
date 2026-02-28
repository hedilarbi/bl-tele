import uuid
import json
import re
import time
import concurrent.futures
import builtins as _builtins
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, timezone, timedelta
from datetime import time as dt_time
from dateutil import parser
from dateutil.tz import gettz
from copy import deepcopy

from .config import (
    DEBUG_PRINT_OFFERS,
    DEBUG_ENDS,
    AUTO_RESERVE_ENABLED,
    P1_RESERVE_TIMEOUT_S,
    P2_RESERVE_TIMEOUT_S,
    FAST_ACCEPT_MODE,
    FAST_ACCEPT_NOTIFY_REJECTED,
    OFFER_MEMORY_DEDUPE,
)
from .utils import (
    _esc,
    _fmt_money,
    _fmt_km,
    _fmt_minutes,
    _fmt_dt_local,
    _fmt_dt_local_from_dt,
    _duration_minutes_from_rid,
    _compute_ends_at,
    _fmt_local_iso,
    _extract_addr,
)
from .filters import (
    _get_enabled_filter_slugs,
    _run_custom_filters,
    _format_filter_summary,
    _find_conflict,
)
from .notify import maybe_send_message, _platform_icon
from .state import accepted_per_user, rejected_per_user, invalidate_rides_cache, set_rides_cache
from .p1_client import reserve_offer_p1, get_rides_p1
from .p1_auth import maybe_refresh_p1_session
from .p2_client import reserve_offer_p2, _athena_get_rides, _filter_rides_by_bl_uuid
from .rides import _extract_intervals_from_rides
from db import log_offer_decision, save_offer_message, get_bl_uuid, set_token_status


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print


def _refresh_rides_cache_now(
    bot_id: str,
    telegram_id: int,
    tz_name: str,
    p1_token: Optional[str],
    p1_headers: Optional[dict],
    p2_token: Optional[str],
):
    intervals: List[Tuple[datetime, Optional[datetime]]] = []
    bl_uuid = get_bl_uuid(bot_id, telegram_id)

    if p2_token:
        status_code, payload, _ = _athena_get_rides(p2_token)
        if status_code == 200 and isinstance(payload, dict):
            data_all = (payload or {}).get("data") or []
            data_kept = _filter_rides_by_bl_uuid(data_all, bl_uuid) if bl_uuid else data_all
            intervals.extend(
                _extract_intervals_from_rides(
                    [
                        (r.get("attributes") or {})
                        | {"starts_at": (r.get("attributes") or {}).get("starts_at")}
                        for r in (data_kept or [])
                    ]
                )
            )

    if p1_token:
        status_code, ride_results = get_rides_p1(p1_token, headers=p1_headers)
        if status_code == 200 and isinstance(ride_results, list):
            kept = _filter_rides_by_bl_uuid(ride_results, bl_uuid) if bl_uuid else ride_results
            intervals.extend(_extract_intervals_from_rides(kept))

    set_rides_cache(bot_id, telegram_id, intervals)


_bg_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
_notify_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)


def _refresh_rides_cache_async(
    bot_id: str,
    telegram_id: int,
    tz_name: str,
    p1_token: Optional[str],
    p1_headers: Optional[dict],
    p2_token: Optional[str],
):
    def _job():
        try:
            _refresh_rides_cache_now(bot_id, telegram_id, tz_name, p1_token, p1_headers, p2_token)
        except Exception:
            invalidate_rides_cache(bot_id, telegram_id)

    _bg_executor.submit(_job)


def _reserve_with_timeout(fn, timeout_s: int, *args, **kwargs):
    # Reserve client functions already enforce request-level timeouts.
    # Avoiding an extra executor queue removes queue-delay timeouts.
    _ = timeout_s
    return fn(*args, **kwargs)


def _send_notification_async(
    bot_id: str,
    telegram_id: int,
    kind: str,
    text: str,
    platform: str,
    reply_markup: Optional[dict] = None,
    force_notify: bool = False,
):
    def _job():
        try:
            maybe_send_message(
                bot_id,
                telegram_id,
                kind,
                text,
                platform,
                reply_markup=reply_markup,
                force_notify=force_notify,
            )
        except Exception:
            return None

    _notify_executor.submit(_job)


def _poll_latency_ms(offer: dict) -> Optional[int]:
    try:
        ts = offer.get("_poll_ts") or offer.get("_poll_time")
        if ts is None:
            return None
        return int((time.time() - float(ts)) * 1000)
    except Exception:
        return None


def _reserve_failure_human_reason(status_code: Optional[int], body: Any) -> str:
    text = ""
    if isinstance(body, dict):
        for k in ("detail", "message", "error", "title"):
            v = body.get(k)
            if v:
                text = str(v)
                break
        if not text:
            try:
                text = json.dumps(body, ensure_ascii=False)
            except Exception:
                text = str(body)
    elif body is not None:
        text = str(body)

    low = text.lower()
    if any(k in low for k in ("already taken", "already accepted", "not available", "no longer available")):
        return "Offer deja prise par un autre chauffeur."
    if status_code == 409:
        return "Conflit 409: offre deja prise."
    if status_code == 410:
        return "Offre expiree/supprimee (410): elle n'est plus reservable."
    if status_code == 422:
        return "Offre devenue invalide (422)."
    if status_code in (401, 403):
        return f"Session expiree (HTTP {status_code})."
    if status_code is not None and 500 <= int(status_code) < 600:
        return f"Erreur serveur Blacklane (HTTP {status_code})."
    if status_code is None:
        if "timeout" in low:
            return "Timeout reseau pendant la reservation."
        return "Erreur reseau pendant la reservation."
    return f"Reservation refusee (HTTP {status_code})."


def _build_user_message(
    offer: dict,
    status: str,
    reason: Optional[str],
    tz_name: Optional[str],
    filters_summary: Optional[str] = None,
    filter_results: Optional[List[dict]] = None,
    platform: Optional[str] = None,
    forced_accept: bool = False,
) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    vclass = (offer.get("vehicleClass") or "")
    typ_disp = "transfer" if otype == "transfer" else ("hourly" if otype == "hourly" else "—")
    price_disp = _fmt_money(offer.get("price"), offer.get("currency"))
    pu_addr = _extract_addr(rid.get("pickUpLocation"))
    do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else None
    pickup_s = rid.get("pickupTime")
    ends_s = rid.get("endsAt")
    pickup_disp = _fmt_dt_local(pickup_s, tz_name) if pickup_s else "—"
    ends_disp = _fmt_dt_local(ends_s, tz_name) if ends_s else "—"

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
    dur = _fmt_minutes(_duration_minutes_from_rid(rid))
    lines = [
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
    if status in ("rejected", "not_accepted") and reason:
        lines += ["", f"⚠️ <b>Reason:</b> {_esc(reason)}"]

    if status == "accepted":
        status_icon = "🟢"
    elif status == "not_accepted":
        status_icon = "🟠"
    else:
        status_icon = "🔴"
    plat_icon = _platform_icon(platform or "p1")
    if status == "accepted":
        status_word = "Offer Accepted"
    elif status == "not_accepted":
        status_word = "Not Accepted"
    else:
        status_word = "not valid"
    if forced_accept and status == "accepted":
        status_word = "valid (override)"
    header = f"🔥 New offer - {price_disp} - {status_icon} {status_word} {plat_icon}"
    body = "\n".join(lines)
    filters_block = _format_filter_summary(filter_results or []) if filter_results else (filters_summary or "")
    parts = [header, body]
    if filters_block:
        parts.append(filters_block)
    return "\n\n".join(parts)


def _build_offer_header_line(
    offer: dict,
    status: str,
    platform: Optional[str],
    forced_accept: bool = False,
) -> str:
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()
    price_disp = _fmt_money(offer.get("price"), offer.get("currency"))
    if status == "accepted":
        status_icon = "🟢"
    elif status == "not_accepted":
        status_icon = "🟠"
    else:
        status_icon = "🔴"
    plat_icon = _platform_icon(platform or "p1")
    if status == "accepted":
        status_word = "Offer Accepted"
    elif status == "not_accepted":
        status_word = "Not Accepted"
    else:
        status_word = "not valid"
    if forced_accept and status == "accepted":
        status_word = "valid (override)"
    return f"🔥 New offer - {price_disp} - {status_icon} {status_word} {plat_icon}"


def _build_reject_summary_lines(filter_results: List[dict]) -> str:
    failed = [fr for fr in (filter_results or []) if not fr.get("ok")]
    if not failed:
        return ""
    lines = []
    for fr in failed:
        detail = fr.get("detail") or fr.get("name") or "rejected"
        lines.append(f"❌ {_esc(detail)}")
    return "\n".join(lines)


def debug_print_offers(telegram_id: int, offers: list):
    if not DEBUG_PRINT_OFFERS:
        return
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
        plat = offer.get("_platform")
        print(
            f"[{datetime.now()}] 🧾 Offer {idx} [{plat}]: id={oid} • type={otype} • class={vclass} • "
            f"price={price} {currency} • pickup={pickup} • PU='{pu}' • DO='{do}'"
        )
        try:
            print(json.dumps(offer, indent=2, ensure_ascii=False))
        except Exception:
            print(str(offer))


def _process_offers_for_user(
    bot_id: str,
    telegram_id: int,
    offers: List[Dict[str, Any]],
    filters: dict,
    class_state: dict,
    booked_slots: List[dict],
    blocked_days: set,
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]],
    tz_name: str,
    p1_token: Optional[str] = None,
    p1_headers: Optional[dict] = None,
    p2_token: Optional[str] = None,
):
    user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
    pending_notifications: List[Tuple[str, str, str, Optional[dict], bool]] = []

    def _queue_notification(
        kind: str,
        text: str,
        platform_name: str,
        reply_markup: Optional[dict] = None,
        force_notify: bool = False,
    ):
        pending_notifications.append((kind, text, platform_name, reply_markup, force_notify))

    if user_cfilters:
        slugs = ", ".join(sorted(user_cfilters.keys()))
        print(f"[{datetime.now()}] 🧩 Custom filters for {bot_id}/{telegram_id}: {slugs}")
    else:
        print(f"[{datetime.now()}] 🧩 Custom filters for {bot_id}/{telegram_id}: none")

    for offer in offers:
        oid = offer.get("id")
        platform = offer.get("_platform", "p1")

        # Optional memory dedupe; disabled in race mode to avoid missing reused offer ids.
        if OFFER_MEMORY_DEDUPE and (
            oid in accepted_per_user[bot_id][telegram_id]
            or oid in rejected_per_user[bot_id][telegram_id][platform]
        ):
            print(f"[{datetime.now()}] ⏭️ Skipping offer {oid} for user {telegram_id} – already processed (memory).")
            continue

        rid = (offer.get("rides") or [{}])[0]
        price = float(offer.get("price", 0) or 0)
        otype = (rid.get("type") or "").lower()
        raw_vc = offer.get("vehicleClass", "")
        pickup_s = rid.get("pickupTime")
        if not pickup_s:
            continue
        try:
            pickup = parser.isoparse(pickup_s)  # aware
        except Exception:
            continue

        # Compute endsAt using formulas or duration
        ends_at_iso, end_calc = _compute_ends_at(offer, filters, pickup, tz_name)
        if ends_at_iso:
            rid["endsAt"] = ends_at_iso
        if end_calc:
            rid["_endsAtCalc"] = end_calc

        if DEBUG_ENDS:
            pid = offer.get("id")
            kind = (rid.get("type") or "").lower()
            pu = rid.get("pickupTime")
            end = rid.get("endsAt")
            fstr = (end_calc or {}).get("formula") or "—"
            print(
                f"[{datetime.now()}] OFFER[{pid}] type={kind} | pickup={_fmt_local_iso(pu, tz_name)} "
                f"| endsAt={_fmt_local_iso(end, tz_name)} | formula={fstr}"
            )

        # --- 0) Working hours & blocked days (user timezone) ---
        filter_results: List[dict] = []
        accept_override = False

        def record_result(name: str, ok: bool, detail: Optional[str] = None):
            filter_results.append({"name": name, "ok": bool(ok), "detail": detail})

        pickup_local = pickup.astimezone(gettz(tz_name))
        pickup_t = pickup_local.time()

        ws = filters.get("work_start")
        we = filters.get("work_end")
        if ws and we:
            from .utils import _parse_hhmm, _to_str
            ws_hm = _parse_hhmm(_to_str(ws))
            we_hm = _parse_hhmm(_to_str(we))
            if ws_hm and we_hm:
                start_t = dt_time(ws_hm[0], ws_hm[1])
                end_t = dt_time(we_hm[0], we_hm[1])
                if not (start_t <= pickup_t <= end_t):
                    reason = f"heure pickup {pickup_t.strftime('%H:%M')} hors plage {ws}–{we}"
                    record_result("Horaires", False, reason)
                else:
                    record_result("Horaires", True, f"{pickup_t.strftime('%H:%M')} dans {ws}–{we}")

        day_key = pickup_local.strftime("%d/%m/%Y")
        if day_key in blocked_days:
            record_result("Jours bloqués", False, f"jour {day_key} bloqué (Schedule)")
        elif blocked_days:
            record_result("Jours bloqués", True, f"{day_key} autorisé")

        # 1) Minimal gap before pickup vs current time (UTC base)
        gap_min_now = filters.get("gap", 0)
        if gap_min_now:
            now_utc = datetime.now(timezone.utc)
            mins_left = max(0, (pickup - now_utc).total_seconds() / 60)
            if pickup < now_utc + timedelta(minutes=float(gap_min_now)):
                record_result(
                    "Délai minimal",
                    False,
                    f"{mins_left:.0f} min restants < seuil {gap_min_now} min",
                )
            else:
                record_result("Délai minimal", True, f"{mins_left:.0f} min restants")

        # 1.5) Enforce min/max hourly duration (filters in HOURS)
        dur_min_est = _duration_minutes_from_rid(rid)
        if otype == "hourly":
            min_hours = filters.get("min_hourly_hours", filters.get("min_duration", 0)) or 0
            max_hours = filters.get("max_hourly_hours", filters.get("max_duration"))  # may be None
            try:
                min_minutes = float(min_hours) * 60.0
            except Exception:
                min_minutes = 0.0
            try:
                max_minutes = float(max_hours) * 60.0 if max_hours is not None else None
            except Exception:
                max_minutes = None

            if min_minutes:
                ok = not (dur_min_est is None or dur_min_est < min_minutes)
                record_result(
                    "Durée horaire min",
                    ok,
                    None if ok else f"{0 if dur_min_est is None else dur_min_est:.0f} min < min {min_minutes:.0f} min",
                )
            if max_minutes is not None and dur_min_est is not None:
                ok = dur_min_est <= max_minutes
                record_result(
                    "Durée horaire max",
                    ok,
                    None if ok else f"{dur_min_est:.0f} min > max {max_minutes:.0f} min",
                )

        # Custom filters (user-defined)
        decision, reason_txt = _run_custom_filters(offer, user_cfilters, tz_name)
        if decision == "reject":
            record_result("Filtres personnalisés", False, reason_txt or "rejeté")
        elif decision == "accept":
            accept_override = True
            record_result("Filtres personnalisés", True, reason_txt or "accepté")
        elif user_cfilters:
            record_result("Filtres personnalisés", True, "ok")

        # 2) Price filter
        min_p = float(filters.get("price_min", 0) or 0)
        max_p = float(filters.get("price_max", float("inf")))
        if min_p:
            ok = price >= min_p
            record_result("Prix min", ok, None if ok else f"prix {price} < minimum {min_p}")
        if max_p != float("inf"):
            ok = price <= max_p
            record_result("Prix max", ok, None if ok else f"prix {price} > maximum {max_p}")

        # 2.5) Distance filters
        if otype == "transfer":
            dist_m = rid.get("estimatedDistanceMeters")
            if dist_m is not None:
                try:
                    dist_m = float(dist_m)
                except Exception:
                    dist_m = None
            if dist_m is not None:
                min_km = float(filters.get("min_km", 0) or 0)
                max_km = float(filters.get("max_km", float("inf")) or float("inf"))
                min_m = min_km * 1000.0
                max_m = max_km * 1000.0
                dist_km = dist_m / 1000.0

                if min_km:
                    ok = dist_m >= min_m
                    record_result("Distance min", ok, None if ok else f"distance {dist_km:.1f} km < {min_km:g} km")
                if max_km != float("inf"):
                    ok = dist_m <= max_m
                    record_result("Distance max", ok, None if ok else f"distance {dist_km:.1f} km > {max_km:g} km")
        elif otype == "hourly":
            # Optional hourly km constraints if provided
            km_inc = rid.get("kmIncluded")
            try:
                km_inc = float(km_inc) if km_inc is not None else None
            except Exception:
                km_inc = None
            if km_inc is not None:
                h_min_km = filters.get("min_hourly_km")
                h_max_km = filters.get("max_hourly_km")
                if h_min_km is not None:
                    ok = km_inc >= float(h_min_km)
                    record_result("Km inclus min", ok, None if ok else f"{km_inc:g} < {float(h_min_km):g}")
                if h_max_km is not None:
                    ok = km_inc <= float(h_max_km)
                    record_result("Km inclus max", ok, None if ok else f"{km_inc:g} > {float(h_max_km):g}")

        # 3) Blacklists
        pickup_terms = (filters.get("pickup_blacklist") or [])
        dropoff_terms = (filters.get("dropoff_blacklist") or [])
        pu_addr = _extract_addr(rid.get("pickUpLocation"))
        do_addr = _extract_addr(rid.get("dropOffLocation")) if rid.get("dropOffLocation") else ""

        def _first_blacklist_hit(text: str, terms):
            if not text or not terms:
                return None
            low = text.lower()
            for term in terms:
                if term and term.strip() and term.lower() in low:
                    return term
            return None

        hit_pu = _first_blacklist_hit(pu_addr, pickup_terms)
        if pickup_terms:
            record_result("Pickup blacklist", hit_pu is None, None if hit_pu is None else f"pickup contient «{hit_pu}»")

        hit_do = _first_blacklist_hit(do_addr, dropoff_terms) if do_addr else None
        if dropoff_terms and do_addr:
            record_result("Dropoff blacklist", hit_do is None, None if hit_do is None else f"dropoff contient «{hit_do}»")

        # 3.5) Flight blocklist
        flight_terms = (filters.get("flight_blacklist") or [])
        flight_no = None
        if isinstance(rid.get("flight"), dict):
            flight_no = rid.get("flight", {}).get("number")
        if not flight_no:
            flight_no = rid.get("flight_number") or offer.get("flight_number")
        if flight_terms:
            def _norm_flight(s: str) -> str:
                return re.sub(r"[^A-Za-z0-9]", "", str(s or "")).upper()

            if flight_no:
                target = _norm_flight(flight_no)
                hit = next((t for t in flight_terms if _norm_flight(t) == target and target), None)
                record_result("Vols bloqués", hit is None, None if hit is None else f"vol {flight_no} bloqué")
            else:
                record_result("Vols bloqués", True, "aucun numéro de vol")

        # 4) Class filter
        otype_dict = class_state.get(otype, {})
        matched_vc = next((cls for cls in otype_dict.keys() if cls.lower() == raw_vc.lower()), None)
        enabled = otype_dict.get(matched_vc, 0) if matched_vc else 0
        record_result("Classe véhicule", bool(enabled), f"{otype} '{raw_vc}' désactivé" if not enabled else None)

        # 5) Booked-slots (user tz) – overlap using start & end
        ends_at_iso = rid.get("endsAt")
        offer_end_local = None
        if ends_at_iso:
            try:
                offer_end_local = parser.isoparse(ends_at_iso).astimezone(gettz(tz_name))
            except Exception:
                offer_end_local = None

        from .utils import _parse_user_slot_local
        conflict_reason = None
        for slot in booked_slots:
            start_local = _parse_user_slot_local(slot.get("from"), tz_name)
            end_local = _parse_user_slot_local(slot.get("to"), tz_name)
            if not start_local or not end_local:
                continue
            if end_local < start_local:
                start_local, end_local = end_local, start_local

            overlap = False
            if offer_end_local:
                if not (offer_end_local <= start_local or pickup_local >= end_local):
                    overlap = True
            else:
                if start_local <= pickup_local <= end_local:
                    overlap = True

            if overlap:
                slot_name = slot.get("name") or "Sans nom"
                conflict_reason = (
                    f"tombe dans créneau bloqué «{slot_name}» "
                    f"({start_local.strftime('%Y-%m-%d %H:%M')} → {end_local.strftime('%Y-%m-%d %H:%M')})"
                )
                break
        if booked_slots:
            record_result("Créneaux bloqués", conflict_reason is None, conflict_reason)

        # 5.5) Conflict with already accepted offers (busy intervals)
        conflict_with = _find_conflict(pickup, ends_at_iso, accepted_intervals)
        if conflict_with:
            a_start, a_end = conflict_with
            conflict_text = (
                "conflit avec une course acceptée "
                f"({_fmt_dt_local_from_dt(a_start, tz_name)} – {_fmt_dt_local_from_dt(a_end, tz_name)})"
            )
            record_result("Conflit trajets acceptés", False, conflict_text)
        elif accepted_intervals:
            record_result("Conflit trajets acceptés", True, "aucun conflit")

        # --- Final decision based on accumulated filters ---
        failed_filters = [fr for fr in filter_results if not fr["ok"]]
        summary_text = _format_filter_summary(filter_results)
        base_reason = "; ".join([fr["detail"] or fr["name"] for fr in failed_filters]) if failed_filters else None

        forced_accept_reason = None
        if accept_override and failed_filters:
            forced_accept_reason = f"accepté (filtre personnalisé) malgré: {base_reason}"

        is_rejected = bool(failed_filters) and not accept_override
        reason_for_log = forced_accept_reason or base_reason

        if is_rejected:
            print(f"[{datetime.now()}] ⛔ Rejected {oid} – {base_reason or 'filtres non respectés'}")
            try:
                log_offer_decision(bot_id, telegram_id, offer, "rejected", reason_for_log or "filtres non respectés")
            except Exception as e:
                _builtins.print(f"[{datetime.now()}] ⚠️ log_offer_decision failed (rejected {oid}): {type(e).__name__}: {e}")
            if FAST_ACCEPT_MODE:
                # In race mode, skip heavy work; optionally keep a lightweight reject notification.
                if FAST_ACCEPT_NOTIFY_REJECTED:
                    header_line = _build_offer_header_line(offer, "rejected", platform, forced_accept=False)
                    reject_lines = _build_reject_summary_lines(filter_results)
                    notify_text = f"{header_line}\n{reject_lines}" if reject_lines else header_line
                    _queue_notification("rejected", notify_text, platform, reply_markup=None, force_notify=True)
                if OFFER_MEMORY_DEDUPE:
                    rejected_per_user[bot_id][telegram_id][platform].add(oid)
                continue
            full_text = _build_user_message(
                offer,
                "rejected",
                None,
                tz_name,
                summary_text,
                filter_results=filter_results,
                platform=platform,
                forced_accept=False,
            )
            header_line = _build_offer_header_line(offer, "rejected", platform, forced_accept=False)
            reject_lines = _build_reject_summary_lines(filter_results)
            notify_text = f"{header_line}\n{reject_lines}" if reject_lines else header_line
            details_key = uuid.uuid4().hex[:16]
            try:
                save_offer_message(bot_id, telegram_id, details_key, header_line, full_text)
            except Exception as e:
                _builtins.print(f"[{datetime.now()}] ⚠️ save_offer_message failed (rejected {oid}): {type(e).__name__}: {e}")
                details_key = None
            kb = {"inline_keyboard": [[{"text": "Show details", "callback_data": f"show_offer:{details_key}"}]]}
            if details_key is None:
                kb = None
            _queue_notification("rejected", notify_text, platform, reply_markup=kb, force_notify=True)
            if OFFER_MEMORY_DEDUPE:
                rejected_per_user[bot_id][telegram_id][platform].add(oid)
            continue

        # Accept (either all filters OK or overridden by custom filter)
        print(f"[{datetime.now()}] ✅ Accepted {oid} [{platform}]")
        offer_to_log = deepcopy(offer)

        # Optionally auto-reserve the offer upstream
        if AUTO_RESERVE_ENABLED:
            reserve_attempted = False
            reserve_ok = True
            reserve_reason = None
            reserve_reason_user = None
            latency_ms = _poll_latency_ms(offer_to_log)
            latency_note = f" | latency={latency_ms}ms" if latency_ms is not None else ""
            _builtins.print(
                f"[{datetime.now()}] 🧾 Reserve check {oid} platform={platform} "
                f"p1_token={'yes' if p1_token else 'no'} "
                f"p2_token={'yes' if p2_token else 'no'} "
                f"price={offer_to_log.get('price')}"
            )
            try:
                if platform == "p1":
                    if p1_token:
                        reserve_attempted = True
                        rs, rb = _reserve_with_timeout(
                            reserve_offer_p1,
                            P1_RESERVE_TIMEOUT_S,
                            p1_token,
                            oid,
                            price=offer_to_log.get("price"),
                            headers=p1_headers,
                        )
                        if rs in (401, 403):
                            new_tok, new_headers, refreshed, note = maybe_refresh_p1_session(
                                bot_id=bot_id,
                                telegram_id=telegram_id,
                                token=p1_token,
                                mobile_headers=p1_headers,
                                force=True,
                                trigger="p1_reserve_unauthorized",
                            )
                            if refreshed and new_tok:
                                p1_token = new_tok
                                p1_headers = new_headers
                                _builtins.print(
                                    f"[{datetime.now()}] 🔁 P1 token refreshed during reserve retry for user {telegram_id}"
                                )
                                rs, rb = _reserve_with_timeout(
                                    reserve_offer_p1,
                                    P1_RESERVE_TIMEOUT_S,
                                    p1_token,
                                    oid,
                                    price=offer_to_log.get("price"),
                                    headers=p1_headers,
                                )
                            else:
                                _builtins.print(
                                    f"[{datetime.now()}] ⚠️ P1 refresh unavailable during reserve for user {telegram_id}: {note}"
                                )
                        _builtins.print(f"[{datetime.now()}] 🎯 P1 reserve {oid} -> {rs} | {rb}{latency_note}")
                        reserve_ok = 200 <= (rs or 0) < 300
                        if not reserve_ok:
                            if rs == 401:
                                set_token_status(bot_id, telegram_id, "expired")
                            _builtins.print(
                                f"[{datetime.now()}] ❌ P1 reserve failed {oid} (status={rs}) body={rb}{latency_note}"
                            )
                            reserve_reason = f"reserve_failed:{rs}"
                            reserve_reason_user = _reserve_failure_human_reason(rs, rb)
                        else:
                            _refresh_rides_cache_async(
                                bot_id,
                                telegram_id,
                                tz_name,
                                p1_token,
                                p1_headers,
                                p2_token,
                            )
                    else:
                        _builtins.print(f"[{datetime.now()}] ⚠️ P1 reserve skipped (no token) for user {telegram_id}")
                else:  # p2
                    if p2_token:
                        bid_price = offer_to_log.get("price")
                        if bid_price is None:
                            _builtins.print(f"[{datetime.now()}] ⚠️ P2 reserve skipped (no price) for {oid}")
                        else:
                            reserve_attempted = True
                            rs, rb = _reserve_with_timeout(
                                reserve_offer_p2,
                                P2_RESERVE_TIMEOUT_S,
                                p2_token,
                                oid,
                                float(bid_price),
                                bl_user_id=get_bl_uuid(bot_id, telegram_id),
                            )
                            _builtins.print(f"[{datetime.now()}] 🎯 P2 reserve {oid} -> {rs} | {rb}{latency_note}")
                        reserve_ok = 200 <= (rs or 0) < 300
                        if not reserve_ok:
                            _builtins.print(f"[{datetime.now()}] ❌ P2 reserve failed {oid} (status={rs}) body={rb}{latency_note}")
                            reserve_reason = f"reserve_failed:{rs}"
                            reserve_reason_user = _reserve_failure_human_reason(rs, rb)
                        else:
                            _refresh_rides_cache_async(
                                bot_id,
                                telegram_id,
                                tz_name,
                                p1_token,
                                p1_headers,
                                p2_token,
                            )
                    else:
                        _builtins.print(f"[{datetime.now()}] ⚠️ P2 reserve skipped (no portal token) for user {telegram_id}")
            except Exception as e:
                _builtins.print(f"[{datetime.now()}] ❌ Auto-reserve error for {oid}: {type(e).__name__}: {e}{latency_note}")
                reserve_attempted = True
                reserve_ok = False
                reserve_reason = f"reserve_error:{type(e).__name__}"
                reserve_reason_user = _reserve_failure_human_reason(None, {"error": f"{type(e).__name__}: {e}"})

        final_status = "accepted"
        if AUTO_RESERVE_ENABLED and reserve_attempted and not reserve_ok:
            final_status = "not_accepted"
        final_reason = reserve_reason_user if final_status == "not_accepted" else None
        final_reason_for_log = reserve_reason if final_status == "not_accepted" else reason_for_log
        try:
            log_offer_decision(bot_id, telegram_id, offer_to_log, final_status, final_reason_for_log)
        except Exception as e:
            _builtins.print(f"[{datetime.now()}] ⚠️ log_offer_decision failed ({final_status} {oid}): {type(e).__name__}: {e}")

        full_text = _build_user_message(
            offer_to_log,
            final_status,
            final_reason,
            tz_name,
            summary_text,
            filter_results=filter_results,
            platform=platform,
            forced_accept=bool(accept_override and failed_filters),
        )
        header_line = _build_offer_header_line(
            offer_to_log,
            final_status,
            platform,
            forced_accept=bool(accept_override and failed_filters),
        )
        notify_line = header_line
        if final_status == "not_accepted" and final_reason:
            notify_line = f"{header_line}\n⚠️ {_esc(final_reason)}"
        details_key = uuid.uuid4().hex[:16]
        try:
            save_offer_message(bot_id, telegram_id, details_key, header_line, full_text)
        except Exception as e:
            _builtins.print(f"[{datetime.now()}] ⚠️ save_offer_message failed ({final_status} {oid}): {type(e).__name__}: {e}")
            details_key = None
        kb = {"inline_keyboard": [[{"text": "Show details", "callback_data": f"show_offer:{details_key}"}]]}
        if details_key is None:
            kb = None
        _queue_notification(
            final_status,
            notify_line,
            platform,
            reply_markup=kb,
            force_notify=(final_status in ("rejected", "not_accepted")),
        )
        if OFFER_MEMORY_DEDUPE:
            if final_status == "accepted":
                accepted_per_user[bot_id][telegram_id].add(oid)
            else:
                # Keep a short-lived per-platform backoff, but do not globally lock
                # the offer id as "accepted" after a failed reserve.
                rejected_per_user[bot_id][telegram_id][platform].add(oid)

        try:
            new_end_dt = parser.isoparse(offer_to_log["rides"][0].get("endsAt")) if offer_to_log["rides"][0].get("endsAt") else None
        except Exception:
            new_end_dt = None
        accepted_intervals.append((pickup, new_end_dt))

    for kind, text, platform_name, reply_markup, force_notify in pending_notifications:
        _send_notification_async(
            bot_id,
            telegram_id,
            kind,
            text,
            platform_name,
            reply_markup=reply_markup,
            force_notify=force_notify,
        )
