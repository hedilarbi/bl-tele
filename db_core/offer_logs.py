import sqlite3
import json as _json

from .config import DB_FILE


def log_offer_decision(bot_id: str, telegram_id: int, offer: dict, status: str, reason: str = None):
    rid = (offer.get("rides") or [{}])[0] if offer else {}

    offer_id = offer.get("id")
    otype = (rid.get("type") or "")
    vehicle_cl = (offer.get("vehicleClass") or "")
    price = offer.get("price")
    currency = offer.get("currency")
    pickup = rid.get("pickupTime")
    ends_at = rid.get("endsAt")

    pu_addr = ((rid.get("pickUpLocation") or {}).get("address")) if rid else None
    do_addr = ((rid.get("dropOffLocation") or {}).get("address")) if rid else None

    duration = rid.get("estimatedDurationMinutes") or rid.get("durationMinutes")
    est_dist = rid.get("estimatedDistanceMeters")
    km_incl = rid.get("kmIncluded")

    guest_raw = rid.get("guestRequests")
    if isinstance(guest_raw, (list, tuple)):
        guest_requests = ", ".join([str(x) for x in guest_raw if str(x).strip()])
    elif isinstance(guest_raw, dict):
        try:
            guest_requests = _json.dumps(guest_raw, ensure_ascii=False)
        except Exception:
            guest_requests = str(guest_raw)
    else:
        guest_requests = guest_raw if guest_raw is not None else None
    flight_number = (rid.get("flight") or {}).get("number") if isinstance(rid.get("flight"), dict) else None
    if not flight_number:
        flight_number = rid.get("flight_number")

    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute("PRAGMA busy_timeout=5000")
    c.execute(
        """
        INSERT INTO offer_logs (
            bot_id, telegram_id, offer_id, status, type, vehicle_class, price, currency,
            pickup_time, ends_at, pu_address, do_address, estimated_distance_meters,
            duration_minutes, km_included, guest_requests, flight_number,
            rejection_reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(bot_id, telegram_id, offer_id) DO UPDATE SET
            status = excluded.status,
            type = excluded.type,
            vehicle_class = excluded.vehicle_class,
            price = excluded.price,
            currency = excluded.currency,
            pickup_time = excluded.pickup_time,
            ends_at = excluded.ends_at,
            pu_address = excluded.pu_address,
            do_address = excluded.do_address,
            estimated_distance_meters = excluded.estimated_distance_meters,
            duration_minutes = excluded.duration_minutes,
            km_included = excluded.km_included,
            guest_requests = excluded.guest_requests,
            flight_number = excluded.flight_number,
            rejection_reason = excluded.rejection_reason,
            created_at = CURRENT_TIMESTAMP
    """,
        (
            bot_id,
            telegram_id,
            offer_id,
            status,
            otype,
            vehicle_cl,
            price,
            currency,
            pickup,
            ends_at,
            pu_addr,
            do_addr,
            est_dist,
            duration,
            km_incl,
            guest_requests,
            flight_number,
            reason,
        ),
    )
    conn.commit()
    conn.close()


def get_processed_offer_ids(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute("SELECT offer_id FROM offer_logs WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    rows = c.fetchall()
    conn.close()
    return {r[0] for r in rows}


def get_offer_logs(bot_id: str, telegram_id: int, limit: int = 10, offset: int = 0):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute(
        """
        SELECT offer_id, status, type, vehicle_class, price, currency, pickup_time, ends_at,
               pu_address, do_address, estimated_distance_meters, duration_minutes, km_included,
               guest_requests, flight_number,
               rejection_reason, created_at
        FROM offer_logs
        WHERE bot_id = ? AND telegram_id = ?
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ? OFFSET ?
    """,
        (bot_id, telegram_id, limit, offset),
    )
    rows = c.fetchall()
    conn.close()
    results = []
    for r in rows:
        results.append(
            {
                "offer_id": r[0],
                "status": r[1],
                "type": r[2],
                "vehicle_class": r[3],
                "price": r[4],
                "currency": r[5],
                "pickup_time": r[6],
                "ends_at": r[7],
                "pu_address": r[8],
                "do_address": r[9],
                "estimated_distance_meters": r[10],
                "duration_minutes": r[11],
                "km_included": r[12],
                "guest_requests": r[13],
                "flight_number": r[14],
                "rejection_reason": r[15],
                "created_at": r[16],
            }
        )
    return results


def get_offer_logs_counts(bot_id: str, telegram_id: int):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM offer_logs WHERE bot_id = ? AND telegram_id = ?", (bot_id, telegram_id))
    total = c.fetchone()[0] or 0
    c.execute(
        "SELECT COUNT(*) FROM offer_logs WHERE bot_id = ? AND telegram_id = ? AND status = 'accepted'",
        (bot_id, telegram_id),
    )
    accepted = c.fetchone()[0] or 0
    c.execute(
        "SELECT COUNT(*) FROM offer_logs WHERE bot_id = ? AND telegram_id = ? AND status = 'rejected'",
        (bot_id, telegram_id),
    )
    rejected = c.fetchone()[0] or 0
    c.execute(
        "SELECT COUNT(*) FROM offer_logs WHERE bot_id = ? AND telegram_id = ? AND status = 'not_accepted'",
        (bot_id, telegram_id),
    )
    not_accepted = c.fetchone()[0] or 0
    conn.close()
    return {"total": total, "accepted": accepted, "rejected": rejected, "not_accepted": not_accepted}


def get_offer_stats(
    bot_id: str,
    telegram_id: int,
    start_utc: str | None = None,
    end_utc: str | None = None,
):
    """
    Aggregate stats for offers in an optional UTC time window.
    start_utc/end_utc should be 'YYYY-MM-DD HH:MM:SS' in UTC.
    """
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    query = (
        "SELECT status, type, vehicle_class, price, currency "
        "FROM offer_logs WHERE bot_id = ? AND telegram_id = ?"
    )
    params = [bot_id, telegram_id]
    if start_utc:
        query += " AND datetime(created_at) >= datetime(?)"
        params.append(start_utc)
    if end_utc:
        query += " AND datetime(created_at) < datetime(?)"
        params.append(end_utc)
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    stats = {
        "total": 0,
        "accepted": 0,
        "rejected": 0,
        "not_accepted": 0,
        "accepted_amount": 0.0,
        "accepted_currency": None,
        "type_counts": {},
        "class_counts": {},
    }

    def _norm_type(t: str | None) -> str:
        val = (t or "").strip().lower()
        return val if val in ("transfer", "hourly") else (val or "other")

    def _norm_class(cname: str | None) -> str:
        val = (cname or "").strip().lower()
        mapping = {
            "business": "Business",
            "van": "Van",
            "suv": "Suv",
            "first": "First",
            "sprinter": "Sprinter",
            "electric": "Electric",
        }
        return mapping.get(val, cname or "—")

    for status, otype, vclass, price, currency in rows:
        stats["total"] += 1
        if status == "accepted":
            stats["accepted"] += 1
            try:
                stats["accepted_amount"] += float(price or 0)
            except Exception:
                pass
            if currency:
                if stats["accepted_currency"] is None:
                    stats["accepted_currency"] = currency
                elif stats["accepted_currency"] != currency:
                    stats["accepted_currency"] = "multi"
        elif status == "not_accepted":
            stats["not_accepted"] += 1
        else:
            stats["rejected"] += 1

        tkey = _norm_type(otype)
        stats["type_counts"][tkey] = stats["type_counts"].get(tkey, 0) + 1

        ckey = _norm_class(vclass)
        stats["class_counts"][ckey] = stats["class_counts"].get(ckey, 0) + 1

    return stats
