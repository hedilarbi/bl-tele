"""
PROVISIONAL TEST — delete after use.
Tests GET /rides (P1) and _filter_rides_by_bl_uuid for a given user.
Usage: python test_rides_filter.py
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from db import get_all_users_with_bot_admin_active, get_mobile_headers, get_bl_uuid
from poller_core.p1_client import get_rides_p1
from poller_core.p2_client import _filter_rides_by_bl_uuid


def _short(s, n=60):
    s = str(s or "")
    return s[:n] + "..." if len(s) > n else s


def test_user(bot_id, telegram_id, token):
    print(f"\n{'='*60}")
    print(f"USER: bot_id={bot_id}  telegram_id={telegram_id}")
    print(f"TOKEN: {_short(token, 40)}")

    mobile_headers = get_mobile_headers(bot_id, telegram_id)
    bl_uuid = get_bl_uuid(bot_id, telegram_id)
    print(f"BL_UUID: {bl_uuid}")

    print("\n--- GET /rides (raw) ---")
    status_code, rides_raw = get_rides_p1(token, headers=mobile_headers)
    print(f"Status: {status_code}")

    if status_code != 200 or not isinstance(rides_raw, list):
        print(f"Error or unexpected response: {rides_raw}")
        return

    print(f"Total rides returned by API: {len(rides_raw)}")

    # Show chauffeur IDs present in raw response
    ids_seen = set()
    for r in rides_raw:
        ch = (r or {}).get("chauffeur") or {}
        cid = ch.get("id")
        name = ch.get("name") or ch.get("displayName") or ""
        if cid:
            ids_seen.add((str(cid), name))

    if ids_seen:
        print("Chauffeur IDs found in raw /rides:")
        for cid, cname in sorted(ids_seen):
            print(f"  id={cid}  name={_short(cname)}")
    else:
        print("No chauffeur.id found in any ride item.")

    print(f"\n--- After _filter_rides_by_bl_uuid(bl_uuid={bl_uuid}) ---")
    if not bl_uuid:
        print("WARNING: bl_uuid is None/empty — filter will return [] (nothing filtered in)")
        kept = []
    else:
        kept = _filter_rides_by_bl_uuid(rides_raw, bl_uuid)

    print(f"Rides kept after filter: {len(kept)} / {len(rides_raw)}")

    for i, r in enumerate(kept):
        ch = (r or {}).get("chauffeur") or {}
        rides_list = r.get("rides") or []
        ride0 = rides_list[0] if rides_list else {}
        pickup = ride0.get("pickupTime") or r.get("pickupTime") or "?"
        otype = (ride0.get("type") or r.get("type") or "?").lower()
        rid = r.get("id") or "?"
        print(f"  [{i+1}] id={rid}  type={otype}  pickup={pickup}  chauffeur_id={ch.get('id')}")


def main():
    users = get_all_users_with_bot_admin_active()
    if not users:
        print("No active users found in DB.")
        return

    print(f"Found {len(users)} active user(s).")
    for user in users:
        bot_id = user[0]
        telegram_id = user[1]
        token = user[2]
        if not token or not str(token).strip():
            print(f"\nUSER bot_id={bot_id} telegram_id={telegram_id}: no token, skipping.")
            continue
        test_user(bot_id, telegram_id, token)


if __name__ == "__main__":
    main()
