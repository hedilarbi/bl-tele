import time
from typing import Optional

from .storage import _get_mobile_token
from .portal import _athena_login, _portal_get_me, _p1_get_me_profile, _portal_token_expired
from db import get_bl_uuid, get_bl_account_full, get_portal_token, update_portal_token, set_bl_uuid

_UUID_ATTEMPT_COOLDOWN_S = 3600  # avoid hammering: try at most once/hour per user
_last_uuid_attempt: dict[tuple[str, int], float] = {}


def _try_update_bl_uuid(bot_id: Optional[str], user_id: int):
    if not bot_id:
        return
    # debounce
    now = time.time()
    key = (bot_id, int(user_id))
    last = _last_uuid_attempt.get(key, 0)
    if now - last < _UUID_ATTEMPT_COOLDOWN_S:
        return
    _last_uuid_attempt[key] = now

    # already saved?
    try:
        if get_bl_uuid(bot_id, user_id):
            return
    except Exception:
        pass

    # 1) Prefer Partner Portal (/me) if we have BL email+password
    try:
        creds = get_bl_account_full(bot_id, user_id)  # returns (email, password) or (None, None)
    except Exception:
        creds = (None, None)

    email, password = (creds or (None, None))
    if email and password:
        # ensure portal token
        ptoken = get_portal_token(bot_id, user_id)
        if _portal_token_expired(ptoken):
            ok, new_tok, note = _athena_login(email, password)
            if ok and new_tok:
                update_portal_token(bot_id, user_id, new_tok)
                ptoken = new_tok
            else:
                ptoken = None  # fallback to P1 below
        if ptoken:
            status, payload = _portal_get_me(ptoken)
            if status == 401 or status == 403:
                # try one re-login
                ok, new_tok, note = _athena_login(email, password)
                if ok and new_tok:
                    update_portal_token(bot_id, user_id, new_tok)
                    status, payload = _portal_get_me(new_tok)
            if status and 200 <= status < 300 and isinstance(payload, dict):
                bl_id = payload.get("id")
                if isinstance(bl_id, str) and bl_id.strip():
                    set_bl_uuid(bot_id, user_id, bl_id.strip())
                    return  # done

    # 2) Fallback to Mobile API (/api/v1/me/profile)
    token = _get_mobile_token(bot_id, user_id)
    if token:
        status, payload = _p1_get_me_profile(token)
        if status and 200 <= status < 300 and isinstance(payload, dict):
            # Prefer 'uuid' if present; else try common alternates
            bl_id = payload.get("uuid") or payload.get("id") or payload.get("chauffeur_id")
            if isinstance(bl_id, str) and bl_id.strip():
                set_bl_uuid(bot_id, user_id, bl_id.strip())
                return
