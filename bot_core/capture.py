import threading
from typing import Optional
from telegram import Update

from .identity import _try_update_bl_uuid
from db import upsert_user_from_bot


def _capture_from_update(update: Update, bot_id: Optional[str] = None):
    try:
        u = update.effective_user
        c = update.effective_chat
        if not u:
            return
        # Prefer native dicts when available
        user_d = u.to_dict() if hasattr(u, "to_dict") else {
            "id": u.id,
            "first_name": getattr(u, "first_name", None),
            "last_name": getattr(u, "last_name", None),
            "username": getattr(u, "username", None),
            "language_code": getattr(u, "language_code", None),
            "is_premium": getattr(u, "is_premium", None),
        }
        chat_d = c.to_dict() if (c and hasattr(c, "to_dict")) else (
            {"id": c.id, "type": c.type, "title": getattr(c, "title", None)} if c else {}
        )
        if not bot_id:
            return
        upsert_user_from_bot(bot_id, user_d, chat_d)
        try:
            threading.Thread(target=_try_update_bl_uuid, args=(bot_id, u.id), daemon=True).start()
        except Exception:
            pass
    except Exception:
        # don't interrupt UX if logging fails
        pass
