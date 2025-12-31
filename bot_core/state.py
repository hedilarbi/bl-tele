from typing import Optional
from telegram.ext import ContextTypes

user_waiting_input = {}
adding_slot_step = {}
work_schedule_state = {}  # holds partial schedule input across two steps

FIELD_MAPPING = {
    "change_price_min": "price_min",
    "change_price_max": "price_max",
    "change_work_start": "work_start",   # kept for backward compatibility (unused in UI)
    "change_work_end": "work_end",       # kept for backward compatibility (unused in UI)
    "change_gap": "gap",
    "change_min_duration": "min_duration",
    "min_km": "min_km",
    "max_km": "max_km",
}


def _ctx_bot_id(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    try:
        return (context.application.bot_data or {}).get("bot_id")
    except Exception:
        return None


def _state_key(bot_id: Optional[str], user_id: int) -> tuple[str, int]:
    return (bot_id or "", int(user_id))
