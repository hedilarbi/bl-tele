from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List
import threading
import time


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

inmem_lock = threading.Lock()

# Athena token/etag helpers
_athena_offers_etag: Dict[Tuple[str, int], Optional[str]] = {}  # (bot_id, telegram_id) -> etag (offers)
_filters_cache: Dict[Tuple[str, int], Dict[str, object]] = {}

# In-memory dedupe for accepted per bot/user and rejected per bot/user/platform.
accepted_per_user = defaultdict(lambda: defaultdict(set))
rejected_per_user = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
_ACCEPTED_RESET_INTERVAL = timedelta(hours=24)
_REJECTED_RESET_INTERVAL = timedelta(minutes=1)
_accepted_last_reset = datetime.now(timezone.utc)
_rejected_last_reset = datetime.now(timezone.utc)

_rides_cache: Dict[Tuple[str, int], Dict[str, object]] = {}


def get_rides_cache(bot_id: str, telegram_id: int):
    entry = _rides_cache.get((bot_id, telegram_id))
    if not entry:
        return None, None
    return entry.get("intervals"), entry.get("ts")


def get_offers_etag(bot_id: str, telegram_id: int) -> Optional[str]:
    return _athena_offers_etag.get((bot_id, telegram_id))


def set_offers_etag(bot_id: str, telegram_id: int, etag: Optional[str]) -> None:
    if etag:
        _athena_offers_etag[(bot_id, telegram_id)] = etag


def get_filters_cache(bot_id: str, telegram_id: int):
    return _filters_cache.get((bot_id, telegram_id))


def set_filters_cache(bot_id: str, telegram_id: int, key: str, filters: dict, ts: Optional[float] = None):
    _filters_cache[(bot_id, telegram_id)] = {
        "key": key,
        "filters": filters,
        "ts": time.time() if ts is None else ts,
    }


def set_rides_cache(bot_id: str, telegram_id: int, intervals: List[Tuple[datetime, Optional[datetime]]], ts: Optional[float] = None):
    _rides_cache[(bot_id, telegram_id)] = {
        "intervals": intervals or [],
        "ts": time.time() if ts is None else ts,
    }


def invalidate_rides_cache(bot_id: str, telegram_id: int):
    _rides_cache.pop((bot_id, telegram_id), None)


def maybe_reset_inmem_caches():
    """Clear accepted daily; clear rejected every minute."""
    global _accepted_last_reset, _rejected_last_reset
    now = datetime.now(timezone.utc)
    if now - _accepted_last_reset >= _ACCEPTED_RESET_INTERVAL:
        accepted_per_user.clear()
        _accepted_last_reset = now
        print(f"[{datetime.now()}] 🔁 Cleared in-memory accept cache (24h rotation)")
    if now - _rejected_last_reset >= _REJECTED_RESET_INTERVAL:
        rejected_per_user.clear()
        _rejected_last_reset = now
