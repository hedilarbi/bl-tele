from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
import threading


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

inmem_lock = threading.Lock()

# Athena token/etag helpers
_athena_offers_etag: Dict[Tuple[str, int], Optional[str]] = {}  # (bot_id, telegram_id) -> etag (offers)

# In-memory dedupe for accepted per bot/user and rejected per bot/user/platform.
accepted_per_user = defaultdict(lambda: defaultdict(set))
rejected_per_user = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
_ACCEPTED_RESET_INTERVAL = timedelta(hours=24)
_REJECTED_RESET_INTERVAL = timedelta(minutes=1)
_accepted_last_reset = datetime.now(timezone.utc)
_rejected_last_reset = datetime.now(timezone.utc)


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
