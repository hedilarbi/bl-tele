import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()  # reads .env in project root


def _ensure_https_base(url: str) -> str:
    """
    Telegram WebApp buttons only accept HTTPS URLs.
    Coerce any http/relative base into https and strip trailing slash.
    """
    url = (url or "").strip().rstrip("/")
    if not url:
        return url
    if url.lower().startswith("http://"):
        return "https://" + url[7:]
    if not url.lower().startswith("https://"):
        return "https://" + url
    return url


def _with_bot_id(url: str, bot_id: str | None, as_user: int | None = None) -> str:
    qs = {}
    if bot_id:
        qs["bot_id"] = str(bot_id)
    if as_user is not None:
        qs["as_user"] = str(as_user)
    if not qs:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{urllib.parse.urlencode(qs)}"


ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "").strip()
ADMIN_BOT_ID = os.getenv("ADMIN_BOT_ID", "").strip()
ADMIN_BOT_NAME = os.getenv("ADMIN_BOT_NAME", "").strip()
BOT_REFRESH_INTERVAL_S = int(os.getenv("BOT_REFRESH_INTERVAL_S", "20"))
MINI_APP_BASE = _ensure_https_base(os.getenv("MINI_APP_BASE", "http://localhost:3000"))
BOOKED_SLOTS_URL = f"{MINI_APP_BASE}/booked-slots"
SCHEDULE_URL = f"{MINI_APP_BASE}/schedule"
CURRENT_SCHEDULE_URL = f"{MINI_APP_BASE}/current-schedule"
BL_ACCOUNT_URL = f"{MINI_APP_BASE}/bl-account"

API_HOST = os.getenv("API_HOST", "https://chauffeur-app-api.blacklane.com")

PORTAL_CLIENT_ID = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
PORTAL_AUTH_BASE = os.getenv("PORTAL_AUTH_BASE", "https://athena.blacklane.com")
PARTNER_PORTAL_API = os.getenv("PARTNER_PORTAL_API", "https://partner-portal-api.blacklane.com")
P1_API_BASE = os.getenv("API_HOST", "https://chauffeur-app-api.blacklane.com")
