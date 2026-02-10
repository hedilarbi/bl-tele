import os
from dotenv import load_dotenv

load_dotenv()  # reads .env in project root

# -------- Config --------
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_HOST = "https://chauffeur-app-api.blacklane.com"  # Platform 1 (mobile)
ATHENA_BASE = "https://athena.blacklane.com"          # Platform 2 (Portal)
PARTNER_API_BASE = "https://partner-portal-api.blacklane.com"
PORTAL_CLIENT_ID = os.getenv("BL_PORTAL_CLIENT_ID", "7qL5jGGai6MqBCatVeoihQx5dKEhrNCh")
PORTAL_PAGE_SIZE = 50

POLL_INTERVAL = 0.5
MAX_WORKERS = 10
RIDES_REFRESH_INTERVAL_S = int(os.getenv("RIDES_REFRESH_INTERVAL_S", "86400"))

# Filter cache (seconds)
FILTERS_CACHE_TTL_S = int(os.getenv("FILTERS_CACHE_TTL_S", "15"))

# Burst polling (seconds)
BURST_POLL_INTERVAL_S = float(os.getenv("BURST_POLL_INTERVAL_S", "0.3"))
BURST_DURATION_S = float(os.getenv("BURST_DURATION_S", "6"))

# HTTP timeouts (seconds)
P1_POLL_TIMEOUT_S = int(os.getenv("P1_POLL_TIMEOUT_S", "8"))
P1_RESERVE_TIMEOUT_S = int(os.getenv("P1_RESERVE_TIMEOUT_S", "8"))
P2_POLL_TIMEOUT_S = int(os.getenv("P2_POLL_TIMEOUT_S", "8"))
P2_RESERVE_TIMEOUT_S = int(os.getenv("P2_RESERVE_TIMEOUT_S", "8"))

# Toggle mock data for development (default: real polling)
USE_MOCK_P1 = False    
USE_MOCK_P2 = False     
ENABLE_P1 = True
ALWAYS_POLL_REAL_ORDERS = True  # always poll real /rides (both platforms when available)
# When enabled, accepted offers will be actually reserved via API calls (P1/P2).
AUTO_RESERVE_ENABLED = True

# Diagnostics
DEBUG_PRINT_OFFERS = False   # print raw offers
CF_DEBUG = False             # custom filters debug
ATHENA_PRINT_DEBUG = True   # print portal token and raw payloads
DEBUG_ENDS = False           # log endsAt math for each offer
APPLY_GAP_TO_BUSY_INTERVALS = False  # gap will NOT extend busy intervals
LOG_OFFERS_PAYLOAD = os.getenv("LOG_OFFERS_PAYLOAD", "0") == "1"
LOG_RAW_API_RESPONSES = os.getenv("LOG_RAW_API_RESPONSES", "0") == "1"
MAX_LOGGED_OFFERS = int(os.getenv("MAX_LOGGED_OFFERS", "3"))

# --- Rides visibility ---
DUMP_RIDES_IN_LOGS = True         # print polled rides to stdout
DUMP_RIDES_IN_TELEGRAM = False    # also send a compact snapshot to the user
MAX_RIDES_SHOWN = 20              # cap to avoid spam

# Athena token/etag helpers
ATHENA_RELOGIN_SKEW_S = int(os.getenv("ATHENA_RELOGIN_SKEW_S", "3600"))
