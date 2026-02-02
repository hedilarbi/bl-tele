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

POLL_INTERVAL = 1
MAX_WORKERS = 10
RIDES_REFRESH_INTERVAL_S = int(os.getenv("RIDES_REFRESH_INTERVAL_S", "86400"))

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

# --- Rides visibility ---
DUMP_RIDES_IN_LOGS = True         # print polled rides to stdout
DUMP_RIDES_IN_TELEGRAM = False    # also send a compact snapshot to the user
MAX_RIDES_SHOWN = 20              # cap to avoid spam

# Athena token/etag helpers
ATHENA_RELOGIN_SKEW_S = int(os.getenv("ATHENA_RELOGIN_SKEW_S", "3600"))
