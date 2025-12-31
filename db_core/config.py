import os

_BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_FILE = os.getenv("DB_FILE") or os.path.join(_BASE_DIR, "users.db")

VEHICLE_CLASSES = ["SUV", "VAN", "Business", "First", "Electric", "Sprinter"]
