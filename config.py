
"""
config.py — Environment variable configuration.

REQUIRED
────────
SAMSARA_API_TOKEN       Samsara fleet API token
TELEGRAM_BOT_TOKEN      Telegram bot token from @BotFather
DISPATCHER_GROUP_ID     Telegram group ID that receives all fuel alerts
ADMIN_CHAT_ID           Your personal Telegram user ID (for admin commands)
DATABASE_URL            PostgreSQL connection string (auto-set by Railway)

OPTIONAL
────────
FUEL_ALERT_THRESHOLD_PCT    Fuel % that triggers alert         default: 35
DEFAULT_TANK_GAL            Default truck tank size (gallons)  default: 150
DEFAULT_MPG                 Default truck MPG                  default: 6.5
SAFETY_RESERVE              Fraction of tank kept as reserve   default: 0.10
VISIT_RADIUS_MILES          Radius to detect truck at stop     default: 0.35
MIN_SAVINGS_DISPLAY         Min $ savings to show alt stop     default: 3.0
BEHIND_PENALTY_MILES        Score penalty for stops behind     default: 15

POLLING INTERVALS (minutes)
────────────────────────────
POLL_INTERVAL_HEALTHY           default: 60
POLL_INTERVAL_WATCH             default: 20
POLL_INTERVAL_CRITICAL_MOVING   default: 10
POLL_INTERVAL_CRITICAL_PARKED   default: 60

CALIFORNIA BORDER REMINDER
──────────────────────────
CA_BORDER_REMINDER_MILES    Distance from CA to send reminder  default: 150
CA_BORDER_FUEL_THRESHOLD    Fuel % threshold to trigger        default: 70

YARDS (up to 20)
─────────────────
YARD_1=Main Yard:28.4277:-81.3816:0.5
YARD_2=Second Yard:29.0000:-82.0000:0.5
Format: Name:latitude:longitude:radius_miles
"""

import os
import sys
# Environment variables set directly in Railway dashboard


# ── Helpers ──────────────────────────────────────────────────────────────────

def _require(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        print(f"[config] FATAL: Missing required env var: {key}", flush=True)
        sys.exit(1)
    return val

def _int(key: str, default: int) -> int:
    val = os.getenv(key)
    if val is None:
        return default
    try:
        return int(val.strip())
    except (ValueError, TypeError):
        print(f"[config] WARNING: Invalid int for {key}={val!r}, using default {default}", flush=True)
        return default

def _float(key: str, default: float) -> float:
    val = os.getenv(key)
    if val is None:
        return default
    try:
        return float(val.strip())
    except (ValueError, TypeError):
        print(f"[config] WARNING: Invalid float for {key}={val!r}, using default {default}", flush=True)
        return default


# ── Required ─────────────────────────────────────────────────────────────────

SAMSARA_API_TOKEN   = _require("SAMSARA_API_TOKEN")
SAMSARA_BASE_URL    = "https://api.samsara.com"

# QuickManage TMS — OAuth2 credentials
QM_CLIENT_ID     = os.getenv("QM_CLIENT_ID", "")
QM_CLIENT_SECRET = os.getenv("QM_CLIENT_SECRET", "")

TELEGRAM_BOT_TOKEN  = _require("TELEGRAM_BOT_TOKEN")
DISPATCHER_GROUP_ID = _require("DISPATCHER_GROUP_ID")
ADMIN_CHAT_ID       = _require("ADMIN_CHAT_ID")
DATABASE_URL        = _require("DATABASE_URL")


# ── Fuel alerting ─────────────────────────────────────────────────────────────

FUEL_ALERT_THRESHOLD_PCT = _float("FUEL_ALERT_THRESHOLD_PCT", 35.0)


# ── Truck defaults ────────────────────────────────────────────────────────────

DEFAULT_TANK_GAL = _float("DEFAULT_TANK_GAL", 150.0)
DEFAULT_MPG      = _float("DEFAULT_MPG",      6.5)
SAFETY_RESERVE   = _float("SAFETY_RESERVE",   0.10)


# ── Fuel stop search ──────────────────────────────────────────────────────────

BEHIND_PENALTY_MILES  = _float("BEHIND_PENALTY_MILES",  15.0)
MIN_SAVINGS_DISPLAY   = _float("MIN_SAVINGS_DISPLAY",   3.0)
VISIT_RADIUS_MILES    = _float("VISIT_RADIUS_MILES",    0.35)

# Legacy — kept for import compatibility
SEARCH_CORRIDOR_MILES = _float("SEARCH_CORRIDOR_MILES", 50.0)
CORRIDOR_WIDTH_MILES  = _float("CORRIDOR_WIDTH_MILES",  8.0)


# ── Polling intervals (minutes) ───────────────────────────────────────────────

POLL_INTERVAL_HEALTHY         = _int("POLL_INTERVAL_HEALTHY",         60)
POLL_INTERVAL_WATCH           = _int("POLL_INTERVAL_WATCH",           20)
POLL_INTERVAL_CRITICAL_MOVING = _int("POLL_INTERVAL_CRITICAL_MOVING", 10)
POLL_INTERVAL_CRITICAL_PARKED = _int("POLL_INTERVAL_CRITICAL_PARKED", 60)


# ── State persistence ─────────────────────────────────────────────────────────

STATE_SAVE_INTERVAL_SECONDS = _int("STATE_SAVE_INTERVAL_SECONDS", 300)


# ── California border reminder ────────────────────────────────────────────────

CA_BORDER_REMINDER_MILES = _float("CA_BORDER_REMINDER_MILES", 150.0)
CA_BORDER_FUEL_THRESHOLD = _float("CA_BORDER_FUEL_THRESHOLD", 70.0)

# IFTA home state — set this to your fleet's base state (e.g. "FL", "IN", "TX", "OH")
# Leave blank to disable IFTA-adjusted pricing
IFTA_HOME_STATE = os.environ.get("IFTA_HOME_STATE", "").upper().strip() or None


# ── Yards ─────────────────────────────────────────────────────────────────────

YARDS = []
for _i in range(1, 21):
    _val = os.getenv(f"YARD_{_i}", "").strip()
    if not _val:
        continue
    _parts = _val.split(":")
    if len(_parts) != 4:
        print(f"[config] WARNING: YARD_{_i} invalid format — expected Name:lat:lng:radius", flush=True)
        continue
    try:
        YARDS.append({
            "name":         _parts[0].strip(),
            "lat":          float(_parts[1]),
            "lng":          float(_parts[2]),
            "radius_miles": float(_parts[3]),
        })
    except ValueError:
        print(f"[config] WARNING: YARD_{_i} has invalid coordinates: {_val}", flush=True)
