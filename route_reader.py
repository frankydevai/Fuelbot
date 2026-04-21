"""
route_reader.py — Read load details from QM Notifier messages in driver groups

QM Notifier posts structured messages to each driver's Telegram group like:

  🚛 NEW TRIP 8646 HAS BEEN ASSIGNED
  🏁 STOP 1: TMMAL
  📍 ADDRESS:
  1 Cotton Valley Trail , Huntsville, AL 35810
  ...TYPE: Pickup Stop

  🏁 STOP 2: Toyota/Ryder Desk
  📍 ADDRESS:
  1 Lone Star Pass , San Antonio, TX 78264
  ...TYPE: Delivery Stop

We read the last such message per group, parse all stops,
and build a route dict identical to what QuickManage API would return.
"""

import re
import logging
import requests
from functools import lru_cache
from config import TELEGRAM_BOT_TOKEN

log = logging.getLogger(__name__)

BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# ---------------------------------------------------------------------------
# Geocoding — convert address string to lat/lng
# ---------------------------------------------------------------------------

@lru_cache(maxsize=512)
def _geocode(address: str) -> tuple[float, float] | None:
    """Convert full address string to (lat, lng) using OpenStreetMap Nominatim."""
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1, "countrycodes": "us"},
            headers={"User-Agent": "FleetFuelAI/1.0"},
            timeout=5,
        )
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        log.warning(f"Geocode failed for '{address}': {e}")
    return None


# ---------------------------------------------------------------------------
# Telegram — fetch last N messages from a group
# ---------------------------------------------------------------------------

def _get_recent_messages(chat_id: str, limit: int = 50) -> list[dict]:
    """Fetch recent messages from a Telegram group using getUpdates is not possible
    for specific chat — use Bot API forwardMessage workaround via stored message_id.
    
    Instead we use getChatHistory via the bot's stored updates.
    Since bots can't read history, we rely on messages being captured during polling.
    """
    # We use the Telegram Bot API copyMessage/forwardMessage workaround:
    # Actually bots CAN'T read chat history — only receive updates in real time.
    # So we store QM Notifier messages as they arrive during poll_for_uploads.
    # This function reads from our local cache (DB).
    from database import get_last_qm_message
    msg = get_last_qm_message(chat_id)
    return [msg] if msg else []


# ---------------------------------------------------------------------------
# Parser — extract stops from QM Notifier message text
# ---------------------------------------------------------------------------

def _parse_qm_message(text: str) -> dict | None:
    """
    Parse a QM Notifier trip assignment message.
    
    Returns:
    {
        "trip_num": "8646",
        "ref_number": "0397390",
        "stops": [
            {"stop_num": 1, "pickup": True,  "company": "TMMAL",
             "address": "1 Cotton Valley Trail, Huntsville, AL 35810",
             "city": "Huntsville", "state": "AL", "zip": "35810",
             "lat": 34.73, "lng": -86.58},
            {"stop_num": 2, "pickup": False, "company": "Toyota/Ryder",
             "address": "1 Lone Star Pass, San Antonio, TX 78264",
             ...},
        ],
        "origin":      {"lat":..., "lng":..., "city":..., "state":...},
        "destination": {"lat":..., "lng":..., "city":..., "state":...},
    }
    """
    if not text:
        return None

    # Must be a trip assignment message
    trip_match = re.search(r'NEW TRIP\s+(\d+)\s+HAS BEEN ASSIGNED', text, re.IGNORECASE)
    if not trip_match:
        return None

    trip_num = trip_match.group(1)

    # Extract REF number
    ref_match = re.search(r'REF\s*#[:\s]+(\S+)', text, re.IGNORECASE)
    ref_number = ref_match.group(1) if ref_match else ""

    # Split into stop blocks by separator or STOP N: pattern
    # Each block starts with "STOP N:"
    stop_blocks = re.split(r'={5,}', text)

    stops = []
    for block in stop_blocks:
        block = block.strip()
        if not block:
            continue

        # Check if this block is a stop
        stop_num_match = re.search(r'STOP\s+(\d+)[:\s]', block, re.IGNORECASE)
        if not stop_num_match:
            continue

        stop_num = int(stop_num_match.group(1))

        # Company name — line after "STOP N:"
        company_match = re.search(r'STOP\s+\d+[:\s]+(.+)', block, re.IGNORECASE)
        company = company_match.group(1).strip() if company_match else ""

        # Address — line after "ADDRESS:"
        addr_match = re.search(r'ADDRESS:\s*\n(.+)', block, re.IGNORECASE)
        address_line = addr_match.group(1).strip() if addr_match else ""

        # Parse city, state, zip from address line
        # Format: "1 Cotton Valley Trail , Huntsville, AL 35810"
        city, state, zip_ = "", "", ""
        addr_parts = re.search(
            r',\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})',
            address_line
        )
        if addr_parts:
            city  = addr_parts.group(1).strip()
            state = addr_parts.group(2).strip()
            zip_  = addr_parts.group(3).strip()

        # Stop type
        type_match = re.search(r'TYPE:\s*(.+)', block, re.IGNORECASE)
        stop_type  = type_match.group(1).strip().lower() if type_match else ""
        is_pickup  = "pickup" in stop_type

        # Appointment
        appt_match = re.search(r'APPT[:\s]+(.+)', block, re.IGNORECASE)
        appt = appt_match.group(1).strip() if appt_match else ""

        # Geocode
        coords = None
        if address_line:
            # Clean up address for geocoding
            clean_addr = re.sub(r'\s+', ' ', address_line.replace(',', ', ')).strip()
            coords = _geocode(clean_addr)

        stops.append({
            "stop_num":   stop_num,
            "pickup":     is_pickup,
            "company":    company,
            "address":    address_line,
            "city":       city,
            "state":      state,
            "zip":        zip_,
            "lat":        coords[0] if coords else None,
            "lng":        coords[1] if coords else None,
            "appt":       appt,
        })

    if not stops:
        return None

    # Sort by stop number
    stops.sort(key=lambda s: s["stop_num"])

    # Origin = first pickup stop
    origin = next((s for s in stops if s["pickup"] and s["lat"]), None)
    # Destination = last delivery stop
    dest   = next((s for s in reversed(stops) if not s["pickup"] and s["lat"]), None)

    if not origin or not dest:
        log.warning(f"Trip {trip_num}: could not resolve origin/destination")
        return None

    log.info(
        f"Parsed trip {trip_num}: "
        f"{origin['city']},{origin['state']} → {dest['city']},{dest['state']} "
        f"({len(stops)} stops)"
    )

    return {
        "trip_num":    trip_num,
        "ref_number":  ref_number,
        "stops":       stops,
        "origin": {
            "lat":   origin["lat"],
            "lng":   origin["lng"],
            "city":  origin["city"],
            "state": origin["state"],
        },
        "destination": {
            "lat":   dest["lat"],
            "lng":   dest["lng"],
            "city":  dest["city"],
            "state": dest["state"],
        },
        "status": "dispatched",  # assume dispatched when newly assigned
    }


# ---------------------------------------------------------------------------
# Main interface — called from telegram_bot.py when message arrives
# ---------------------------------------------------------------------------

def parse_qm_notifier_message(text: str, group_chat_id: str) -> dict | None:
    """
    Called when bot receives a message from QM NOTIFIER in a driver group.
    Parses the trip details and returns a route dict, or None if not a trip message.
    """
    route = _parse_qm_message(text)
    if route:
        log.info(f"QM message parsed for group {group_chat_id}: trip {route['trip_num']}")
    return route


def get_route_from_db(truck_number: str) -> dict | None:
    """Get the last parsed route for a truck from DB."""
    from database import get_truck_route
    return get_truck_route(truck_number)
