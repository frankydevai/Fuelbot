"""
california.py  -  California border reminder logic.

Sends a one-time reminder when a truck is heading toward California
and has enough fuel to still fill up cheaply before crossing.

Trigger conditions:
  - Truck is in a CA border state (NV, AZ, OR)
  - Truck heading is generally westward (toward CA)
  - Truck is within CA_BORDER_REMINDER_MILES of the CA border
  - Fuel is below CA_BORDER_FUEL_THRESHOLD (default 70%)
  - Reminder not already sent for this approach

Reminder resets when:
  - Truck fuel goes above 80% (they filled up)
  - Truck crosses into CA (state changes)
  - Truck turns around (heading no longer toward CA)
"""

import math
import logging
from config import CA_BORDER_REMINDER_MILES, CA_BORDER_FUEL_THRESHOLD

log = logging.getLogger(__name__)

# Approximate CA border waypoints (lat/lng along the state border)
# We use these to estimate distance to CA border
_CA_BORDER_POINTS = [
    (42.00, -120.00),   # OR/CA border north
    (41.99, -121.45),
    (41.99, -122.51),
    (41.99, -123.00),
    (39.00, -114.04),   # NV/CA border middle
    (37.50, -114.04),
    (35.00, -114.63),
    (34.47, -114.13),
    (32.72, -114.72),   # AZ/CA border south
]

# States that border California
_CA_BORDER_STATES = {"NV", "AZ", "OR"}

# Westward heading range — widened to catch I-40, I-80, I-10 approaches
_WEST_HEADING_MIN = 240
_WEST_HEADING_MAX = 320

# California average diesel price premium over surrounding states
CA_PRICE_PREMIUM = float(1.10)   # $1.10/gal higher on average


def _dist_to_ca_border(lat: float, lng: float) -> float:
    """Approximate distance in miles to the nearest CA border point."""
    from truck_stop_finder import haversine_miles
    return min(haversine_miles(lat, lng, blat, blng)
               for blat, blng in _CA_BORDER_POINTS)


def _is_heading_toward_ca(heading: float) -> bool:
    """Return True if truck heading is generally westward (toward CA)."""
    return _WEST_HEADING_MIN <= heading <= _WEST_HEADING_MAX


def should_send_ca_reminder(
    state_code: str,
    lat: float,
    lng: float,
    heading: float,
    fuel_pct: float,
    ca_reminder_sent: bool,
    tank_gal: float = 150.0,
    mpg: float = 6.5,
    route_dest_state: str = "",
) -> bool:
    """
    Return True ONLY if truck genuinely needs to fuel before entering CA.

    Logic:
    1. Truck must be heading toward CA (west)
    2. Truck must be within reminder distance of border
    3. Truck must NOT have enough fuel to complete the CA crossing

    CA is ~800 miles wide on I-10 (El Paso to LA) or ~300 miles on I-15 (NV to LA).
    We estimate miles through CA based on route destination.
    If truck can coast through CA on current fuel → no reminder.
    """
    if ca_reminder_sent:
        return False

    if state_code not in _CA_BORDER_STATES:
        return False

    if not _is_heading_toward_ca(heading):
        return False

    dist = _dist_to_ca_border(lat, lng)
    if dist > CA_BORDER_REMINDER_MILES:
        return False

    # Estimate miles through CA based on route destination
    # If delivering IN CA → estimate 200mi average
    # If passing through CA (dest outside CA) → estimate 300mi (I-5/I-10 crossing)
    if route_dest_state and route_dest_state.upper() == "CA":
        ca_miles = 200.0   # delivering inside CA
    else:
        ca_miles = 320.0   # passing through CA

    # Total miles needed = dist to border + miles through CA
    total_needed   = dist + ca_miles
    range_miles    = (fuel_pct / 100) * tank_gal * mpg * 0.85  # 85% safety buffer

    if range_miles >= total_needed:
        # Truck has enough fuel — no reminder needed
        log.info(f"CA border: range={range_miles:.0f}mi needed={total_needed:.0f}mi "
                 f"— enough fuel, no reminder")
        return False

    # Truck needs fuel before CA
    log.info(f"CA border reminder triggered: fuel={fuel_pct:.0f}% "
             f"range={range_miles:.0f}mi needed={total_needed:.0f}mi "
             f"dist_to_border={dist:.0f}mi")
    return True


def should_reset_ca_reminder(
    state_code: str,
    fuel_pct: float,
    heading: float,
    ca_reminder_sent: bool,
) -> bool:
    """
    Return True if the CA reminder flag should be reset
    (truck filled up, crossed into CA, or turned around).
    """
    if not ca_reminder_sent:
        return False

    # Truck crossed into CA — reset so next approach triggers again
    if state_code == "CA":
        return True

    # Truck filled up (above 80%) — they listened!
    if fuel_pct >= 80:
        return True

    # Truck turned around — no longer heading west
    if not _is_heading_toward_ca(heading):
        return True

    return False


def get_ca_avg_diesel_price(stops: list) -> float | None:
    """Calculate average diesel price from CA stops in the DB."""
    ca_prices = [
        s["diesel_price"] for s in stops
        if s.get("state") == "CA" and s.get("diesel_price")
    ]
    if not ca_prices:
        return None
    return round(sum(ca_prices) / len(ca_prices), 3)
