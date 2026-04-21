"""
yard_geofence.py  -  Detect whether a truck is inside a company yard.

Yards configured in .env as YARD_N=Name:lat:lng:radius_miles
Example: YARD_1=Main Yard:28.4277:-81.3816:0.5
"""

import math
import logging
from config import YARDS

log = logging.getLogger(__name__)
EARTH_RADIUS_MILES = 3958.8


def _haversine(lat1, lng1, lat2, lng2) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return EARTH_RADIUS_MILES * 2 * math.asin(math.sqrt(a))


def is_in_yard(lat: float, lng: float) -> bool:
    if not YARDS:
        return False
    for yard in YARDS:
        if _haversine(lat, lng, yard["lat"], yard["lng"]) <= yard["radius_miles"]:
            return True
    return False


def get_yard_name(lat: float, lng: float) -> str | None:
    for yard in YARDS:
        if _haversine(lat, lng, yard["lat"], yard["lng"]) <= yard["radius_miles"]:
            return yard["name"]
    return None
