"""
quickmanage_client.py — QuickManage TMS integration with OAuth2

Auth flow:
  POST /auth/token with client_id + client_secret → get Bearer token
  POST /x/trips/search → get active trips

Trip status logic:
  dispatched  → truck heading to first pickup
  in_transit  → truck heading to next undelivered stop
  upcoming/delivered/canceled → ignored
"""

import logging
import requests
import time
from datetime import datetime, timezone, timedelta
from config import QM_CLIENT_ID, QM_CLIENT_SECRET

log = logging.getLogger(__name__)

QM_BASE_URL     = "https://api.quickmanage.com"
_ACTIVE_STATUSES = {"dispatched", "in_transit"}

# Token cache
_token        = None
_token_expiry = 0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_token() -> str | None:
    global _token, _token_expiry

    if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
        log.warning("QuickManage: QM_CLIENT_ID or QM_CLIENT_SECRET not set")
        return None

    # Reuse token if still valid (refresh 60s before expiry)
    if _token and time.time() < _token_expiry - 60:
        return _token

    try:
        # Try with JSON body first
        resp = requests.post(
            f"{QM_BASE_URL}/auth/token",
            json={"client_id": QM_CLIENT_ID, "client_secret": QM_CLIENT_SECRET},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        log.info(f"QM auth response: {resp.status_code} — {resp.text[:300]}")

        if not resp.ok:
            # Try with form data
            resp = requests.post(
                f"{QM_BASE_URL}/auth/token",
                data={"client_id": QM_CLIENT_ID, "client_secret": QM_CLIENT_SECRET},
                timeout=10,
            )
            log.info(f"QM auth form response: {resp.status_code} — {resp.text[:300]}")

        if not resp.ok:
            log.error(f"QM auth failed: {resp.status_code} {resp.text[:200]}")
            return None

        data       = resp.json()
        _token     = data.get("access_token") or data.get("token") or data.get("data", {}).get("access_token")
        expires_in = data.get("expires_in", 3600)
        _token_expiry = time.time() + expires_in
        log.info(f"QuickManage: token obtained (expires in {expires_in}s)")
        return _token
    except Exception as e:
        log.error(f"QuickManage auth failed: {e}")
        return None


def _headers() -> dict:
    token = _get_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    } if token else {}


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

# In-memory geocode cache (persists for process lifetime)
_geocode_cache: dict = {}

def _geocode(address: str) -> tuple[float, float] | None:
    """Geocode using US Census API (free, fast, no rate limit) with fallback to Nominatim."""
    if not address:
        return None

    key = address.strip().lower()
    if key in _geocode_cache:
        return _geocode_cache[key]

    # Try US Census Geocoder first — fast, free, no rate limit
    try:
        resp = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={"address": address, "benchmark": "Public_AR_Current", "format": "json"},
            timeout=5,
        )
        if resp.ok:
            matches = resp.json().get("result", {}).get("addressMatches", [])
            if matches:
                coords = matches[0]["coordinates"]
                result = (float(coords["y"]), float(coords["x"]))
                _geocode_cache[key] = result
                return result
    except Exception:
        pass

    # Fallback: Nominatim with rate limit
    time.sleep(1.1)
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1, "countrycodes": "us"},
            headers={"User-Agent": "FleetFuelAI/1.0"},
            timeout=8,
        )
        if resp.ok and resp.text.strip():
            results = resp.json()
            if results:
                result = float(results[0]["lat"]), float(results[0]["lon"])
                _geocode_cache[key] = result
                return result
    except Exception as e:
        log.warning(f"Geocode failed for '{address}': {e}")

    return None


def _stop_coords(stop: dict) -> tuple[float, float] | None:
    addr     = stop.get("address") or {}
    line1    = addr.get("address_line_1", "").strip()
    city     = addr.get("city", "").strip()
    state    = addr.get("state", "").strip()
    zip_code = addr.get("zip_code", "").strip()
    if not city or not state:
        return None
    query = f"{line1}, {city}, {state} {zip_code}".strip(", ")
    return _geocode(query)


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _search_trips(page_size: int = 100) -> list[dict]:
    hdrs = _headers()
    if not hdrs:
        return []

    # Try filtering by active statuses directly
    endpoints = [
        # Filter by in_transit status
        ("POST", f"{QM_BASE_URL}/x/trips/search", {
            "query": "", "page": 0, "page_size": page_size,
            "filters": [{"field": "status", "operator": "in", "value": ["in_transit", "dispatched", "upcoming"]}]
        }),
        # Filter by in_transit only
        ("POST", f"{QM_BASE_URL}/x/trips/search", {
            "query": "", "page": 0, "page_size": page_size,
            "filters": [{"field": "status", "operator": "eq", "value": "in_transit"}]
        }),
        # No filter — get all and filter client side
        ("POST", f"{QM_BASE_URL}/x/trips/search", {"query": "", "filters": [], "page": 0, "page_size": page_size}),
        ("GET",  f"{QM_BASE_URL}/x/trips", None),
    ]

    for method, url, payload in endpoints:
        try:
            if method == "POST":
                resp = requests.post(url, json=payload, headers=hdrs, timeout=10)
            else:
                resp = requests.get(url, headers=hdrs, timeout=10)

            log.info(f"QM {method} {url} → {resp.status_code}: {resp.text[:600]}")

            if not resp.ok:
                continue

            data = resp.json()
            # Handle different response structures
            items = (
                data.get("data", {}).get("items") or
                data.get("data", {}).get("trips") or
                data.get("items") or
                data.get("trips") or
                (data.get("data") if isinstance(data.get("data"), list) else None) or
                []
            )
            if items:
                log.info(f"QM: found {len(items)} trips via {method} {url}")
                return items
        except Exception as e:
            log.error(f"QM {method} {url} failed: {e}")

    return []


def _build_route(trip: dict, truck_number: str) -> dict | None:
    stops_raw = trip.get("stops") or []
    if len(stops_raw) < 2:
        return None

    status = trip.get("status", "").lower()
    stops  = []

    for s in stops_raw:
        addr  = s.get("address") or {}
        city  = addr.get("city", "").strip()
        state = addr.get("state", "").strip()
        zip_  = addr.get("zip_code", "").strip()
        line1 = addr.get("address_line_1", "").strip()
        coords = _stop_coords(s)

        stops.append({
            "pickup":       bool(s.get("pickup")),
            "company_name": s.get("company_name", ""),
            "address":      line1,
            "city":         city,
            "state":        state,
            "zip":          zip_,
            "lat":          coords[0] if coords else None,
            "lng":          coords[1] if coords else None,
            "appt":         s.get("appointment_date", ""),
        })

    # Origin = first pickup with coords
    origin = next((s for s in stops if s["pickup"] and s["lat"]), None)

    # Destination depends on status
    if status == "dispatched":
        dest = next((s for s in stops if s["pickup"] and s["lat"]), None)
    else:
        # in_transit — next stop after first pickup
        passed_first = False
        dest = None
        for s in stops:
            if s["pickup"] and not passed_first:
                passed_first = True
                continue
            if s["lat"]:
                dest = s
                break
        if not dest:
            dest = next((s for s in reversed(stops) if s["lat"]), None)

    if not origin or not dest:
        log.warning(f"Trip {trip.get('trip_num')}: no coords for origin/dest")
        return None

    return {
        "trip_id":      trip.get("id", ""),
        "trip_num":     trip.get("trip_num"),
        "ref_number":   trip.get("ref_number", ""),
        "truck_number": truck_number,
        "status":       status,
        "stops":        stops,
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
    }


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_all_truck_routes() -> dict[str, dict]:
    """
    Fetch all active trips and return truck_number → route mapping.
    Called every poll cycle from main.py.
    """
    if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
        return {}

    trips  = _search_trips()
    active = [t for t in trips if t.get("status", "").lower() in _ACTIVE_STATUSES]
    # Log all statuses for debugging
    from collections import Counter
    status_counts = Counter(t.get("status","unknown") for t in trips)
    log.info(f"QuickManage: {len(trips)} trips total — statuses: {dict(status_counts)}")
    log.info(f"QuickManage: {len(active)} active (dispatched/in_transit)")

    routes = {}
    for trip in active:
        stops = trip.get("stops") or []
        # Find truck number — check all stops
        truck_number = None
        for stop in stops:
            truck = stop.get("assigned_truck") or {}
            num   = str(truck.get("number", "")).strip()
            if num and truck.get("id") != "00000000-0000-0000-0000-000000000000":
                truck_number = num
                break
        if not truck_number:
            continue

        route = _build_route(trip, truck_number)
        if route:
            routes[truck_number] = route
            log.info(
                f"  Truck {truck_number}: trip {route['trip_num']} "
                f"{route['origin']['city']} → {route['destination']['city']} "
                f"[{route['status']}]"
            )

    return routes


def get_route_for_truck(truck_number: str) -> dict | None:
    """Get active route for a specific truck — searches QM by truck number directly."""
    hdrs = _headers()
    if not hdrs:
        return None

    # Search trips filtered by truck number
    payloads = [
        {"query": str(truck_number), "filters": [], "page": 0, "page_size": 20},
        {"query": "", "filters": [{"field": "truck_number", "operator": "eq", "value": str(truck_number)}], "page": 0, "page_size": 10},
        {"query": "", "filters": [{"field": "status", "operator": "in", "value": ["in_transit","dispatched","upcoming"]}], "page": 0, "page_size": 100},
    ]

    for payload in payloads:
        try:
            resp = requests.post(f"{QM_BASE_URL}/x/trips/search", json=payload, headers=hdrs, timeout=10)
            if not resp.ok:
                continue
            items = resp.json().get("data", {}).get("items", [])
            # Find trip assigned to this truck
            for trip in items:
                if trip.get("status","").lower() not in _ACTIVE_STATUSES:
                    continue
                stops = trip.get("stops") or []
                for stop in stops:
                    truck = stop.get("assigned_truck") or {}
                    if str(truck.get("number","")).strip() == str(truck_number).strip():
                        route = _build_route(trip, str(truck_number))
                        if route:
                            log.info(f"Found route for truck {truck_number}: trip {route['trip_num']}")
                            return route
        except Exception as e:
            log.error(f"get_route_for_truck search failed: {e}")

    # Fall back to full route list
    routes = get_all_truck_routes()
    return routes.get(str(truck_number))
