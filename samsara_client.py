"""
samsara_client.py  -  Fetch vehicle locations and fuel levels from Samsara API.
"""

import requests
from config import SAMSARA_API_TOKEN, SAMSARA_BASE_URL

HEADERS = {
    "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
    "Content-Type":  "application/json",
}


def _get(endpoint: str, params: dict = None) -> dict:
    url  = f"{SAMSARA_BASE_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_vehicle_locations() -> list[dict]:
    """Fetch current vehicle locations from Samsara."""
    url  = "https://api.samsara.com/fleet/vehicles/locations"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    # Log any trucks with stale locations (>2 hours old)
    import logging
    log = logging.getLogger(__name__)
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    fresh = []
    for v in data:
        loc = v.get("location", {})
        ts  = loc.get("time", "")
        if ts:
            try:
                t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_min = (now - t).total_seconds() / 60
                if age_min > 120:
                    log.warning(f"Truck {v.get('name')} GPS stale: {age_min:.0f} min old ({loc.get('reverseGeo',{}).get('formattedLocation','')})")
                    continue  # skip stale trucks — don't alert on bad data
            except Exception:
                pass
        fresh.append(v)
    log.info(f"Samsara: {len(data)} trucks total, {len(fresh)} with fresh GPS")
    return fresh


def get_vehicle_stats() -> list[dict]:
    """Fetch current fuel levels using stats/feed — returns latest value per vehicle."""
    url    = "https://api.samsara.com/fleet/vehicles/stats/feed"
    params = {"types": "fuelPercents"}
    resp   = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", [])


def get_driver_for_vehicle(vehicle_id: str) -> dict | None:
    try:
        url  = f"https://api.samsara.com/fleet/vehicles/{vehicle_id}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.json().get("data", {}).get("currentDriver")
    except Exception:
        return None


def get_combined_vehicle_data() -> list[dict]:
    """
    Merge locations + fuel stats into one list per vehicle.
    Returns list of dicts with: vehicle_id, vehicle_name, lat, lng,
    heading, speed_mph, fuel_pct.
    """
    locations_raw = get_vehicle_locations()
    stats_raw     = get_vehicle_stats()

    # Index fuel stats by vehicle id
    stats_map = {}
    for s in stats_raw:
        vid = s.get("id")
        if not vid:
            continue
        fuel_events = s.get("fuelPercents", [])
        if fuel_events:
            latest = max(fuel_events, key=lambda x: x.get("time", ""))
            val = latest.get("value")
            # value is 0.0-1.0 in feed (fraction) — convert to percentage
            if val is not None:
                fval = float(val)
                stats_map[vid] = round(fval * 100, 1) if fval <= 1.0 else round(fval, 1)
            else:
                stats_map[vid] = 100.0
        else:
            stats_map[vid] = 100.0

    results = []
    for v in locations_raw:
        vid  = v.get("id")
        name = v.get("name", vid)
        loc  = v.get("location", {})
        lat  = loc.get("latitude")
        lng  = loc.get("longitude")

        if lat is None or lng is None:
            continue

        driver      = get_driver_for_vehicle(vid)
        driver_name = driver.get("name") if driver else None

        results.append({
            "vehicle_id":   vid,
            "vehicle_name": name,
            "driver_name":  driver_name,
            "lat":          float(lat),
            "lng":          float(lng),
            "heading":      float(loc.get("heading", 0)),
            "speed_mph":    float(loc.get("speed", 0)),
            "fuel_pct":     stats_map.get(vid, 100.0),
        })

    return results

def get_vehicle_location_history(vehicle_id: str, hours_back: int = 1) -> list[dict]:
    """
    Fetch GPS location history for a vehicle for the last N hours.
    Uses Samsara /fleet/vehicles/locations/feed endpoint.
    Returns list of {lat, lng, time} sorted oldest first.
    """
    from datetime import datetime, timezone, timedelta
    import requests as _req

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)

    try:
        resp = _req.get(
            "https://api.samsara.com/fleet/vehicles/locations/history",
            headers=HEADERS,
            params={
                "vehicleIds": vehicle_id,
                "startTime":  start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime":    end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            timeout=10,
        )
        if not resp.ok:
            return []
        data = resp.json().get("data", [])
        if not data:
            return []
        locations = data[0].get("locations", [])
        result = []
        for loc in locations:
            lat = loc.get("latitude") or loc.get("location", {}).get("latitude")
            lng = loc.get("longitude") or loc.get("location", {}).get("longitude")
            ts  = loc.get("time") or loc.get("location", {}).get("time")
            if lat and lng:
                result.append({"lat": float(lat), "lng": float(lng), "time": ts})
        return sorted(result, key=lambda x: x["time"])
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Location history failed for {vehicle_id}: {e}")
        return []

def get_vehicle_fuel_efficiency(vehicle_id: str = None) -> dict:
    """
    Fetch real MPG and idle data from Samsara Fuel & Energy report.
    Returns dict of vehicle_id -> {mpg, idle_hours, idle_pct, fuel_used_gal}
    """
    import requests as _req
    from datetime import datetime, timezone, timedelta

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=30)

    params = {
        "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endTime":   end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if vehicle_id:
        params["vehicleIds"] = vehicle_id

    try:
        resp = _req.get(
            "https://api.samsara.com/fleet/reports/vehicles/fuel-energy",
            headers=HEADERS,
            params=params,
            timeout=15,
        )
        if not resp.ok:
            import logging
            logging.getLogger(__name__).warning(f"Samsara fuel report: {resp.status_code} {resp.text[:200]}")
            return {}

        data    = resp.json().get("data", [])
        results = {}
        for v in data:
            vid        = v.get("id") or v.get("vehicleId", "")
            stats      = v.get("stats") or v.get("fuelAndEnergyStats") or {}
            mpg        = stats.get("mpg") or stats.get("fuelEfficiencyMpg") or 0
            idle_hours = stats.get("idleTimeHours") or stats.get("idleHours") or 0
            idle_pct   = stats.get("idleTimePercent") or stats.get("idlePercent") or 0
            fuel_gal   = stats.get("fuelUsedGallons") or stats.get("totalFuelUsedGallons") or 0
            if vid:
                results[vid] = {
                    "mpg":        round(float(mpg), 2) if mpg else 0,
                    "idle_hours": round(float(idle_hours), 1) if idle_hours else 0,
                    "idle_pct":   round(float(idle_pct), 1) if idle_pct else 0,
                    "fuel_gal":   round(float(fuel_gal), 1) if fuel_gal else 0,
                }
        return results
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Samsara fuel efficiency failed: {e}")
        return {}


def get_vehicle_idle_events(vehicle_id: str, hours_back: int = 24) -> list[dict]:
    """
    Fetch idling events for a specific vehicle.
    Returns list of {start_time, duration_minutes, location}
    """
    import requests as _req
    from datetime import datetime, timezone, timedelta

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)

    try:
        resp = _req.get(
            "https://api.samsara.com/idling/events",
            headers=HEADERS,
            params={
                "vehicleIds": vehicle_id,
                "startTime":  start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime":    end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            timeout=10,
        )
        if not resp.ok:
            return []
        events  = resp.json().get("data", [])
        results = []
        for e in events:
            duration_ms  = e.get("durationMilliseconds") or e.get("duration", 0)
            duration_min = round(duration_ms / 60000, 1) if duration_ms else 0
            results.append({
                "start_time":      e.get("startTime", ""),
                "duration_minutes": duration_min,
                "location":        e.get("location", {}).get("reverseGeo", {}).get("formattedLocation", ""),
            })
        return results
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Samsara idle events failed: {e}")
        return []
