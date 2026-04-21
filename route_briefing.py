"""
route_briefing.py — One-time route fuel plan sent when QM status → in_transit

Triggered once per trip when dispatched → in_transit.
Calculates exactly how many stops the truck needs based on:
  - Current fuel %
  - Tank capacity
  - Real MPG from Samsara
  - Total route distance
  - IFTA-adjusted net cost per stop

Sends to: dispatcher group + driver's Telegram group
"""

import math
import logging
from database import get_all_diesel_stops, db_cursor
from truck_stop_finder import haversine_miles, bearing, angle_diff, reachable_miles
from ifta import net_price_after_ifta, get_ifta_rate
from border_strategy import (
    analyze_route_borders, build_border_strategy,
    format_border_warnings, AVOID_FUEL_STATES, LOW_STOP_STATES
)
from config import DEFAULT_TANK_GAL, DEFAULT_MPG, IFTA_HOME_STATE

log = logging.getLogger(__name__)

CORRIDOR_MILES = 75.0   # search width either side of route line
LATE_STOP_RATIO = 0.60  # prefer using at least 60% of current range before fueling
LATE_STOP_MILES = 120.0 # or stopping within the last 120 miles of reachable range


def _reachable_miles(fuel_pct: float, tank_gal: float, mpg: float) -> float:
    """Match the core stop finder reachability logic."""
    return reachable_miles(fuel_pct, tank_gal, mpg)


def _gallons_to_full(fuel_pct: float, tank_gal: float) -> float:
    """Gallons needed to fill tank from current level."""
    return round(tank_gal * (1 - fuel_pct / 100), 1)


def _nearest_priced_stop(
    truck_lat: float,
    truck_lng: float,
    all_stops: list[dict],
    max_miles: float,
) -> dict | None:
    """Emergency fallback: nearest stop with a valid card price."""
    nearest = None
    nearest_dist = None
    for stop in all_stops:
        price = stop.get("diesel_price")
        if not price:
            continue
        try:
            dist = haversine_miles(
                truck_lat, truck_lng,
                float(stop["latitude"]), float(stop["longitude"])
            )
        except Exception:
            continue
        if dist > max_miles:
            continue
        if nearest is None or dist < nearest_dist:
            nearest = {
                **stop,
                "dist_from_truck": round(dist, 1),
                "card_price": round(float(price), 3),
                "retail_price": stop.get("retail_price"),
                "net_price": round(net_price_after_ifta(float(price), stop.get("state", "")), 4),
                "ifta_rate": round(get_ifta_rate(stop.get("state", "")), 3),
            }
            nearest_dist = dist
    return nearest


def _can_continue_after_stop(
    stop: dict,
    total_dist: float,
    all_candidates: list[dict],
    fill_to_pct: float,
    tank_gal: float,
    mpg: float,
) -> bool:
    """Whether stopping here still allows the route to continue after refueling."""
    dist_to_stop = stop["dist_from_truck"]
    post_fill_range = _reachable_miles(fill_to_pct, tank_gal, mpg)

    if post_fill_range >= (total_dist - dist_to_stop):
        return True

    next_candidates = [
        s for s in all_candidates
        if s["dist_from_truck"] > dist_to_stop + 5
        and (s["dist_from_truck"] - dist_to_stop) <= post_fill_range
    ]
    return bool(next_candidates)


def _choose_best_route_stop(
    viable_candidates: list[dict],
    sim_dist: float,
    range_now: float,
) -> dict:
    """Prefer cheap stops later in the reachable window, not the first cheap stop."""
    late_floor = sim_dist + max(range_now * LATE_STOP_RATIO, range_now - LATE_STOP_MILES)
    preferred = [
        s for s in viable_candidates
        if s["dist_from_truck"] >= late_floor
    ]
    pool = preferred or viable_candidates
    return min(pool, key=lambda stop: (stop["net_price"], -stop["dist_from_truck"]))


def _is_waypoint_ahead(
    truck_lat: float,
    truck_lng: float,
    dest_lat: float,
    dest_lng: float,
    wp_lat: float,
    wp_lng: float,
) -> bool:
    """Keep only waypoints generally ahead toward the destination."""
    route_bearing = bearing(truck_lat, truck_lng, dest_lat, dest_lng)
    wp_bearing = bearing(truck_lat, truck_lng, wp_lat, wp_lng)
    return angle_diff(route_bearing, wp_bearing) <= 100


def _stops_on_segment(from_lat, from_lng, to_lat, to_lng,
                       all_stops, exclude_names=None) -> list:
    """
    Find fuel stops along a route segment.
    Uses geographic bounding box + heading filter to prevent wrong-direction stops.
    """
    seg_bearing = bearing(from_lat, from_lng, to_lat, to_lng)
    seg_dist    = haversine_miles(from_lat, from_lng, to_lat, to_lng)
    exclude     = exclude_names or set()

    # Bounding box — segment bounds + corridor buffer in degrees
    buf     = CORRIDOR_MILES / 55.0
    buf_lng = CORRIDOR_MILES / 45.0
    min_lat = min(from_lat, to_lat) - buf
    max_lat = max(from_lat, to_lat) + buf
    min_lng = min(from_lng, to_lng) - buf_lng
    max_lng = max(from_lng, to_lng) + buf_lng

    candidates = []
    for stop in all_stops:
        if not stop.get("diesel_price"):
            continue
        if stop.get("store_name") in exclude:
            continue

        slat = float(stop["latitude"])
        slng = float(stop["longitude"])

        # Geographic bounding box — fast reject of wrong-direction stops
        if not (min_lat <= slat <= max_lat and min_lng <= slng <= max_lng):
            continue

        dist = haversine_miles(from_lat, from_lng, slat, slng)
        if dist > seg_dist * 1.1 + 15:
            continue

        stop_bear = bearing(from_lat, from_lng, slat, slng)
        adiff     = angle_diff(seg_bearing, stop_bear)
        if adiff > 75:
            continue

        along = dist * math.cos(math.radians(adiff))
        cross = abs(dist * math.sin(math.radians(adiff)))

        if along <= 0 or cross > CORRIDOR_MILES:
            continue

        state    = stop.get("state", "")
        card     = float(stop["diesel_price"])
        net      = net_price_after_ifta(card, state)
        ifta_adj = get_ifta_rate(state)

        candidates.append({
            **stop,
            "dist_from_origin": round(along, 1),
            "net_price":        round(net, 4),
            "ifta_rate":        round(ifta_adj, 3),
            "card_price":       round(card, 3),
            "retail_price":     stop.get("retail_price"),
        })

    return sorted(candidates, key=lambda s: s["dist_from_origin"])

def plan_route_briefing(
    truck_lat: float,
    truck_lng: float,
    current_fuel_pct: float,
    tank_gal: float,
    mpg: float,
    route: dict,
) -> dict:
    """
    Plan ALL fuel stops needed for entire route from current position.

    Returns a full route plan with the first recommended stop, all planned stops,
    and any border-fuel strategy decisions.
    """
    all_stops = get_all_diesel_stops()
    stops_raw = route.get("stops", [])
    dest      = route.get("destination", {})
    dest_lat  = float(dest["lat"]) if dest.get("lat") else None
    dest_lng  = float(dest["lng"]) if dest.get("lng") else None

    # Build waypoints: current pos -> remaining route stops with coords -> destination.
    # For in_transit trips, completed pickup legs are behind the truck and should not
    # influence border strategy or first-stop selection.
    waypoints = [{"lat": truck_lat, "lng": truck_lng}]
    for s in stops_raw:
        if s.get("lat") and s.get("lng"):
            wp_lat = float(s["lat"])
            wp_lng = float(s["lng"])
            if str(route.get("status", "")).lower() == "in_transit" and s.get("pickup"):
                continue
            wp_dist = haversine_miles(truck_lat, truck_lng, wp_lat, wp_lng)
            if wp_dist > 1.0:
                if dest_lat is not None and dest_lng is not None:
                    if not _is_waypoint_ahead(truck_lat, truck_lng, dest_lat, dest_lng, wp_lat, wp_lng):
                        continue
                waypoints.append({
                    "lat":   wp_lat,
                    "lng":   wp_lng,
                    "city":  s.get("city", ""),
                    "state": s.get("state", ""),
                })

    if dest_lat is not None and dest_lng is not None:
        waypoints.append({
            "lat":   dest_lat,
            "lng":   dest_lng,
            "city":  dest.get("city", ""),
            "state": dest.get("state", ""),
        })


    if len(waypoints) < 2:
        return {"error": "Not enough route waypoints with coordinates"}

    # Total route distance
    total_dist = sum(
        haversine_miles(waypoints[i]["lat"], waypoints[i]["lng"],
                        waypoints[i+1]["lat"], waypoints[i+1]["lng"])
        for i in range(len(waypoints) - 1)
    )

    # Can truck complete without stopping?
    range_miles = _reachable_miles(current_fuel_pct, tank_gal, mpg)
    can_complete = range_miles >= total_dist

    if can_complete:
        return {
            "stops_needed":               0,
            "planned_stops":              [],
            "total_distance":             round(total_dist, 1),
            "total_card_cost":            0,
            "total_net_cost":             0,
            "warnings":                   [],
            "can_complete_without_stop":  True,
        }

    # Plan stops along entire route
    planned_stops  = []
    warnings       = []
    total_card     = 0.0
    total_net      = 0.0
    used_names     = set()

    cur_lat        = truck_lat
    cur_lng        = truck_lng
    cur_fuel_pct   = current_fuel_pct
    stop_number = 1
    emergency_mode = current_fuel_pct <= 10
    critical_mode  = current_fuel_pct <= 20
    FILL_TO        = 100.0 if emergency_mode else 90.0

    if emergency_mode:
        warnings.append(
            f"Emergency fuel level: {current_fuel_pct:.0f}% fuel. Nearest reachable stop takes priority."
        )
    elif critical_mode:
        warnings.append(
            f"Critical fuel level: {current_fuel_pct:.0f}% fuel. First safe stop takes priority over later cheaper stops."
        )

    # ── Collect all stops along route ────────────────────────────────────────
    all_candidates = []
    prev_lat, prev_lng = truck_lat, truck_lng
    seg_base = 0.0

    for wp in waypoints[1:]:
        seg_stops = _stops_on_segment(
            prev_lat, prev_lng,
            wp["lat"], wp["lng"],
            all_stops, exclude_names=used_names
        )
        for s in seg_stops:
            s["dist_from_truck"] = round(seg_base + s["dist_from_origin"], 1)
        all_candidates.extend(seg_stops)
        seg_base += haversine_miles(prev_lat, prev_lng, wp["lat"], wp["lng"])
        prev_lat, prev_lng = wp["lat"], wp["lng"]

    # Deduplicate — keep lowest net_price per name+city
    seen_stops = {}
    for s in sorted(all_candidates, key=lambda x: x["dist_from_truck"]):
        key = (s["store_name"], s["city"])
        if key not in seen_stops:
            seen_stops[key] = s
    unique_candidates = sorted(seen_stops.values(), key=lambda x: x["dist_from_truck"])

    # ── Greedy planning — only stops truck can physically reach ──────────────
    # Key rule: NEVER recommend a stop beyond current fuel range.
    # Walk forward stop by stop. At each position, check:
    #   1. Can I reach the NEXT stop (or destination) from here? → skip
    #   2. Can I not? → stop here, fill to 90%, continue

    sim_fuel = current_fuel_pct
    sim_dist = 0.0

    while sim_dist < total_dist:
        range_now = _reachable_miles(sim_fuel, tank_gal, mpg)
        max_reach_dist = sim_dist + range_now

        if max_reach_dist >= total_dist:
            break

        reachable_candidates = [
            s for s in unique_candidates
            if sim_dist < s["dist_from_truck"] <= max_reach_dist
        ]
        viable_candidates = [
            s for s in reachable_candidates
            if _can_continue_after_stop(
                s, total_dist, unique_candidates, FILL_TO, tank_gal, mpg
            )
        ]

        if not viable_candidates:
            if (emergency_mode or critical_mode) and reachable_candidates:
                s = min(reachable_candidates, key=lambda stop: stop["dist_from_truck"])
            elif emergency_mode and sim_dist == 0.0:
                s = _nearest_priced_stop(
                    truck_lat,
                    truck_lng,
                    all_stops,
                    max(range_now, 60.0),
                )
                if s:
                    warnings.append(
                        "Emergency fallback used: nearest priced stop selected because no full route candidate was available."
                    )
                else:
                    warnings.append(
                        f"No reachable fuel stop found within {range_now:.0f} miles of current range."
                    )
                    break
            else:
                warnings.append(
                    f"No reachable fuel stop found within {range_now:.0f} miles of current range."
                )
                break
        else:
            # At low fuel, prioritize the first safe reachable stop.
            if (emergency_mode or critical_mode) and sim_dist == 0.0:
                s = min(viable_candidates, key=lambda stop: stop["dist_from_truck"])
            else:
                # Pick a cheap stop late enough in the range window to avoid fueling too early.
                s = _choose_best_route_stop(viable_candidates, sim_dist, range_now)

        dist_to_stop = s["dist_from_truck"]
        miles_to_stop = dist_to_stop - sim_dist
        fuel_arrival = sim_fuel - (miles_to_stop / mpg / tank_gal) * 100
        gal_to_fill = round((FILL_TO - max(fuel_arrival, 5)) / 100 * tank_gal, 1)
        gal_to_fill = max(min(gal_to_fill, tank_gal * 0.95), 15)  # min 15 gal

        card_cost = round(s["card_price"] * gal_to_fill, 2)
        net_cost  = round(s["net_price"]  * gal_to_fill, 2)
        total_card += card_cost
        total_net  += net_cost

        maps_url = (f"https://maps.google.com/?q={s['latitude']},{s['longitude']}"
                    if s.get("latitude") and s.get("longitude") else None)

        planned_stops.append({
            "stop_number":      stop_number,
            "store_name":       s["store_name"],
            "address":          s.get("address", ""),
            "city":             s.get("city", ""),
            "state":            s.get("state", ""),
            "dist_from_truck":  dist_to_stop,
            "card_price":       s["card_price"],
            "retail_price":     s.get("retail_price"),
            "net_price":        s["net_price"],
            "ifta_rate":        s.get("ifta_rate", 0),
            "gallons_to_fill":  gal_to_fill,
            "total_card_cost":  card_cost,
            "total_net_cost":   net_cost,
            "maps_url":         maps_url,
            "low_stop_warning": None,
            "latitude":         s.get("latitude"),
            "longitude":        s.get("longitude"),
        })

        used_names.add(s["store_name"])
        stop_number += 1

        sim_fuel = FILL_TO
        sim_dist = dist_to_stop

        # ── Border strategy analysis ──────────────────────────────────────────
    # Build waypoints with CUMULATIVE along-route distance (not straight-line)
    border_waypoints = []
    cumulative_dist  = 0.0
    prev_lat2, prev_lng2 = truck_lat, truck_lng

    for i, wp in enumerate(waypoints[1:], 1):
        seg_dist = haversine_miles(prev_lat2, prev_lng2, wp["lat"], wp["lng"])
        cumulative_dist += seg_dist
        border_waypoints.append({
            **wp,
            "dist_from_truck": round(cumulative_dist, 1),
            "is_delivery":     i < len(waypoints) - 1,
        })
        prev_lat2, prev_lng2 = wp["lat"], wp["lng"]

    truck_state = waypoints[0].get("state", "") if waypoints else ""
    border_events   = analyze_route_borders(border_waypoints, truck_state)

    # Attach dist_from_truck to all_stops for border strategy
    for s in all_stops:
        if "dist_from_truck" not in s:
            s["dist_from_truck"] = haversine_miles(
                truck_lat, truck_lng,
                float(s["latitude"]), float(s["longitude"])
            )
        s["net_price"] = net_price_after_ifta(
            float(s.get("diesel_price", 0)),
            s.get("state", "")
        )

    border_decisions = build_border_strategy(
        current_fuel_pct, tank_gal, mpg,
        border_events, all_stops,
        route_waypoints=border_waypoints,
        truck_lat=truck_lat,
        truck_lng=truck_lng,
        truck_heading=route.get("heading", 0),
    )
    border_warn_miles = 100 if current_fuel_pct < 70 else 0
    border_warnings = format_border_warnings(
        border_decisions,
        approaching_miles=border_warn_miles,
    )

    # Remove any planned stops INSIDE avoid/low-stop states
    # if border strategy already handles fueling before the border
    avoid_states = set()
    for d in border_decisions:
        if d["action"] == "fuel_before_border":
            avoid_states.add(d["event"].state)

    # Add the pre-border stop from border strategy into planned_stops
    border_stops = []
    for d in border_decisions:
        if d["action"] == "fuel_before_border" and d["stop"]:
            s = d["stop"]
            # Calculate fuel at arrival to the pre-border stop
            dist_to_stop    = s.get("dist_from_truck", 0)
            fuel_consumed   = (dist_to_stop / mpg / tank_gal) * 100
            fuel_at_arrival = max(current_fuel_pct - fuel_consumed, 5)
            # Fill from fuel_at_arrival up to fill_to_pct
            gal = round(tank_gal * max(d["fill_to_pct"] - fuel_at_arrival, 0) / 100, 1)
            border_stops.append({
                "stop_number":      0,  # will be renumbered below
                "store_name":       s.get("store_name", ""),
                "address":          s.get("address", ""),
                "city":             s.get("city", ""),
                "state":            s.get("state", ""),
                "dist_from_truck":  s.get("dist_from_truck", 0),
                "card_price":       s.get("diesel_price", 0),
                "retail_price":     s.get("retail_price"),
                "net_price":        s.get("net_price", s.get("diesel_price", 0)),
                "ifta_rate":        s.get("ifta_rate", 0),
                "gallons_to_fill":  gal,
                "total_card_cost":  round(s.get("diesel_price", 0) * gal, 2),
                "total_net_cost":   round(s.get("net_price", s.get("diesel_price", 0)) * gal, 2),
                "maps_url":         (f"https://maps.google.com/?q={s['latitude']},{s['longitude']}"
                                     if s.get("latitude") else None),
                "low_stop_warning": f"⚠️ Fill to {d['fill_to_pct']:.0f}% — last stop before {d['event'].state_name}",
            })

    # Filter out stops inside avoid states AND stops already covered by border strategy
    border_stop_names = {s["store_name"] for s in border_stops}
    filtered_stops = [s for s in planned_stops
                      if s.get("state", "").upper() not in avoid_states
                      and s.get("store_name") not in border_stop_names]

    # Merge: border stops + filtered regular stops, sorted by distance
    merged = sorted(border_stops + filtered_stops,
                    key=lambda s: s.get("dist_from_truck", 0))

    # Renumber
    for i, s in enumerate(merged, 1):
        s["stop_number"] = i
    planned_stops = merged

    return {
        "stops_needed":              len(planned_stops),
        "planned_stops":             planned_stops,
        "border_decisions":          border_decisions,
        "truck_lat":                 truck_lat,
        "truck_lng":                 truck_lng,
        "total_distance":            round(total_dist, 1),
        "total_card_cost":           round(total_card, 2),
        "total_net_cost":            round(total_net, 2),
        "warnings":                  warnings,       # only real warnings (low fuel etc)
        "border_warnings":           border_warnings, # sent separately
        "can_complete_without_stop": False,
        "emergency_mode":            emergency_mode,
        "critical_mode":             critical_mode,
        "ifta_enabled":              bool(IFTA_HOME_STATE),
        "ifta_home_state":           IFTA_HOME_STATE,
    }


def format_route_briefing(plan: dict, truck_name: str,
                           route: dict, fuel_pct: float, mpg: float) -> str:
    """Format the route briefing as a clean Telegram message."""
    if "error" in plan:
        return f"❌ Route plan error: {plan['error']}"

    origin = route.get("origin", {})
    dest   = route.get("destination", {})
    trip   = route.get("trip_num", "")

    o_city = f"{origin.get('city','?')}, {origin.get('state','')}"
    d_city = f"{dest.get('city','?')}, {dest.get('state','')}"

    lines = [
        f"*Route Fuel Plan - Truck {truck_name}*",
        f"Trip #{trip}  |  {o_city} -> {d_city}",
        f"{plan['total_distance']:.0f} miles  |  {fuel_pct:.0f}% fuel  |  {mpg:.1f} MPG",
        "",
    ]

    truck_lat = plan.get("truck_lat")
    truck_lng = plan.get("truck_lng")
    if truck_lat is not None and truck_lng is not None:
        lines.append(f"Current location: {truck_lat:.4f}, {truck_lng:.4f}")
        lines.append(f"https://maps.google.com/?q={truck_lat},{truck_lng}")
        lines.append("")

    if plan.get("emergency_mode"):
        lines.append("EMERGENCY: fuel is critically low. Route pricing is secondary to reaching fuel safely.")
        lines.append("")
    elif plan.get("critical_mode"):
        lines.append("CRITICAL: fuel is low. The first safe stop is prioritized before later cheaper options.")
        lines.append("")

    if plan["can_complete_without_stop"]:
        lines += [
            "*Truck has enough fuel for the full route.*",
            "No fuel stops needed.",
        ]
        return "\n".join(lines)

    if not plan["planned_stops"]:
        lines.append("No fuel stop recommendation is available for this route yet.")
        return "\n".join(lines)


    total = plan["stops_needed"]
    lines.append(f"⛽ *First fuel stop* (trip needs ~{total} stop{'s' if total > 1 else ''} total):")
    lines.append("")

    # Only show the FIRST stop — next stop sent when truck needs fuel
    for s in plan["planned_stops"][:1]:
        if s.get("low_stop_warning"):
            lines.append(s["low_stop_warning"])

        lines.append(f"*Stop {s['stop_number']} — {s['store_name']}*")
        lines.append(f"📌 {s['address']}, {s['city']}, {s['state']}")
        lines.append(f"🛣 {s['dist_from_truck']:.0f} mi from current position")

        if s.get("retail_price"):
            lines.append(f"💰 Retail: ${s['retail_price']:.3f}/gal")
        lines.append(f"💳 Card:   *${s['card_price']:.3f}/gal*")
        if plan.get("ifta_enabled"):
            if abs(s["total_net_cost"] - s["total_card_cost"]) >= 1:
                lines.append(
                    f"💵 Fill *{s['gallons_to_fill']:.0f} gal* → "
                    f"Pump: ${s['total_card_cost']:.0f} · "
                    f"Net after IFTA: *${s['total_net_cost']:.0f}*"
                )
            else:
                lines.append(
                    f"💵 Fill *{s['gallons_to_fill']:.0f} gal* → "
                    f"Estimated total: *${s['total_card_cost']:.0f}*"
                )
        else:
            lines.append(
                f"💵 Fill *{s['gallons_to_fill']:.0f} gal* → "
                f"Card total: *${s['total_card_cost']:.0f}*"
            )
            lines.append("📋 IFTA adjustment is off because `IFTA_HOME_STATE` is not set.")

        if s.get("maps_url"):
            lines.append(f"🗺 [Open in Google Maps]({s['maps_url']})")
        lines.append("")

    # Note about remaining stops
    if total > 1:
        lines += [
            "",
            f"📍 Next stop will be sent when fuel is needed.",
        ]

    return "\n".join(lines)


def format_next_stop(stop: dict, stop_num: int, total_stops: int,
                     truck_name: str, current_fuel_pct: float,
                     tank_gal: float = 150) -> str:
    """
    Format the next fuel stop alert — sent after truck refuels and needs the next stop.
    Simple and clean — one stop, all info driver needs.
    """
    NL = "\n"
    name   = stop.get("store_name", "Unknown")
    addr   = ", ".join(filter(None, [
        stop.get("address",""), stop.get("city",""), stop.get("state","")
    ]))
    dist   = stop.get("dist_from_truck", 0)
    card   = stop.get("card_price", 0)
    retail = stop.get("retail_price", 0)
    net    = stop.get("net_price", card)
    lat    = stop.get("latitude","")
    lng    = stop.get("longitude","")

    gallons   = round(tank_gal * (1 - current_fuel_pct / 100) * 0.9, 0)
    pump_cost = round(card * gallons, 0) if card else 0
    net_cost  = round(net  * gallons, 0) if net  else pump_cost

    lines = [
        f"⛽ *Next Fuel Stop — Truck {truck_name}*",
        f"Stop {stop_num} of {total_stops}",
        "",
        f"*{name}*",
    ]
    if addr:
        lines.append(f"📌 {addr}")
    if dist:
        lines.append(f"🛣 {dist:.0f} mi from current position")
    if retail and retail != card:
        lines.append(f"💰 Retail: ${retail:.3f}/gal")
    if card:
        lines.append(f"💳 Card:   *${card:.3f}/gal*")
    if pump_cost:
        if IFTA_HOME_STATE and abs(net_cost - pump_cost) > 1:
            lines.append(f"💵 Fill ~{gallons:.0f} gal → Pump: ${pump_cost:.0f} · Net after IFTA: *${net_cost:.0f}*")
        elif IFTA_HOME_STATE:
            lines.append(f"💵 Fill ~{gallons:.0f} gal → Estimated total: ${pump_cost:.0f}")
        else:
            lines.append(f"💵 Fill ~{gallons:.0f} gal → Card total: ${pump_cost:.0f}")
            lines.append("📋 IFTA adjustment is off because `IFTA_HOME_STATE` is not set.")
    if lat and lng:
        lines.append(f"🗺 [Open in Google Maps](https://maps.google.com/?q={lat},{lng})")

    return NL.join(lines)
