"""
route_planner.py — Full route fuel planning from A to B

Given a QM route (shipper → receiver with all stops),
find the best fuel stops along the entire route considering:
1. IFTA-adjusted net price per gallon
2. Low truck-stop states ahead (fuel up before entering)
3. Multiple delivery stops in order
4. Truck's actual MPG from Samsara
5. Tank capacity and current fuel level

Returns a complete fuel plan for the entire trip.
"""

import math
import logging
from database import get_all_diesel_stops, get_truck_mpg
from truck_stop_finder import haversine_miles, bearing, angle_diff, gallons_to_fill
from ifta import (
    best_stop_after_ifta, check_low_stop_states_ahead,
    get_ifta_rate, net_price_after_ifta, format_ifta_savings,
    LOW_STOP_STATES, LOW_STOP_MIN_FUEL,
)
from config import DEFAULT_TANK_GAL

log = logging.getLogger(__name__)

# How far ahead to look for fuel stops
PLAN_LOOKAHEAD_MILES = 150

# Minimum fuel % to enter a normal state
MIN_FUEL_NORMAL = 25

# Search corridor width for route planning
CORRIDOR_MILES = 50.0


def _stops_between(from_lat, from_lng, to_lat, to_lng,
                   all_stops, corridor_miles=CORRIDOR_MILES) -> list[dict]:
    """
    Find all fuel stops between two points within corridor_miles of the route line.
    Returns stops sorted by distance from origin.
    """
    route_bearing = bearing(from_lat, from_lng, to_lat, to_lng)
    total_dist    = haversine_miles(from_lat, from_lng, to_lat, to_lng)

    candidates = []
    for stop in all_stops:
        slat = float(stop["latitude"])
        slng = float(stop["longitude"])

        if not stop.get("diesel_price"):
            continue

        dist_from_origin = haversine_miles(from_lat, from_lng, slat, slng)
        if dist_from_origin > total_dist * 1.1 + 20:
            continue

        stop_bear = bearing(from_lat, from_lng, slat, slng)
        adiff     = angle_diff(route_bearing, stop_bear)

        # Must be ahead
        along = dist_from_origin * math.cos(math.radians(adiff))
        cross = abs(dist_from_origin * math.sin(math.radians(adiff)))

        if along <= 0 or adiff > 90:
            continue
        if cross > corridor_miles:
            continue

        # Add IFTA data
        state    = stop.get("state", "")
        rate     = get_ifta_rate(state)
        net      = net_price_after_ifta(stop["diesel_price"], state)

        candidates.append({
            **stop,
            "dist_from_origin": round(along, 1),
            "ifta_rate":        rate,
            "net_price":        net,
        })

    return sorted(candidates, key=lambda s: s["dist_from_origin"])


def plan_route_fuel(
    truck_lat: float,
    truck_lng: float,
    current_fuel_pct: float,
    vehicle_id: str,
    route: dict,
    tank_gal: float = DEFAULT_TANK_GAL,
) -> dict:
    """
    Plan fuel stops for the entire route from current position to final destination.

    Returns:
    {
        "segments": [
            {
                "from": "Chicago, IL",
                "to": "St. Louis, MO",
                "distance_miles": 300,
                "fuel_needed_gal": 46,
                "recommended_stop": {...stop dict with ifta data...},
                "all_options": [...top 3 stops...],
                "low_stop_warning": None or {"state": "MD", ...},
            },
            ...
        ],
        "total_distance": 2100,
        "total_fuel_cost_pump": 450.20,
        "total_fuel_cost_ifta": 412.30,
        "total_ifta_savings": 37.90,
        "warnings": ["⚠️ Entering MD in 180mi — fuel up before crossing into MD"],
    }
    """
    # Get real MPG for this truck
    mpg = get_truck_mpg(vehicle_id)
    if mpg < 3.0:
        mpg = 6.5  # sanity check

    all_stops = get_all_diesel_stops()
    stops     = route.get("stops", [])
    warnings  = []

    # Build waypoints: current position + all route stops with coords
    waypoints = [{"lat": truck_lat, "lng": truck_lng, "city": "Current position", "state": ""}]
    for s in stops:
        if s.get("lat") and s.get("lng"):
            waypoints.append({
                "lat":     s["lat"],
                "lng":     s["lng"],
                "city":    s.get("city", ""),
                "state":   s.get("state", ""),
                "company": s.get("company_name", ""),
                "pickup":  s.get("pickup", False),
            })

    if len(waypoints) < 2:
        return {"error": "Not enough route waypoints"}

    segments          = []
    current_fuel_pct_ = current_fuel_pct
    total_pump_cost   = 0
    total_ifta_cost   = 0
    total_distance    = 0

    # Check low-stop states ahead on entire route
    low_stop_warnings = check_low_stop_states_ahead(route, waypoints[1].get("state",""))

    for i in range(len(waypoints) - 1):
        wp_from = waypoints[i]
        wp_to   = waypoints[i + 1]

        seg_dist = haversine_miles(wp_from["lat"], wp_from["lng"],
                                   wp_to["lat"], wp_to["lng"])
        total_distance += seg_dist

        fuel_needed_gal = seg_dist / mpg
        fuel_needed_pct = (fuel_needed_gal / tank_gal) * 100

        from_label = f"{wp_from.get('city','')}, {wp_from.get('state','')}"
        to_label   = f"{wp_to.get('city','')}, {wp_to.get('state','')}"

        # Find fuel stops on this segment
        seg_stops = _stops_between(
            wp_from["lat"], wp_from["lng"],
            wp_to["lat"], wp_to["lng"],
            all_stops,
        )

        # Apply IFTA sorting
        seg_stops_ifta = best_stop_after_ifta(seg_stops) if seg_stops else []

        # Check if entering a low-stop state on this segment
        to_state = wp_to.get("state", "").upper()
        low_stop_warn = None
        if to_state in LOW_STOP_STATES:
            min_fuel = LOW_STOP_MIN_FUEL.get(to_state, 50)
            if current_fuel_pct_ < min_fuel:
                low_stop_warn = {
                    "state":    to_state,
                    "name":     LOW_STOP_STATES[to_state]["name"],
                    "min_fuel": min_fuel,
                    "reason":   LOW_STOP_STATES[to_state]["reason"],
                }
                warnings.append(
                    f"⚠️ Truck needs ≥{min_fuel}% fuel before entering {LOW_STOP_STATES[to_state]['name']} "
                    f"({LOW_STOP_STATES[to_state]['reason']})"
                )

        # Best stop = lowest IFTA-adjusted net price
        best_stop = seg_stops_ifta[0] if seg_stops_ifta else None
        top3      = seg_stops_ifta[:3]

        # Calculate costs
        fill_gal = gallons_to_fill(current_fuel_pct_, tank_gal)
        if best_stop and fill_gal > 0:
            pump_cost = round(best_stop["diesel_price"] * fill_gal, 2)
            ifta_cost = round(best_stop["net_price"] * fill_gal, 2)
            total_pump_cost += pump_cost
            total_ifta_cost += ifta_cost
        else:
            pump_cost = 0
            ifta_cost = 0

        segments.append({
            "from":              from_label,
            "to":                to_label,
            "distance_miles":    round(seg_dist, 1),
            "fuel_needed_gal":   round(fuel_needed_gal, 1),
            "fuel_needed_pct":   round(fuel_needed_pct, 1),
            "current_fuel_pct":  round(current_fuel_pct_, 1),
            "recommended_stop":  best_stop,
            "all_options":       top3,
            "low_stop_warning":  low_stop_warn,
            "pump_cost":         pump_cost,
            "ifta_adjusted_cost": ifta_cost,
            "mpg_used":          round(mpg, 2),
        })

        # Update fuel level for next segment (assume truck fuels at recommended stop)
        if best_stop:
            current_fuel_pct_ = 90.0  # assume filled to 90%
        else:
            current_fuel_pct_ = max(0, current_fuel_pct_ - fuel_needed_pct)

    return {
        "segments":            segments,
        "total_distance":      round(total_distance, 1),
        "total_fuel_cost_pump": round(total_pump_cost, 2),
        "total_fuel_cost_ifta": round(total_ifta_cost, 2),
        "total_ifta_savings":   round(total_pump_cost - total_ifta_cost, 2),
        "mpg":                  mpg,
        "warnings":             warnings,
        "low_stop_warnings":    low_stop_warnings,
    }


def format_route_plan(plan: dict, truck_name: str) -> str:
    """Format the route fuel plan as a Telegram message."""
    if "error" in plan:
        return f"❌ Route plan error: {plan['error']}"

    lines = [
        f"🗺 *Route Fuel Plan — Truck {truck_name}*",
        f"📏 Total distance: {plan['total_distance']:.0f} miles",
        f"⚡ MPG: {plan['mpg']:.1f}",
        f"💰 Pump cost: ${plan['total_fuel_cost_pump']:.2f}",
        f"📋 After IFTA: ${plan['total_fuel_cost_ifta']:.2f}",
        f"💵 IFTA savings: *${plan['total_ifta_savings']:.2f}*",
        "",
    ]

    # Warnings first
    for w in plan.get("warnings", []):
        lines.append(w)
    if plan.get("warnings"):
        lines.append("")

    # Segments
    for i, seg in enumerate(plan["segments"], 1):
        lines.append(f"📍 *Segment {i}: {seg['from']} → {seg['to']}*")
        lines.append(f"   📏 {seg['distance_miles']:.0f} mi | ⛽ needs {seg['fuel_needed_gal']:.0f} gal")

        if seg.get("low_stop_warning"):
            w = seg["low_stop_warning"]
            lines.append(f"   ⚠️ *Fuel up before {w['name']}!* Need ≥{w['min_fuel']}% — {w['reason']}")

        if seg["recommended_stop"]:
            stop  = seg["recommended_stop"]
            name  = stop["store_name"]
            addr  = f"{stop.get('city','')}, {stop.get('state','')}"
            pump  = stop["diesel_price"]
            net   = stop["net_price"]
            rate  = stop["ifta_rate"]
            dist  = stop.get("dist_from_origin", 0)
            lines += [
                f"   ⛽ *Best stop:* {name}",
                f"   📌 {addr} | {dist:.0f} mi into segment",
                f"   💰 Pump: ${pump:.3f} | IFTA rate: ${rate:.3f} | Net: *${net:.3f}*",
            ]

            # Show top 3
            if len(seg["all_options"]) > 1:
                lines.append("   *Other options:*")
                for opt in seg["all_options"][1:3]:
                    lines.append(
                        f"   • {opt['store_name']} {opt.get('city','')} "
                        f"${opt['diesel_price']:.3f} (net ${opt['net_price']:.3f})"
                    )
        else:
            lines.append("   ❌ No fuel stops found on this segment")

        lines.append("")

    return "\n".join(lines)
