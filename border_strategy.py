"""
border_strategy.py — Smart fuel strategy for expensive and low-stop states

Rules:
1. AVOID fueling in expensive states (CA, WA, OR, CT, MA, RI, NH)
2. Before entering expensive/low-stop state:
   - Calculate miles to exit that state
   - Fill exactly enough to exit + 15% buffer at last cheap stop before border
3. After each delivery stop → send new route briefing for remaining legs
4. Never recommend a stop inside an avoid state unless truck cannot exit on fuel

State categories:
  AVOID_STATES     — expensive, never fuel here if possible
  LOW_STOP_STATES  — sparse stops, must have enough to enter safely
  BOTH             — expensive AND sparse (MA, CT, RI, NH)
"""

from dataclasses import dataclass
from typing import Optional

# ── State definitions ──────────────────────────────────────────────────────

# States to AVOID fueling in — too expensive
AVOID_FUEL_STATES = {
    "CA": {"name": "California",    "avg_card_premium": 1.80},
    "WA": {"name": "Washington",    "avg_card_premium": 1.20},
    "OR": {"name": "Oregon",        "avg_card_premium": 1.10},
    "CT": {"name": "Connecticut",   "avg_card_premium": 0.90},
    "MA": {"name": "Massachusetts", "avg_card_premium": 0.85},
    "RI": {"name": "Rhode Island",  "avg_card_premium": 0.80},
    "NH": {"name": "New Hampshire", "avg_card_premium": 0.75},
    "NY": {"name": "New York",      "avg_card_premium": 0.70},
}

# States with very few truck stops — need minimum fuel to enter
LOW_STOP_STATES = {
    "MD": {"name": "Maryland",      "min_fuel_pct": 65, "typical_width_miles": 120},
    "NJ": {"name": "New Jersey",    "min_fuel_pct": 60, "typical_width_miles": 80},
    "PA": {"name": "Pennsylvania",  "min_fuel_pct": 55, "typical_width_miles": 300},
    "WY": {"name": "Wyoming",       "min_fuel_pct": 70, "typical_width_miles": 400},
    "MT": {"name": "Montana",       "min_fuel_pct": 70, "typical_width_miles": 550},
    "ID": {"name": "Idaho",         "min_fuel_pct": 65, "typical_width_miles": 300},
    "MA": {"name": "Massachusetts", "min_fuel_pct": 70, "typical_width_miles": 190},
    "CT": {"name": "Connecticut",   "min_fuel_pct": 70, "typical_width_miles": 100},
    "RI": {"name": "Rhode Island",  "min_fuel_pct": 75, "typical_width_miles": 60},
    "NH": {"name": "New Hampshire", "min_fuel_pct": 70, "typical_width_miles": 180},
    "VT": {"name": "Vermont",       "min_fuel_pct": 65, "typical_width_miles": 160},
    "ME": {"name": "Maine",         "min_fuel_pct": 65, "typical_width_miles": 320},
}

# Both expensive AND low stops — highest priority to avoid fueling inside
AVOID_AND_LOW_STOP = set(AVOID_FUEL_STATES.keys()) & set(LOW_STOP_STATES.keys())
# = CT, MA, RI, NH


@dataclass
class BorderEvent:
    """Represents an upcoming state border crossing on the route."""
    state:          str
    state_name:     str
    dist_to_entry:  float   # miles from current position to border entry
    dist_through:   float   # miles through this state (entry to exit or delivery)
    is_avoid:       bool    # expensive state — avoid fueling
    is_low_stop:    bool    # sparse stops — need min fuel
    min_fuel_pct:   float   # minimum fuel % required to enter
    exit_dist:      float   # total miles to exit this state safely
    has_delivery:   bool    # does route have a delivery INSIDE this state?


def analyze_route_borders(waypoints: list, truck_state: str = "") -> list[BorderEvent]:
    """
    Walk the route waypoints and identify upcoming state borders
    that require a fuel strategy decision.

    Only includes borders for states the route ACTUALLY passes through.
    Waypoints must have dist_from_truck set correctly (along-route distance).
    Returns list sorted by distance — closest first.
    """
    events     = []
    seen       = set()
    prev_state = truck_state.upper() if truck_state else ""

    for i, wp in enumerate(waypoints):
        wp_state = wp.get("state", "").upper()
        if not wp_state or wp_state == prev_state or wp_state in seen:
            prev_state = wp_state if wp_state else prev_state
            continue

        seen.add(wp_state)
        is_avoid    = wp_state in AVOID_FUEL_STATES
        is_low_stop = wp_state in LOW_STOP_STATES

        if not is_avoid and not is_low_stop:
            prev_state = wp_state
            continue

        dist_to_entry = wp.get("dist_from_truck", 0)

        # Only include borders that are actually ahead (positive distance)
        if dist_to_entry <= 0:
            prev_state = wp_state
            continue

        exit_dist    = LOW_STOP_STATES.get(wp_state, {}).get("typical_width_miles", 200)
        has_delivery = any(
            w.get("state", "").upper() == wp_state and w.get("is_delivery")
            for w in waypoints[i:]
        )
        state_info = AVOID_FUEL_STATES.get(wp_state) or LOW_STOP_STATES.get(wp_state, {})

        events.append(BorderEvent(
            state         = wp_state,
            state_name    = state_info.get("name", wp_state),
            dist_to_entry = dist_to_entry,
            dist_through  = exit_dist,
            is_avoid      = is_avoid,
            is_low_stop   = is_low_stop,
            min_fuel_pct  = LOW_STOP_STATES.get(wp_state, {}).get("min_fuel_pct", 50),
            exit_dist     = exit_dist,
            has_delivery  = has_delivery,
        ))
        prev_state = wp_state

    return sorted(events, key=lambda e: e.dist_to_entry)


def gallons_needed_to_exit(
    dist_through_state: float,
    tank_gal: float,
    mpg: float,
    buffer_pct: float = 0.15,
) -> float:
    """
    Calculate exactly how many gallons needed to exit a state + buffer.
    buffer_pct = 15% safety margin on top of theoretical range needed.
    """
    gal_to_exit  = dist_through_state / mpg
    gal_with_buf = gal_to_exit * (1 + buffer_pct)
    return round(min(gal_with_buf, tank_gal * 0.95), 1)  # cap at 95% full


def fuel_pct_needed_to_exit(
    dist_through_state: float,
    tank_gal: float,
    mpg: float,
    buffer_pct: float = 0.15,
) -> float:
    """Returns the fuel % level needed to safely enter and exit a state."""
    gal_needed = gallons_needed_to_exit(dist_through_state, tank_gal, mpg, buffer_pct)
    return round((gal_needed / tank_gal) * 100, 1)


def can_exit_on_current_fuel(
    current_fuel_pct: float,
    dist_through_state: float,
    tank_gal: float,
    mpg: float,
) -> bool:
    """Can truck enter and exit this state without fueling?"""
    range_miles = (current_fuel_pct / 100) * tank_gal * mpg * 0.85
    return range_miles >= dist_through_state


def find_last_stop_before_border(
    all_stops: list,
    dist_to_border: float,
    avoid_states: set,
    border_state: str = "",
    route_states: list = None,
    truck_lat: float = None,
    truck_lng: float = None,
    truck_heading: float = None,
) -> Optional[dict]:
    """
    Find the cheapest fuel stop within 100 miles BEFORE the border.

    Critical filters:
    1. Stop must be in a state the route passes through (not random far states)
    2. Stop must be AHEAD of the truck (positive along-track distance)
    3. Stop must not be in the avoid state itself
    4. Stop must be within 100 miles of the border entry point
    """
    from truck_stop_finder import haversine_miles, bearing, angle_diff
    import math

    # States the truck passes through before this border
    # If not provided, fall back to distance-based filtering
    allowed_states = set(route_states) if route_states else None

    window_start = max(0, dist_to_border - 100)
    window_end   = dist_to_border - 2

    candidates = []
    for s in all_stops:
        # Must have a price
        if not s.get("diesel_price"):
            continue

        stop_state = s.get("state", "").upper()

        # Never recommend a stop inside the avoid state
        if stop_state in avoid_states or stop_state == border_state.upper():
            continue

        # Must be in a state the route actually passes through
        if allowed_states and stop_state not in allowed_states:
            continue

        # Must be ahead of truck (not behind)
        dist = s.get("dist_from_truck", 0)
        if not (window_start <= dist <= window_end):
            continue

        # Double-check heading if we have coords
        if truck_lat and truck_lng and truck_heading is not None:
            slat = float(s.get("latitude", 0))
            slng = float(s.get("longitude", 0))
            if slat and slng:
                stop_bearing = bearing(truck_lat, truck_lng, slat, slng)
                adiff = angle_diff(truck_heading, stop_bearing)
                if adiff > 90:
                    continue  # stop is behind truck

        candidates.append(s)

    if not candidates:
        return None

    # Pick cheapest by IFTA net cost
    return min(candidates, key=lambda s: s.get("net_price", s.get("diesel_price", 99)))


def build_border_strategy(
    current_fuel_pct: float,
    tank_gal: float,
    mpg: float,
    border_events: list[BorderEvent],
    all_stops: list,
    route_waypoints: list = None,
    truck_lat: float = None,
    truck_lng: float = None,
    truck_heading: float = None,
) -> list[dict]:
    """
    For each upcoming border crossing, determine the fuel strategy:
    - Can truck coast through? → no stop needed
    - Must fuel before? → find last cheap stop before border, fill to exact amount

    Returns list of strategy decisions with stop recommendations.
    """
    decisions     = []
    fuel_sim      = current_fuel_pct   # simulate fuel level as we plan ahead
    dist_position = 0.0

    for event in border_events:
        # Simulate fuel at border entry
        dist_driven   = event.dist_to_entry - dist_position
        fuel_consumed = (dist_driven / mpg / tank_gal) * 100
        fuel_at_entry = fuel_sim - fuel_consumed

        can_exit  = can_exit_on_current_fuel(
            fuel_at_entry, event.dist_through, tank_gal, mpg
        )
        min_entry = fuel_pct_needed_to_exit(
            event.dist_through, tank_gal, mpg
        )

        if can_exit and fuel_at_entry >= event.min_fuel_pct:
            # ✅ Truck can coast through
            decisions.append({
                "event":        event,
                "action":       "coast",
                "reason":       f"Enough fuel to enter and exit {event.state_name}",
                "stop":         None,
                "fill_to_pct":  None,
                "fill_gallons": None,
            })
            # Update sim — truck exits at lower fuel
            fuel_exit     = fuel_at_entry - (event.dist_through / mpg / tank_gal * 100)
            fuel_sim      = max(fuel_exit, 5)
            dist_position = event.dist_to_entry + event.dist_through

        else:
            # ⚠️ Need to fuel before border
            fill_to_pct = max(min_entry, event.min_fuel_pct) + 5
            fill_to_pct = min(fill_to_pct, 95)

            # Gallons needed = fill from current fuel level to fill_to_pct
            # Use CURRENT fuel sim — capped at 0 minimum
            fuel_now_pct  = max(fuel_sim, 5)
            gal_to_add    = round(((fill_to_pct - fuel_now_pct) / 100) * tank_gal, 1)
            gal_to_add    = max(min(gal_to_add, tank_gal * 0.95), 0)  # cap at tank size

            # Find best stop before border
            # Build list of states truck passes through before this border
            route_states = []
            if route_waypoints:
                for wp in route_waypoints:
                    wp_state = wp.get("state", "").upper()
                    if wp_state and wp_state not in AVOID_FUEL_STATES and wp_state not in LOW_STOP_STATES:
                        route_states.append(wp_state)
                    if wp.get("dist_from_truck", 0) >= event.dist_to_entry:
                        break  # only states before the border

            stop = find_last_stop_before_border(
                all_stops,
                dist_to_border=event.dist_to_entry,
                avoid_states=set(AVOID_FUEL_STATES.keys()),
                border_state=event.state,
                route_states=route_states if route_states else None,
                truck_lat=truck_lat,
                truck_lng=truck_lng,
                truck_heading=truck_heading,
            )

            # ── REACHABILITY CHECK ─────────────────────────────────────────────
            # Can truck physically REACH this pre-border stop on current fuel?
            # Use 85% safety margin (same as other range calcs).
            can_reach_stop = True
            if stop:
                stop_dist = stop.get("dist_from_truck", event.dist_to_entry)
                # fuel_sim is fuel at CURRENT dist_position, so gallons needed
                # is from here to the stop
                miles_to_stop = max(stop_dist - dist_position, 0)
                range_on_sim  = (fuel_sim / 100.0) * tank_gal * mpg * 0.85
                can_reach_stop = range_on_sim >= miles_to_stop
            # If truck would arrive at pre-border stop too empty to be safe,
            # the route_briefing planner must insert an earlier emergency stop.
            needs_earlier_stop = (fuel_sim < 10) or (not can_reach_stop)

            decisions.append({
                "event":              event,
                "action":              "fuel_before_border",
                "reason":              (
                    f"Must enter {event.state_name} with ≥{fill_to_pct:.0f}% fuel"
                    + (" — expensive state" if event.is_avoid else "")
                    + (" — very few truck stops" if event.is_low_stop else "")
                ),
                "stop":                stop,
                "fill_to_pct":         fill_to_pct,
                "fill_gallons":        gal_to_add,
                "fuel_sim_at_stop":    round(fuel_sim, 1),  # for diagnostics
                "needs_earlier_stop":  needs_earlier_stop,
            })

            # Simulate after fill
            fuel_sim      = fill_to_pct
            fuel_exit     = fuel_sim - (event.dist_through / mpg / tank_gal * 100)
            fuel_sim      = max(fuel_exit, 5)
            dist_position = event.dist_to_entry + event.dist_through

    return decisions


def format_border_warnings(decisions: list[dict],
                            approaching_miles: float = 9999) -> list[str]:
    """
    Format border strategy decisions as warning lines.
    approaching_miles: only show low-stop warnings when within this distance.
    For route briefing (trip start) — only show fuel_before_border decisions.
    For approaching alerts — show when within 150 miles.
    """
    lines = []
    for d in decisions:
        event = d["event"]

        if d["action"] == "coast":
            lines.append(
                f"✅ {event.state_name} ({event.state}) — "
                f"enough fuel to coast through, no stop needed"
            )

        elif d["action"] == "fuel_before_border":
            reasons = []
            if event.is_avoid:
                premium = AVOID_FUEL_STATES[event.state].get("avg_card_premium", 0)
                reasons.append(f"diesel ~${premium:.2f}/gal more expensive")
            if event.is_low_stop:
                reasons.append(f"very few truck stops inside")
            reason_str = " · ".join(reasons)

            lines.append(f"")
            lines.append(
                f"⚠️ *{event.state_name} ({event.state}) ahead* — {reason_str}"
            )
            lines.append(
                f"   Fuel up before crossing — check your route briefing for the recommended stop."
            )

    return lines
