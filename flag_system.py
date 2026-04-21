"""
flag_system.py — Flag drivers when they deviate from fuel recommendations

Three flag types:
  WRONG_STOP   — Driver fueled at different stop than recommended
  MISSED_STOP  — Driver passed recommended stop without fueling
  LOW_STOP_STATE — Truck entered low-stop state (MD/NJ/PA etc) below safe fuel level

Flags are sent instantly to driver group + dispatcher group.
Also stored in DB for weekly owner report.

Schema note: the driver_flags table is created by database.py.init_db().
This module does NOT re-create it — a split schema caused silent NULLs in
card_price / savings_lost before.
"""

import logging
from database import db_cursor
from config import DISPATCHER_GROUP_ID

log = logging.getLogger(__name__)

FLAG_WRONG_STOP    = "WRONG_STOP"
FLAG_MISSED_STOP   = "MISSED_STOP"
FLAG_LOW_STOP_STATE = "LOW_STOP_STATE"


def save_flag(vehicle_name: str, flag_type: str, details: str,
              recommended_stop: str = None, actual_stop: str = None,
              fuel_pct: float = None, state: str = None,
              card_price: float = None, savings_lost: float = None) -> int:
    """Save a flag to DB. Returns flag ID.

    card_price and savings_lost default to NULL. For MISSED_STOP we fill in
    a projected savings_lost at insert time so weekly reports have a number
    even if the driver hasn't refueled yet. state_machine.py overwrites it
    with the real loss once the driver actually fuels elsewhere.
    """
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO driver_flags
                (vehicle_name, flag_type, details, recommended_stop,
                 actual_stop, fuel_pct, state, card_price, savings_lost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (vehicle_name, flag_type, details,
               recommended_stop, actual_stop, fuel_pct, state,
               card_price, savings_lost))
        return cur.fetchone()["id"]


def send_flag(vehicle_name: str, flag_type: str, message: str,
              truck_group_id: str = None) -> None:
    """Send flag alert to driver group + dispatcher group."""
    from telegram_bot import _send_to, _send_to_dispatcher

    if truck_group_id:
        _send_to(truck_group_id, message)
    _send_to_dispatcher(message)
    log.warning(f"FLAG [{flag_type}] Truck {vehicle_name}: {message[:100]}")


def flag_wrong_stop(vehicle_name: str, truck_group_id: str,
                     recommended: str, actual: str,
                     fuel_before: float, fuel_after: float,
                     recommended_card_price: float = None,
                     actual_card_price: float = None,
                     tank_gal: float = 150) -> None:
    """Driver fueled at a different stop than recommended.

    If both card prices are known, we can compute the real savings_lost on
    this very flag (no waiting for a refuel to back-fill).
    """
    # Estimate savings lost if we have both prices
    savings_lost = None
    if recommended_card_price and actual_card_price and fuel_before is not None:
        gallons_added = round(tank_gal * max(fuel_after - fuel_before, 0) / 100, 1)
        if gallons_added > 0 and actual_card_price > recommended_card_price:
            savings_lost = round(
                (actual_card_price - recommended_card_price) * gallons_added, 2
            )

    savings_line = (f"\n💸 *Savings lost: ${savings_lost:.2f}*"
                    if savings_lost else "")
    msg = (
        f"🚩 *Flag — Truck {vehicle_name}*\n"
        f"Type: *Wrong Fuel Stop*\n\n"
        f"✅ Recommended: *{recommended}*\n"
        f"❌ Actual stop: *{actual}*\n"
        f"⛽ Fuel: {fuel_before:.0f}% → {fuel_after:.0f}%"
        f"{savings_line}\n\n"
        f"Driver did not follow the fuel recommendation."
    )
    save_flag(vehicle_name, FLAG_WRONG_STOP, msg,
              recommended_stop=recommended, actual_stop=actual,
              fuel_pct=fuel_before,
              card_price=recommended_card_price,
              savings_lost=savings_lost)
    send_flag(vehicle_name, FLAG_WRONG_STOP, msg, truck_group_id)


def flag_missed_stop(vehicle_name: str, truck_group_id: str,
                      stop_name: str, dist_past: float,
                      fuel_pct: float, tank_gal: float = 150,
                      card_price: float = None,
                      net_price: float = None) -> None:
    """Driver passed the recommended stop without fueling.

    Records a PROJECTED savings_lost based on a conservative estimate of
    what the driver will pay elsewhere. state_machine.py overwrites this
    with the real loss once the driver actually refuels.
    """
    savings_lost_line = ""
    projected_loss = None
    if card_price and fuel_pct and tank_gal:
        gallons_needed  = round(tank_gal * (1 - fuel_pct / 100), 1)
        cost_at_rec     = round(card_price * gallons_needed, 2)
        net_at_rec      = round((net_price if net_price else card_price) * gallons_needed, 2)
        # Projected loss: assume driver pays ~$0.15/gal more at a random stop.
        # Real loss overwrites this on refuel (state_machine.py).
        projected_loss  = round(0.15 * gallons_needed, 2) if gallons_needed > 0 else None
        savings_lost_line = (
            f"\n💸 *Would have cost: ${cost_at_rec:.0f}* at recommended stop"
            f"\n   (${card_price:.3f}/gal × {gallons_needed:.0f} gal)"
            f"\n⏳ Actual loss calculated when driver fuels elsewhere..."
        )

    msg = (
        f"🚩 *Flag — Truck {vehicle_name}*\n"
        f"Type: *Missed Recommended Stop*\n\n"
        f"❌ Passed: *{stop_name}*\n"
        f"📏 Now {dist_past:.0f} miles past the stop\n"
        f"⛽ Current fuel: *{fuel_pct:.0f}%*"
        f"{savings_lost_line}\n\n"
        f"Finding next available stop ahead..."
    )
    save_flag(vehicle_name, FLAG_MISSED_STOP, msg,
              recommended_stop=stop_name, fuel_pct=fuel_pct,
              card_price=card_price, savings_lost=projected_loss)
    send_flag(vehicle_name, FLAG_MISSED_STOP, msg, truck_group_id)


def flag_low_stop_state(vehicle_name: str, truck_group_id: str,
                         state: str, state_name: str,
                         fuel_pct: float, min_fuel: float) -> None:
    """Truck entered a low-stop state below safe fuel level."""
    msg = (
        f"🚩 *Flag — Truck {vehicle_name}*\n"
        f"Type: *Entered Low-Stop State Under-Fueled*\n\n"
        f"📍 Entered: *{state_name} ({state})*\n"
        f"⛽ Fuel level: *{fuel_pct:.0f}%* (minimum recommended: {min_fuel:.0f}%)\n\n"
        f"⚠️ Very few truck stops in {state_name}. "
        f"Driver should have fueled before crossing the border."
    )
    save_flag(vehicle_name, FLAG_LOW_STOP_STATE, msg,
              fuel_pct=fuel_pct, state=state)
    send_flag(vehicle_name, FLAG_LOW_STOP_STATE, msg, truck_group_id)


def get_flags_summary(days: int = 7) -> dict:
    """Get flag summary for weekly report."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        cur.execute("""
            SELECT flag_type, COUNT(*) as cnt,
                   array_agg(vehicle_name ORDER BY flagged_at DESC) as trucks
            FROM driver_flags
            WHERE flagged_at >= %s
            GROUP BY flag_type
            ORDER BY cnt DESC
        """, (since,))
        rows = cur.fetchall()
    result = {}
    for r in rows:
        result[r["flag_type"]] = {
            "count": r["cnt"],
            "trucks": list(set(r["trucks"]))[:5]
        }
    return result
