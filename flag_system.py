"""
flag_system.py - Flag drivers when they deviate from fuel recommendations.

Three flag types:
  WRONG_STOP - Driver fueled at a different stop than recommended
  MISSED_STOP - Driver passed recommended stop without fueling
  LOW_STOP_STATE - Truck entered a low-stop state below safe fuel level

Flags are sent instantly to driver group + dispatcher group and stored in DB.
"""

import logging
from database import db_cursor
from config import DISPATCHER_GROUP_ID

log = logging.getLogger(__name__)

FLAG_WRONG_STOP = "WRONG_STOP"
FLAG_MISSED_STOP = "MISSED_STOP"
FLAG_LOW_STOP_STATE = "LOW_STOP_STATE"


def _ensure_flags_table():
    """Create flags table if not exists."""
    with db_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS driver_flags (
                id               SERIAL PRIMARY KEY,
                vehicle_name     TEXT NOT NULL,
                flag_type        TEXT NOT NULL,
                details          TEXT,
                recommended_stop TEXT,
                actual_stop      TEXT,
                fuel_pct         REAL,
                state            TEXT,
                flagged_at       TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )


def save_flag(
    vehicle_name: str,
    flag_type: str,
    details: str,
    recommended_stop: str = None,
    actual_stop: str = None,
    fuel_pct: float = None,
    state: str = None,
) -> int:
    """Save a flag to DB. Returns flag ID."""
    _ensure_flags_table()
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO driver_flags
                (vehicle_name, flag_type, details, recommended_stop,
                 actual_stop, fuel_pct, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (vehicle_name, flag_type, details, recommended_stop, actual_stop, fuel_pct, state),
        )
        return cur.fetchone()["id"]


def send_flag(vehicle_name: str, flag_type: str, message: str, truck_group_id: str = None) -> None:
    """Send flag alert to driver group + dispatcher group."""
    from telegram_bot import _send_to, _send_to_dispatcher

    if truck_group_id:
        _send_to(truck_group_id, message)
    _send_to_dispatcher(message)
    log.warning(f"FLAG [{flag_type}] Truck {vehicle_name}: {message[:100]}")


def flag_wrong_stop(
    vehicle_name: str,
    truck_group_id: str,
    recommended: str,
    actual: str,
    fuel_before: float,
    fuel_after: float,
) -> None:
    """Driver fueled at a different stop than recommended."""
    msg = (
        f"🚩 *Flag Alert — Truck {vehicle_name}*\n"
        f"🧾 Type: *Wrong Fuel Stop*\n\n"
        f"✅ Recommended stop: *{recommended}*\n"
        f"❌ Actual stop: *{actual}*\n"
        f"⛽ Fuel: {fuel_before:.0f}% → {fuel_after:.0f}%\n\n"
        f"⚠️ Driver did not follow the fuel recommendation."
    )
    save_flag(
        vehicle_name,
        FLAG_WRONG_STOP,
        msg,
        recommended_stop=recommended,
        actual_stop=actual,
        fuel_pct=fuel_before,
    )
    send_flag(vehicle_name, FLAG_WRONG_STOP, msg, truck_group_id)


def flag_missed_stop(
    vehicle_name: str,
    truck_group_id: str,
    stop_name: str,
    dist_past: float,
    fuel_pct: float,
    tank_gal: float = 150,
    card_price: float = None,
    net_price: float = None,
) -> None:
    """Driver passed the recommended stop without fueling."""
    savings_lost_line = ""
    if card_price and fuel_pct and tank_gal:
        gallons_needed = round(tank_gal * (1 - fuel_pct / 100), 1)
        cost_at_rec = round(card_price * gallons_needed, 2)
        net_at_rec = round((net_price if net_price is not None else card_price) * gallons_needed, 2)
        price_label = "Net after IFTA" if net_price is not None else "Card total"
        shown_total = net_at_rec if net_price is not None else cost_at_rec
        savings_lost_line = (
            f"\n💵 *{price_label}: ${shown_total:.0f}* at recommended stop"
            f"\n📊 Rate: ${card_price:.3f}/gal × {gallons_needed:.0f} gal"
            f"\n⏳ Actual loss will be calculated when driver fuels elsewhere..."
        )

    msg = (
        f"🚩 *Flag Alert — Truck {vehicle_name}*\n"
        f"🧾 Type: *Missed Recommended Stop*\n\n"
        f"❌ Passed stop: *{stop_name}*\n"
        f"📏 Distance past stop: {dist_past:.0f} miles\n"
        f"⛽ Current fuel: *{fuel_pct:.0f}%*"
        f"{savings_lost_line}\n\n"
        f"🔎 Finding next available stop ahead..."
    )
    save_flag(
        vehicle_name,
        FLAG_MISSED_STOP,
        msg,
        recommended_stop=stop_name,
        fuel_pct=fuel_pct,
    )
    send_flag(vehicle_name, FLAG_MISSED_STOP, msg, truck_group_id)


def flag_low_stop_state(
    vehicle_name: str,
    truck_group_id: str,
    state: str,
    state_name: str,
    fuel_pct: float,
    min_fuel: float,
) -> None:
    """Truck entered a low-stop state below safe fuel level."""
    msg = (
        f"🚩 *Flag Alert — Truck {vehicle_name}*\n"
        f"🧾 Type: *Entered Low-Stop State Under-Fueled*\n\n"
        f"📍 Entered state: *{state_name} ({state})*\n"
        f"⛽ Fuel level: *{fuel_pct:.0f}%* (minimum recommended: {min_fuel:.0f}%)\n\n"
        f"⚠️ Very few truck stops in {state_name}.\n"
        f"⛽ Driver should have fueled before crossing the border."
    )
    save_flag(
        vehicle_name,
        FLAG_LOW_STOP_STATE,
        msg,
        fuel_pct=fuel_pct,
        state=state,
    )
    send_flag(vehicle_name, FLAG_LOW_STOP_STATE, msg, truck_group_id)


def get_flags_summary(days: int = 7) -> dict:
    """Get flag summary for weekly report."""
    _ensure_flags_table()
    from datetime import datetime, timezone, timedelta

    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT flag_type, COUNT(*) as cnt,
                   array_agg(vehicle_name ORDER BY flagged_at DESC) as trucks
            FROM driver_flags
            WHERE flagged_at >= %s
            GROUP BY flag_type
            ORDER BY cnt DESC
            """,
            (since,),
        )
        rows = cur.fetchall()

    result = {}
    for row in rows:
        result[row["flag_type"]] = {
            "count": row["cnt"],
            "trucks": list(set(row["trucks"]))[:5],
        }
    return result
