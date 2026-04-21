"""
main.py  -  FleetFuel Bot entry point.

Runs two concurrent loops:
  1. Samsara polling loop  (every 30 seconds tick, trucks polled per their schedule)
  2. Price updater         (daily at 06:00 UTC via simple time check)
"""

import logging
import time
import signal
import sys
import os
import re
from datetime import datetime, timedelta, timezone

from config import STATE_SAVE_INTERVAL_SECONDS
from database import init_db, load_all_truck_states, save_all_truck_states, reset_truck_states, auto_register_truck
from samsara_client import get_combined_vehicle_data
from state_machine import process_truck
import telegram_bot
from telegram_bot import send_startup_message, send_price_update_notification, poll_for_uploads

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# -- State --------------------------------------------------------------------
truck_states     = {}
_running         = True

# -- Graceful shutdown --------------------------------------------------------
def _shutdown(signum, frame):
    global _running
    log.info("Shutdown signal — saving state...")
    save_all_truck_states(truck_states)
    _running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# -- Helpers ------------------------------------------------------------------
def _utcnow():
    return datetime.now(timezone.utc)


def _truck_route_keys(vehicle_name: str) -> list[str]:
    """Possible QuickManage route keys for a Samsara vehicle name."""
    text = str(vehicle_name or "").strip()
    if not text:
        return []

    keys: list[str] = []

    def _add(value: str):
        value = str(value or "").strip()
        if value and value not in keys:
            keys.append(value)

    _add(text)

    for grp in re.findall(r"\d+", text):
        _add(grp)
        _add(grp.lstrip("0") or "0")

    first = text.split()[0] if text.split() else ""
    _add(first)
    return keys


# -- Price updater scheduler --------------------------------------------------
_last_price_update = None   # Track last update time

def _should_update_prices(now: datetime) -> bool:
    """Run price update once daily at 06:00 UTC."""
    global _last_price_update
    if _last_price_update is None:
        return True  # Always run on startup
    hours_since = (now - _last_price_update).total_seconds() / 3600
    return hours_since >= 23 and now.hour == 6



# -- Main loop ----------------------------------------------------------------
def main():
    global truck_states

    log.info("FleetFuel Bot starting up...")
    log.info("Initializing database...")
    init_db()
    if os.getenv("RESET_DB", "0") == "1":
        log.info("RESET_DB=1 — clearing truck states...")
        reset_truck_states()

    # Load persisted truck states
    truck_states = load_all_truck_states()
    log.info(f"Loaded {len(truck_states)} truck states from DB.")

    # Restore trip planning state from DB into truck_states
    # This ensures planned stops, briefing status survive restarts
    try:
        from database import load_all_trip_states
        trip_states = load_all_trip_states()
        for vname, tstate in trip_states.items():
            # Find truck_states entry by vehicle_name
            for vid, ts in truck_states.items():
                if ts.get("vehicle_name") == vname or ts.get("vname") == vname:
                    # Merge trip state into truck state — don't overwrite existing
                    for key, val in tstate.items():
                        if val is not None and key not in ts:
                            ts[key] = val
                    log.info(f"  Restored trip state for {vname}: "
                             f"briefing={tstate.get('briefing_sent_trip')!r} "
                             f"stop={tstate.get('assigned_stop_name')!r}")
                    break
        log.info(f"Trip states restored for {len(trip_states)} trucks.")
    except Exception as e:
        log.warning(f"Could not restore trip states: {e}")

    try:
        send_startup_message()
    except Exception as e:
        log.warning(f"Could not send startup message: {e}")

    log.info("Polling loop started.")

    last_db_save        = _utcnow()
    last_upload_check   = _utcnow()
    last_weekly_report  = _utcnow()
    last_route_fetch    = _utcnow() - timedelta(minutes=10)  # fetch immediately on start
    last_mpg_sync       = _utcnow() - timedelta(hours=2)     # sync MPG immediately on start
    last_ifta_check     = _utcnow() - timedelta(hours=25)    # check IFTA rates immediately on start
    last_ifta_update    = _utcnow() - timedelta(days=91)     # update IFTA rates immediately
    poll_cycle          = 0

    # Start background thread for QM route geocoding (slow — don't block alerts)
    import threading
    _route_lock = threading.Lock()

    def _fetch_routes_background():
        """Fetch + geocode QM routes in background. Saves to DB when done."""
        try:
            from config import QM_CLIENT_ID, QM_CLIENT_SECRET
            if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
                return
            from quickmanage_client import get_all_truck_routes
            routes = get_all_truck_routes()
            if routes:
                from database import save_truck_route
                for tn, route in routes.items():
                    save_truck_route(tn, "", route)
                log.info(f"Background: routes saved for {len(routes)} trucks")
        except Exception as e:
            log.warning(f"Background route fetch failed: {e}")

    def _update_ifta_background():
        """Auto-update IFTA rates when new quarter starts."""
        try:
            from ifta import should_update_rates, scrape_and_update_ifta_rates, get_rates_info
            if should_update_rates():
                log.info("IFTA: new quarter detected — updating rates...")
                result = scrape_and_update_ifta_rates()
                if result:
                    log.info(f"IFTA rates updated: {len(result)} states")
                    from telegram_bot import _send_to
                    from config import ADMIN_CHAT_ID
                    _send_to(ADMIN_CHAT_ID, f"📋 *IFTA rates auto-updated*\n{get_rates_info()}")
                else:
                    log.warning("IFTA auto-update failed — keeping current rates")
            else:
                log.info(f"IFTA rates current: {get_rates_info()}")
        except Exception as e:
            log.warning(f"IFTA update check failed: {e}")

    def _sync_mpg_background():
        """Sync real MPG and idle data from Samsara every hour."""
        try:
            from samsara_client import get_vehicle_fuel_efficiency, get_combined_vehicle_data
            from database import save_truck_efficiency
            efficiency = get_vehicle_fuel_efficiency()
            if not efficiency:
                log.warning("Background MPG sync: no data from Samsara fuel report")
                return
            # Get vehicle names
            vehicles = get_combined_vehicle_data()
            name_map = {v["vehicle_id"]: v["vehicle_name"] for v in vehicles}
            updated  = 0
            for vid, stats in efficiency.items():
                if stats.get("mpg") and stats["mpg"] > 3:
                    name = name_map.get(vid, vid)
                    save_truck_efficiency(
                        vehicle_id=vid, vehicle_name=name,
                        mpg=stats["mpg"], idle_hours=stats["idle_hours"],
                        idle_pct=stats["idle_pct"], fuel_gal=stats["fuel_gal"],
                    )
                    updated += 1
            log.info(f"Background MPG sync: updated {updated} trucks")
        except Exception as e:
            log.warning(f"Background MPG sync failed: {e}")

    while _running:
        try:
            poll_cycle += 1
            now = _utcnow()

            # -- Background route fetch (every 5 min) ------------------------
            if (now - last_route_fetch).total_seconds() >= 300:
                t = threading.Thread(target=_fetch_routes_background, daemon=True)
                t.start()
                last_route_fetch = now

            # -- IFTA rates update (every 90 days = quarterly) ---------------
            if (now - last_ifta_update).total_seconds() >= 7776000:
                def _update_ifta():
                    try:
                        from ifta import update_ifta_rates_from_web
                        ok = update_ifta_rates_from_web()
                        if ok:
                            from telegram_bot import _send_to
                            from config import ADMIN_CHAT_ID
                            _send_to(ADMIN_CHAT_ID, "✅ IFTA rates updated from official source.")
                    except Exception as e:
                        log.warning(f"IFTA update failed: {e}")
                threading.Thread(target=_update_ifta, daemon=True).start()
                last_ifta_update = now

            # -- IFTA rates check (every 24h — updates when new quarter starts) --
            if (now - last_ifta_check).total_seconds() >= 86400:
                t = threading.Thread(target=_update_ifta_background, daemon=True)
                t.start()
                last_ifta_check = now

            # -- Background MPG sync (every hour) -----------------------------
            if (now - last_mpg_sync).total_seconds() >= 3600:
                t = threading.Thread(target=_sync_mpg_background, daemon=True)
                t.start()
                last_mpg_sync = now

            # -- Weekly savings report (every Monday 08:00 UTC) --------------
            if (now - last_weekly_report).total_seconds() >= 3600:  # check every hour
                if now.weekday() == 0 and now.hour == 8:  # Monday 08:00 UTC
                    try:
                        from telegram_bot import send_weekly_savings_report, send_weekly_truck_report
                        send_weekly_savings_report()
                        send_weekly_truck_report()
                        last_weekly_report = now
                    except Exception as e:
                        log.error(f"Weekly report error: {e}")

            # -- Check for admin file uploads (every 30 seconds) --------------
            if (now - last_upload_check).total_seconds() >= 30:
                try:
                    poll_for_uploads()
                except Exception as e:
                    log.error(f"Upload poll error: {e}")
                last_upload_check = now

            # -- Fetch from Samsara -------------------------------------------
            try:
                all_trucks = get_combined_vehicle_data()
            except Exception as e:
                log.error(f"Samsara fetch failed: {e}")
                time.sleep(60)
                continue

            # -- Fetch routes from DB (pre-cached by background thread) ------
            # Routes are geocoded in background every 5 min — never blocks alerts
            try:
                from database import get_all_truck_routes_from_db
                qm_routes = get_all_truck_routes_from_db()
            except Exception as e:
                log.warning(f"DB route load failed: {e}")
                qm_routes = {}

            # -- Find trucks due for polling -----------------------------------
            due_trucks = []
            for truck in all_trucks:
                vid = truck["vehicle_id"]
                if vid not in truck_states:
                    # Brand new truck — register and process immediately
                    auto_register_truck(vid, truck["vehicle_name"])
                    log.info(f"New truck: {truck['vehicle_name']} — registered, processing now.")
                    due_trucks.append(truck)
                else:
                    # Force check bypasses next_poll entirely
                    if telegram_bot.force_check_now:
                        due_trucks.append(truck)
                        continue
                    next_poll = truck_states[vid].get("next_poll")
                    if next_poll is None:
                        due_trucks.append(truck)
                    else:
                        if next_poll.tzinfo is None:
                            next_poll = next_poll.replace(tzinfo=timezone.utc)
                        if next_poll <= now:
                            due_trucks.append(truck)

            if telegram_bot.force_check_now:
                import main as _main
                _main.force_check_now = False
                log.info(f"/checknow: forcing check on all {len(due_trucks)} trucks")

            log.info(f"Poll #{poll_cycle}: {len(all_trucks)} trucks  "
                     f"{len(due_trucks)} due for check")

            # -- Process due trucks -------------------------------------------
            for truck in due_trucks:
                vid = truck["vehicle_id"]
                # Attach QuickManage route to truck state if available
                vehicle_name = truck.get("vehicle_name", "")
                route = None
                matched_key = None
                for key in _truck_route_keys(vehicle_name):
                    if key in qm_routes:
                        route = qm_routes[key]
                        matched_key = key
                        break
                if route:
                    prev_trip = (truck_states.get(vid, {}) or {}).get("qm_route", {}).get("trip_num")
                    truck_states.setdefault(vid, {})["qm_route"] = route
                    if prev_trip != route.get("trip_num"):
                        log.info(
                            f"Attached QM route to {vehicle_name}: key={matched_key} "
                            f"trip={route.get('trip_num')} status={route.get('status')}"
                        )
                elif truck_states.get(vid, {}).get("qm_route"):
                    pass  # keep existing route
                try:
                    process_truck(vid, truck_states.get(vid, {}),
                                  truck, truck_states)
                    # Save immediately if an alert was fired — preserves msg_ids for deletion
                    if truck_states.get(vid, {}).get("alert_sent"):
                        from database import save_truck_state
                        save_truck_state(truck_states[vid])
                except Exception as e:
                    log.error(f"Error processing {truck['vehicle_name']}: {e}", exc_info=True)

            # -- Periodic DB save ---------------------------------------------
            if (now - last_db_save).total_seconds() >= STATE_SAVE_INTERVAL_SECONDS:
                save_all_truck_states(truck_states)
                last_db_save = now

        except Exception as e:
            log.error(f"Unhandled error in poll cycle: {e}", exc_info=True)

        time.sleep(30)

    log.info("FleetFuel Bot stopped cleanly.")


if __name__ == "__main__":
    main()
