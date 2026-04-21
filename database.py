"""
database.py  -  PostgreSQL connection + schema + all queries.

Tables:
  trucks          - maps Samsara vehicle_name → telegram_group_id
  fuel_stops      - all Pilot + Love's locations with diesel prices
  truck_states    - current state of each truck (persisted across restarts)
  fuel_alerts     - one row per low-fuel alert event
  ca_reminders    - tracks California border reminders sent (cooldown)
  bot_config      - key/value store for config (pilot locations cache etc.)
"""

import logging
import time
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from datetime import datetime
from config import DATABASE_URL

log = logging.getLogger(__name__)


# -- Connection ---------------------------------------------------------------

def get_connection(retries: int = 3, delay: float = 2.0):
    """Connect to PostgreSQL with automatic retry on connection failure."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                DATABASE_URL,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            log.warning(f"DB connection attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_err


@contextmanager
def db_cursor():
    """Yields a dict cursor; commits on success, rolls back on error."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -- Schema -------------------------------------------------------------------

SCHEMA_SQL = """
-- trucks: manually inserted to map Samsara name → Telegram group
CREATE TABLE IF NOT EXISTS trucks (
    id                  SERIAL PRIMARY KEY,
    vehicle_name        TEXT    NOT NULL UNIQUE,
    telegram_group_id   TEXT,
    tank_capacity_gal   REAL    NOT NULL DEFAULT 150,
    avg_mpg             REAL    NOT NULL DEFAULT 6.5,
    tank_size_known     BOOLEAN NOT NULL DEFAULT FALSE,
    mpg_known           BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_stops: loaded daily from EFS price CSV
-- Columns: Station, Address, City, State, longitude, latitude, Retail price, Discounted price
-- Drop old fuel_stops table if it has wrong schema, recreate clean
DO $$
BEGIN
    -- Check if old schema (has 'source' column) — drop and recreate
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fuel_stops' AND column_name = 'source'
    ) THEN
        DROP TABLE IF EXISTS fuel_stops CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fuel_stops (
    id               SERIAL PRIMARY KEY,
    station_name     TEXT    NOT NULL,
    address          TEXT,
    city             TEXT,
    state            TEXT    NOT NULL,
    longitude        FLOAT   NOT NULL,
    latitude         FLOAT   NOT NULL,
    retail_price     REAL,
    discounted_price REAL,
    price_updated    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_name, city, state)
);

CREATE INDEX IF NOT EXISTS idx_fuel_stops_location ON fuel_stops (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_state    ON fuel_stops (state);

-- truck_states: full state persisted to survive Railway redeploys
CREATE TABLE IF NOT EXISTS truck_states (
    vehicle_id              TEXT PRIMARY KEY,
    vehicle_name            TEXT,
    state                   TEXT        NOT NULL DEFAULT 'UNKNOWN',
    fuel_pct                REAL,
    latitude                REAL,
    longitude               REAL,
    speed_mph               REAL,
    heading                 REAL,
    next_poll               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parked_since            TIMESTAMPTZ,
    alert_sent              BOOLEAN     NOT NULL DEFAULT FALSE,
    overnight_alert_sent    BOOLEAN     NOT NULL DEFAULT FALSE,
    open_alert_id           INTEGER,
    assigned_stop_id        INTEGER,
    assigned_stop_name      TEXT,
    assigned_stop_lat       REAL,
    assigned_stop_lng       REAL,
    assignment_time         TIMESTAMPTZ,
    in_yard                 BOOLEAN     NOT NULL DEFAULT FALSE,
    yard_name               TEXT,
    sleeping                BOOLEAN     NOT NULL DEFAULT FALSE,
    fuel_when_parked        REAL,
    ca_reminder_sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    prev_truck_group        TEXT,
    prev_truck_msg_id       BIGINT,
    prev_dispatcher_msg_id  BIGINT,
    prev_ca_truck_msg_id       BIGINT,
    prev_ca_dispatcher_msg_id  BIGINT,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_alerts: history of every alert sent
CREATE TABLE IF NOT EXISTS fuel_alerts (
    id                  SERIAL PRIMARY KEY,
    vehicle_id          TEXT    NOT NULL,
    vehicle_name        TEXT,
    fuel_pct            REAL    NOT NULL,
    latitude            REAL    NOT NULL,
    longitude           REAL    NOT NULL,
    heading             REAL,
    speed_mph           REAL,
    best_stop_id        INTEGER,
    best_stop_name      TEXT,
    best_stop_price     REAL,
    alt_stop_id         INTEGER,
    alt_stop_name       TEXT,
    alt_stop_price      REAL,
    best_stop_state     TEXT,
    gallons_purchased   REAL,
    savings_usd         REAL,
    alert_type          TEXT    NOT NULL DEFAULT 'low_fuel',
    status              TEXT    NOT NULL DEFAULT 'open',
    alerted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fuel_alerts_vehicle ON fuel_alerts (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fuel_alerts_status  ON fuel_alerts (status);

-- ca_reminders: prevent duplicate CA border reminders
CREATE TABLE IF NOT EXISTS ca_reminders (
    id          SERIAL PRIMARY KEY,
    vehicle_id  TEXT        NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS truck_efficiency (
    vehicle_id      TEXT PRIMARY KEY,
    vehicle_name    TEXT,
    mpg             FLOAT DEFAULT 6.5,
    idle_hours_30d  FLOAT DEFAULT 0,
    idle_pct_30d    FLOAT DEFAULT 0,
    fuel_used_30d   FLOAT DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS truck_routes (
    truck_number    TEXT PRIMARY KEY,
    group_chat_id   TEXT,
    trip_num        TEXT,
    ref_number      TEXT,
    route_json      TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- trip_state: persists all trip planning data across bot restarts
-- One row per active truck — updated every time state changes
CREATE TABLE IF NOT EXISTS trip_state (
    vehicle_name            TEXT PRIMARY KEY,
    trip_num                TEXT,
    trip_status             TEXT,
    briefing_sent_trip      TEXT,       -- trip_id of last briefing sent
    all_planned_stops       TEXT,       -- JSON array of planned stops
    planned_stop_index      INTEGER DEFAULT 0,
    assigned_stop_name      TEXT,
    assigned_stop_lat       REAL,
    assigned_stop_lng       REAL,
    assigned_stop_card_price REAL,
    assigned_stop_net_price  REAL,
    missed_stop_name        TEXT,
    missed_stop_card_price  REAL,
    completed_waypoints     TEXT,       -- JSON array of completed stop IDs
    border_warned           TEXT,       -- JSON dict of {state: True} already warned
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);
-- Add border_warned column if it doesn't exist (migration)
DO $$ BEGIN
    ALTER TABLE trip_state ADD COLUMN IF NOT EXISTS border_warned TEXT DEFAULT '{}';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- driver_flags: wrong stop / missed stop / low-stop state flags
CREATE TABLE IF NOT EXISTS driver_flags (
    id               SERIAL PRIMARY KEY,
    vehicle_name     TEXT NOT NULL,
    flag_type        TEXT NOT NULL,
    details          TEXT,
    recommended_stop TEXT,
    actual_stop      TEXT,
    fuel_pct         REAL,
    state            TEXT,
    card_price       REAL,
    savings_lost     REAL,
    flagged_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_flags_truck ON driver_flags (vehicle_name);
CREATE INDEX IF NOT EXISTS idx_flags_time  ON driver_flags (flagged_at);

CREATE TABLE IF NOT EXISTS stop_visits (
    id              SERIAL PRIMARY KEY,
    vehicle_name    TEXT NOT NULL,
    alert_id        INTEGER,
    recommended_stop_name  TEXT,
    recommended_stop_lat   FLOAT,
    recommended_stop_lng   FLOAT,
    actual_stop_name       TEXT,
    actual_stop_lat        FLOAT,
    actual_stop_lng        FLOAT,
    actual_stop_state      TEXT,
    visited         BOOLEAN,   -- TRUE=went to recommended, FALSE=went elsewhere, NULL=unknown
    fuel_before     FLOAT,
    fuel_after      FLOAT,
    gallons_purchased REAL,
    savings_usd     REAL,
    visited_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_db():
    """Create all tables if they don't exist. Runs migrations for existing DBs."""
    log.info("Initializing PostgreSQL schema...")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    # Migrations for existing DBs
    cur.execute("ALTER TABLE trucks ALTER COLUMN telegram_group_id DROP NOT NULL")
    # Add missing fuel_alerts columns if not exists
    for col_def in [
        "alt_stop_id INTEGER",
        "alt_stop_name TEXT",
        "alt_stop_price REAL",
        "best_stop_state TEXT",
        "gallons_purchased REAL",
    ]:
        col_name = col_def.split()[0]
        try:
            cur.execute(f"ALTER TABLE fuel_alerts ADD COLUMN IF NOT EXISTS {col_def}")
        except Exception:
            pass
    for col, coltype in [
        ("prev_truck_group",       "TEXT"),
        ("prev_truck_msg_id",      "BIGINT"),
        ("prev_dispatcher_msg_id", "BIGINT"),
        ("prev_ca_truck_msg_id",      "BIGINT"),
        ("prev_ca_dispatcher_msg_id", "BIGINT"),
    ]:
        cur.execute(f"ALTER TABLE truck_states ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("assigned_stop_card_price", "REAL"),
        ("assigned_stop_net_price",  "REAL"),
        ("missed_stop_name",         "TEXT"),
        ("missed_stop_card_price",   "REAL"),
        ("completed_waypoints",      "TEXT"),
        ("border_warned",            "TEXT"),
    ]:
        cur.execute(f"ALTER TABLE trip_state ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("card_price",   "REAL"),
        ("savings_lost", "REAL"),
    ]:
        cur.execute(f"ALTER TABLE driver_flags ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("actual_stop_state",  "TEXT"),
        ("gallons_purchased",  "REAL"),
        ("savings_usd",        "REAL"),
    ]:
        cur.execute(f"ALTER TABLE stop_visits ADD COLUMN IF NOT EXISTS {col} {coltype}")
    conn.commit()
    conn.close()
    log.info("✅ Database schema ready.")


# -- Config -------------------------------------------------------------------

def set_config_value(key: str, value: str):
    sql = """
        INSERT INTO bot_config (key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, (key, value))


def get_config_value(key: str) -> str | None:
    with db_cursor() as cur:
        cur.execute("SELECT value FROM bot_config WHERE key=%s", (key,))
        row = cur.fetchone()
        return row["value"] if row else None


# -- Helpers ------------------------------------------------------------------

def _row(r):
    return dict(r) if r else None

def _rows(rs):
    return [dict(r) for r in rs]

def _dt(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


# -- trucks -------------------------------------------------------------------

def get_truck_group(vehicle_name: str) -> str | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT telegram_group_id FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return row["telegram_group_id"] if row else None


def get_truck_config(vehicle_name: str) -> dict | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        return _row(cur.fetchone())


def get_all_registered_trucks() -> list:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM trucks WHERE is_active = TRUE ORDER BY vehicle_name")
        return _rows(cur.fetchall())


def auto_register_truck(vehicle_id: str, vehicle_name: str) -> bool:
    """Auto-register a truck seen from Samsara if not already in DB. Returns True if newly registered."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM trucks WHERE vehicle_name = %s", (vehicle_name,))
        if cur.fetchone():
            return False
        cur.execute(
            "INSERT INTO trucks (vehicle_name, is_active) VALUES (%s, TRUE)",
            (vehicle_name,)
        )
        log.info(f"Auto-registered new truck: {vehicle_name}")
        return True


def get_truck_by_group(group_id: str) -> dict | None:
    """Get truck record by its Telegram group ID."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trucks WHERE telegram_group_id = %s AND is_active = TRUE",
            (str(group_id),)
        )
        return cur.fetchone()


def upsert_truck_group(vehicle_name: str, group_id: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "UPDATE trucks SET telegram_group_id = %s WHERE vehicle_name = %s",
            (group_id, vehicle_name)
        )
        return cur.rowcount > 0


def deactivate_truck(vehicle_name: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "UPDATE trucks SET is_active = FALSE WHERE vehicle_name = %s",
            (vehicle_name,)
        )
        return cur.rowcount > 0


# -- fuel_stops ---------------------------------------------------------------

def upsert_fuel_stop(row: dict):
    bulk_upsert_fuel_stops([row])


def bulk_upsert_fuel_stops(records: list[dict]) -> int:
    """Bulk insert/update fuel stops. Returns count inserted/updated."""
    if not records:
        return 0
    # Legacy function — use import_efs_csv instead
    return 0


def import_efs_csv(file_bytes: bytes) -> tuple[int, str]:
    """
    Import daily EFS price CSV into fuel_stops.
    Expected columns: Station, Address, City, State, longitude, latitude,
                      Retail price, Discounted price
    Clears existing data and reloads fresh every time (daily upload).
    """
    import csv, io, re

    def _norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())

    def _pick(row: dict, aliases: tuple[str, ...], default: str = "") -> str:
        for alias in aliases:
            actual = header_map.get(_norm(alias))
            if actual is not None:
                return str(row.get(actual, default) or "").strip()
        return default

    def _to_float(value: str):
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace("$", "").replace(",", "")
        return float(text)
    try:
        text   = file_bytes.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return 0, "CSV file is missing a header row."
        rows   = list(reader)
    except Exception as e:
        return 0, f"❌ Could not parse file: {e}"

    header_map = {_norm(name): name for name in (reader.fieldnames or [])}
    required_groups = {
        "station": ("Station", "station_name", "store_name", "name"),
        "city": ("City", "city"),
        "state": ("State", "state"),
        "latitude": ("latitude", "lat"),
        "longitude": ("longitude", "lng", "lon"),
        "discounted_price": (
            "Discounted price", "discounted_price", "discount_price",
            "card price", "card_price", "diesel_price",
        ),
    }
    missing = [
        group for group, aliases in required_groups.items()
        if not any(_norm(alias) in header_map for alias in aliases)
    ]
    if missing:
        return 0, "CSV missing required columns: " + ", ".join(missing)

    records = []
    skipped = 0
    for r in rows:
        try:
            lat = _to_float(_pick(r, required_groups["latitude"]))
            lng = _to_float(_pick(r, required_groups["longitude"]))
            name = _pick(r, required_groups["station"])
            city = _pick(r, required_groups["city"])
            state = _pick(r, required_groups["state"]).upper()
            retail = _to_float(_pick(r, ("Retail price", "retail_price", "retail", "pump_price")))
            discount = _to_float(_pick(r, required_groups["discounted_price"]))
            if not name or not state or lat is None or lng is None or discount is None:
                skipped += 1
                continue
            records.append({
                "station_name":     name,
                "address":          _pick(r, ("Address", "address", "street_address")),
                "city":             city,
                "state":            state,
                "longitude":        lng,
                "latitude":         lat,
                "retail_price":     retail,
                "discounted_price": discount,
            })
        except Exception:
            skipped += 1

    if not records:
        return 0, "❌ No valid records found in file."

    with db_cursor() as cur:
        # Upsert — update existing prices, insert new ones
        # NEVER delete — price history stays forever
        cur.execute("TRUNCATE TABLE fuel_stops RESTART IDENTITY")
        cur.executemany("""
            INSERT INTO fuel_stops
                (station_name, address, city, state, longitude, latitude,
                 retail_price, discounted_price, price_updated)
            VALUES
                (%(station_name)s, %(address)s, %(city)s, %(state)s,
                 %(longitude)s, %(latitude)s,
                 %(retail_price)s, %(discounted_price)s, NOW())
        """, records)

    msg = (
        f"✅ *Fuel prices updated*\n"
        f"⛽ {len(records)} stations loaded\n"
        f"⏭ {skipped} skipped (missing data)\n"
        f"🔄 Using discounted (card) price for routing"
    )
    return len(records), msg


def get_all_diesel_stops() -> list:
    """Return all stops that have a discounted price (card price)."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                id,
                station_name  AS store_name,
                address,
                city,
                state,
                longitude,
                latitude,
                retail_price,
                discounted_price AS diesel_price,
                price_updated
            FROM fuel_stops
            WHERE discounted_price IS NOT NULL
            ORDER BY state, city
        """)
        return _rows(cur.fetchall())


def get_stops_count() -> int:
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM fuel_stops WHERE discounted_price IS NOT NULL")
        return cur.fetchone()["cnt"]


def get_price_last_updated():
    with db_cursor() as cur:
        cur.execute("SELECT MAX(price_updated) as latest FROM fuel_stops")
        row = cur.fetchone()
        return row["latest"] if row else None


# -- truck_states -------------------------------------------------------------

def load_all_truck_states() -> dict:
    """Load all truck states from DB. Returns {vehicle_id: state_dict}."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM truck_states")
        rows = cur.fetchall()

    states = {}
    for row in rows:
        r = dict(row)
        vid = r["vehicle_id"]
        states[vid] = {
            "vehicle_id":             vid,
            "vehicle_name":           r["vehicle_name"],
            "state":                  r["state"],
            "fuel_pct":               r["fuel_pct"],
            "lat":                    r["latitude"],
            "lng":                    r["longitude"],
            "speed_mph":              r["speed_mph"],
            "heading":                r["heading"],
            "next_poll":              _dt(r["next_poll"]),
            "parked_since":           _dt(r["parked_since"]),
            "alert_sent":             bool(r["alert_sent"]),
            "overnight_alert_sent":   bool(r["overnight_alert_sent"]),
            "open_alert_id":          r["open_alert_id"],
            "assigned_stop_id":       r["assigned_stop_id"],
            "assigned_stop_name":     r["assigned_stop_name"],
            "assigned_stop_lat":      r["assigned_stop_lat"],
            "assigned_stop_lng":      r["assigned_stop_lng"],
            "assignment_time":        _dt(r["assignment_time"]),
            "in_yard":                bool(r["in_yard"]),
            "yard_name":              r["yard_name"],
            "sleeping":               bool(r["sleeping"]),
            "fuel_when_parked":       r["fuel_when_parked"],
            "ca_reminder_sent":       bool(r["ca_reminder_sent"]),
            "prev_truck_group":       r.get("prev_truck_group"),
            "prev_truck_msg_id":      r.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": r.get("prev_dispatcher_msg_id"),
            "prev_ca_truck_msg_id":      r.get("prev_ca_truck_msg_id"),
            "prev_ca_dispatcher_msg_id": r.get("prev_ca_dispatcher_msg_id"),
        }
    return states


def save_truck_state(state: dict):
    """Upsert a single truck state to DB."""
    sql = """
        INSERT INTO truck_states (
            vehicle_id, vehicle_name, state, fuel_pct,
            latitude, longitude, speed_mph, heading,
            next_poll, parked_since, alert_sent, overnight_alert_sent,
            open_alert_id, assigned_stop_id, assigned_stop_name,
            assigned_stop_lat, assigned_stop_lng, assignment_time,
            in_yard, yard_name, sleeping, fuel_when_parked,
            ca_reminder_sent, prev_truck_group, prev_truck_msg_id,
            prev_dispatcher_msg_id, prev_ca_truck_msg_id, prev_ca_dispatcher_msg_id, last_updated
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(state)s, %(fuel_pct)s,
            %(lat)s, %(lng)s, %(speed_mph)s, %(heading)s,
            %(next_poll)s, %(parked_since)s, %(alert_sent)s, %(overnight_alert_sent)s,
            %(open_alert_id)s, %(assigned_stop_id)s, %(assigned_stop_name)s,
            %(assigned_stop_lat)s, %(assigned_stop_lng)s, %(assignment_time)s,
            %(in_yard)s, %(yard_name)s, %(sleeping)s, %(fuel_when_parked)s,
            %(ca_reminder_sent)s, %(prev_truck_group)s, %(prev_truck_msg_id)s,
            %(prev_dispatcher_msg_id)s, %(prev_ca_truck_msg_id)s, %(prev_ca_dispatcher_msg_id)s, NOW()
        )
        ON CONFLICT (vehicle_id) DO UPDATE SET
            vehicle_name           = EXCLUDED.vehicle_name,
            state                  = EXCLUDED.state,
            fuel_pct               = EXCLUDED.fuel_pct,
            latitude               = EXCLUDED.latitude,
            longitude              = EXCLUDED.longitude,
            speed_mph              = EXCLUDED.speed_mph,
            heading                = EXCLUDED.heading,
            next_poll              = EXCLUDED.next_poll,
            parked_since           = EXCLUDED.parked_since,
            alert_sent             = EXCLUDED.alert_sent,
            overnight_alert_sent   = EXCLUDED.overnight_alert_sent,
            open_alert_id          = EXCLUDED.open_alert_id,
            assigned_stop_id       = EXCLUDED.assigned_stop_id,
            assigned_stop_name     = EXCLUDED.assigned_stop_name,
            assigned_stop_lat      = EXCLUDED.assigned_stop_lat,
            assigned_stop_lng      = EXCLUDED.assigned_stop_lng,
            assignment_time        = EXCLUDED.assignment_time,
            in_yard                = EXCLUDED.in_yard,
            yard_name              = EXCLUDED.yard_name,
            sleeping               = EXCLUDED.sleeping,
            fuel_when_parked       = EXCLUDED.fuel_when_parked,
            ca_reminder_sent       = EXCLUDED.ca_reminder_sent,
            prev_truck_group       = EXCLUDED.prev_truck_group,
            prev_truck_msg_id      = EXCLUDED.prev_truck_msg_id,
            prev_dispatcher_msg_id = EXCLUDED.prev_dispatcher_msg_id,
            prev_ca_truck_msg_id      = EXCLUDED.prev_ca_truck_msg_id,
            prev_ca_dispatcher_msg_id = EXCLUDED.prev_ca_dispatcher_msg_id,
            last_updated           = NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":             state["vehicle_id"],
            "vehicle_name":           state.get("vehicle_name"),
            "state":                  state.get("state", "UNKNOWN"),
            "fuel_pct":               state.get("fuel_pct"),
            "lat":                    state.get("lat"),
            "lng":                    state.get("lng"),
            "speed_mph":              state.get("speed_mph"),
            "heading":                state.get("heading"),
            "next_poll":              state.get("next_poll"),
            "parked_since":           state.get("parked_since"),
            "alert_sent":             bool(state.get("alert_sent", False)),
            "overnight_alert_sent":   bool(state.get("overnight_alert_sent", False)),
            "open_alert_id":          state.get("open_alert_id"),
            "assigned_stop_id":       state.get("assigned_stop_id"),
            "assigned_stop_name":     state.get("assigned_stop_name"),
            "assigned_stop_lat":      state.get("assigned_stop_lat"),
            "assigned_stop_lng":      state.get("assigned_stop_lng"),
            "assignment_time":        state.get("assignment_time"),
            "in_yard":                bool(state.get("in_yard", False)),
            "yard_name":              state.get("yard_name"),
            "sleeping":               bool(state.get("sleeping", False)),
            "fuel_when_parked":       state.get("fuel_when_parked"),
            "ca_reminder_sent":       bool(state.get("ca_reminder_sent", False)),
            "prev_truck_group":       state.get("prev_truck_group"),
            "prev_truck_msg_id":      state.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": state.get("prev_dispatcher_msg_id"),
            "prev_ca_truck_msg_id":      state.get("prev_ca_truck_msg_id"),
            "prev_ca_dispatcher_msg_id": state.get("prev_ca_dispatcher_msg_id"),
        })


def save_all_truck_states(states: dict):
    for state in states.values():
        save_truck_state(state)


def reset_truck_states():
    with db_cursor() as cur:
        cur.execute("DELETE FROM truck_states")
    log.info("✅ Truck states reset.")


# -- fuel_alerts --------------------------------------------------------------

def create_fuel_alert(vehicle_id, vehicle_name, fuel_pct, lat, lng,
                      heading, speed_mph, alert_type="low_fuel",
                      best_stop=None, alt_stop=None, savings_usd=None) -> int:
    sql = """
        INSERT INTO fuel_alerts (
            vehicle_id, vehicle_name, fuel_pct, latitude, longitude,
            heading, speed_mph, alert_type,
            best_stop_id, best_stop_name, best_stop_price, best_stop_state,
            alt_stop_id,  alt_stop_name,  alt_stop_price, savings_usd
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(fuel_pct)s, %(lat)s, %(lng)s,
            %(heading)s, %(speed_mph)s, %(alert_type)s,
            %(best_stop_id)s, %(best_stop_name)s, %(best_stop_price)s, %(best_stop_state)s,
            %(alt_stop_id)s,  %(alt_stop_name)s,  %(alt_stop_price)s, %(savings_usd)s
        ) RETURNING id
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":      vehicle_id,
            "vehicle_name":    vehicle_name,
            "fuel_pct":        fuel_pct,
            "lat":             lat,
            "lng":             lng,
            "heading":         heading,
            "speed_mph":       speed_mph,
            "alert_type":      alert_type,
            "best_stop_id":    best_stop["id"]          if best_stop else None,
            "best_stop_name":  best_stop["store_name"]  if best_stop else None,
            "best_stop_price": best_stop["diesel_price"] if best_stop else None,
            "best_stop_state": best_stop["state"]       if best_stop else None,
            "alt_stop_id":     alt_stop["id"]           if alt_stop else None,
            "alt_stop_name":   alt_stop["store_name"]   if alt_stop else None,
            "alt_stop_price":  alt_stop["diesel_price"]  if alt_stop else None,
            "savings_usd":     savings_usd,
        })
        return cur.fetchone()["id"]


def resolve_alert(alert_id: int):
    with db_cursor() as cur:
        cur.execute(
            "UPDATE fuel_alerts SET status='resolved', resolved_at=NOW() WHERE id=%s",
            (alert_id,)
        )


# -- Aliases ------------------------------------------------------------------

def get_bot_config(key: str) -> str | None:
    return get_config_value(key)

def set_bot_config(key: str, value: str):
    set_config_value(key, value)

def save_truck_route(truck_number: str, group_chat_id: str, route: dict) -> None:
    """Save parsed QM Notifier route for a truck."""
    import json
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO truck_routes (truck_number, group_chat_id, trip_num, ref_number, route_json, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (truck_number) DO UPDATE SET
                group_chat_id = EXCLUDED.group_chat_id,
                trip_num      = EXCLUDED.trip_num,
                ref_number    = EXCLUDED.ref_number,
                route_json    = EXCLUDED.route_json,
                updated_at    = NOW()
        """, (
            truck_number,
            group_chat_id,
            str(route.get("trip_num", "")),
            str(route.get("ref_number", "")),
            json.dumps(route),
        ))
    log.info(f"Route saved for truck {truck_number}: trip {route.get('trip_num')}")


def get_truck_route(truck_number: str) -> dict | None:
    """Get the last saved route for a truck."""
    import json
    with db_cursor() as cur:
        cur.execute(
            "SELECT route_json FROM truck_routes WHERE truck_number = %s",
            (truck_number,)
        )
        row = cur.fetchone()
        if row and row["route_json"]:
            return json.loads(row["route_json"])
    return None


def get_all_truck_routes_from_db() -> dict[str, dict]:
    """Get all saved routes keyed by truck_number."""
    import json
    routes = {}
    with db_cursor() as cur:
        cur.execute("SELECT truck_number, route_json FROM truck_routes")
        for row in cur.fetchall():
            if row["route_json"]:
                try:
                    routes[row["truck_number"]] = json.loads(row["route_json"])
                except Exception:
                    pass
    return routes


def get_last_qm_message(chat_id: str) -> dict | None:
    """Stub — routes stored via save_truck_route."""
    return None

def log_stop_visit(vehicle_name: str, alert_id: int,
                   recommended_stop_name: str,
                   recommended_lat: float, recommended_lng: float,
                   actual_stop_name: str,
                   actual_lat: float, actual_lng: float,
                   visited: bool,
                   fuel_before: float, fuel_after: float,
                   actual_stop_state: str = None,
                   gallons_purchased: float = None,
                   savings_usd: float = None) -> None:
    """Log whether truck visited the recommended stop or went elsewhere."""
    from datetime import datetime, timezone
    if gallons_purchased is None and fuel_before is not None and fuel_after is not None:
        fuel_delta = max(float(fuel_after) - float(fuel_before), 0.0)
        gallons_purchased = round(150.0 * fuel_delta / 100.0, 1) if fuel_delta > 0 else 0.0
    if savings_usd is None and visited and alert_id:
        try:
            with db_cursor() as cur:
                cur.execute("SELECT savings_usd FROM fuel_alerts WHERE id = %s", (alert_id,))
                row = cur.fetchone()
                savings_usd = float(row["savings_usd"]) if row and row.get("savings_usd") is not None else None
        except Exception:
            savings_usd = None
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO stop_visits (
                vehicle_name, alert_id,
                recommended_stop_name, recommended_stop_lat, recommended_stop_lng,
                actual_stop_name, actual_stop_lat, actual_stop_lng, actual_stop_state,
                visited, fuel_before, fuel_after, gallons_purchased, savings_usd, visited_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            vehicle_name, alert_id,
            recommended_stop_name, recommended_lat, recommended_lng,
            actual_stop_name, actual_lat, actual_lng, actual_stop_state,
            visited, fuel_before, fuel_after, gallons_purchased, savings_usd,
            datetime.now(timezone.utc)
        ))


def get_stop_compliance(vehicle_name: str = None, days: int = 7) -> list:
    """Get stop visit compliance stats."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        if vehicle_name:
            cur.execute("""
                SELECT vehicle_name, recommended_stop_name, actual_stop_name,
                       visited, fuel_before, fuel_after, visited_at
                FROM stop_visits
                WHERE vehicle_name = %s AND created_at >= %s
                ORDER BY visited_at DESC LIMIT 20
            """, (vehicle_name, since))
        else:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE)  AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped,
                    COUNT(*) FILTER (WHERE visited IS NULL) AS unknown
                FROM stop_visits WHERE created_at >= %s
            """, (since,))
        return cur.fetchall()

def save_truck_efficiency(vehicle_id: str, vehicle_name: str, 
                           mpg: float, idle_hours: float, 
                           idle_pct: float, fuel_gal: float) -> None:
    """Save real MPG and idle data from Samsara."""
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO truck_efficiency 
                (vehicle_id, vehicle_name, mpg, idle_hours_30d, idle_pct_30d, fuel_used_30d, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                vehicle_name   = EXCLUDED.vehicle_name,
                mpg            = EXCLUDED.mpg,
                idle_hours_30d = EXCLUDED.idle_hours_30d,
                idle_pct_30d   = EXCLUDED.idle_pct_30d,
                fuel_used_30d  = EXCLUDED.fuel_used_30d,
                updated_at     = NOW()
        """, (vehicle_id, vehicle_name, mpg, idle_hours, idle_pct, fuel_gal))


def get_truck_mpg(vehicle_id: str) -> float:
    """Get real MPG for a truck. Returns 6.5 default if not available."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT mpg FROM truck_efficiency WHERE vehicle_id = %s",
            (vehicle_id,)
        )
        row = cur.fetchone()
        if row and row["mpg"] and row["mpg"] > 0:
            return float(row["mpg"])
    return 6.5  # default


def get_all_truck_efficiency() -> list:
    """Get all truck efficiency stats."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT vehicle_name, mpg, idle_hours_30d, idle_pct_30d, fuel_used_30d, updated_at
            FROM truck_efficiency
            ORDER BY mpg ASC
        """)
        return cur.fetchall()

def get_truck_params(vehicle_name: str) -> dict | None:
    """Get tank size and MPG for a truck. Returns None if not found."""
    with db_cursor() as cur:
        cur.execute(
            """SELECT tank_capacity_gal AS tank_gal, avg_mpg AS mpg
               FROM trucks WHERE vehicle_name = %s AND is_active = TRUE""",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_truck_mpg(vehicle_id: str) -> float:
    """Get real MPG for a truck from Samsara efficiency data. Returns 6.5 default."""
    with db_cursor() as cur:
        try:
            cur.execute(
                "SELECT mpg FROM truck_efficiency WHERE vehicle_id = %s AND mpg > 3",
                (vehicle_id,)
            )
            row = cur.fetchone()
            if row and row["mpg"]:
                return float(row["mpg"])
        except Exception:
            pass
    return 6.5


# ═══════════════════════════════════════════════════════════════════
# DATA RETENTION POLICY — NOTHING IS EVER DELETED
# fuel_alerts, stop_visits, driver_flags, trip_state all grow forever
# Weekly report queries by date range — data stays in DB permanently
# ═══════════════════════════════════════════════════════════════════

def get_flags_for_report(days: int = 7) -> list:
    """Get all flags for weekly Excel report."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        try:
            cur.execute("""
                SELECT vehicle_name, flag_type, details,
                       recommended_stop, actual_stop,
                       fuel_pct, state, card_price, savings_lost,
                       flagged_at
                FROM driver_flags
                WHERE flagged_at >= %s
                ORDER BY flagged_at DESC
            """, (since,))
            return _rows(cur.fetchall())
        except Exception:
            return []


def get_compliance_for_report(days: int = 7) -> list:
    """Get per-truck compliance stats for weekly Excel report."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        try:
            cur.execute("""
                SELECT
                    vehicle_name,
                    COUNT(*) FILTER (WHERE visited = TRUE)  AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped,
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN visited THEN savings_usd ELSE 0 END), 0) AS savings,
                    COALESCE(SUM(CASE WHEN NOT visited THEN ABS(savings_usd) ELSE 0 END), 0) AS losses
                FROM stop_visits
                WHERE visited_at >= %s
                GROUP BY vehicle_name
                ORDER BY vehicle_name
            """, (since,))
            return _rows(cur.fetchall())
        except Exception:
            return []


def save_trip_state(vehicle_name: str, state: dict) -> None:
    """Save trip planning state to DB — survives bot restarts."""
    import json

    # Collect all border_warned_{STATE} keys into a single dict
    border_warned = {
        k.replace("border_warned_", ""): v
        for k, v in state.items()
        if k.startswith("border_warned_") and v
    }

    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO trip_state (
                vehicle_name, trip_num, trip_status,
                briefing_sent_trip,
                all_planned_stops, planned_stop_index,
                assigned_stop_name, assigned_stop_lat, assigned_stop_lng,
                assigned_stop_card_price, assigned_stop_net_price,
                missed_stop_name, missed_stop_card_price,
                completed_waypoints, border_warned, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (vehicle_name) DO UPDATE SET
                trip_num                 = EXCLUDED.trip_num,
                trip_status              = EXCLUDED.trip_status,
                briefing_sent_trip       = EXCLUDED.briefing_sent_trip,
                all_planned_stops        = EXCLUDED.all_planned_stops,
                planned_stop_index       = EXCLUDED.planned_stop_index,
                assigned_stop_name       = EXCLUDED.assigned_stop_name,
                assigned_stop_lat        = EXCLUDED.assigned_stop_lat,
                assigned_stop_lng        = EXCLUDED.assigned_stop_lng,
                assigned_stop_card_price = EXCLUDED.assigned_stop_card_price,
                assigned_stop_net_price  = EXCLUDED.assigned_stop_net_price,
                missed_stop_name         = EXCLUDED.missed_stop_name,
                missed_stop_card_price   = EXCLUDED.missed_stop_card_price,
                completed_waypoints      = EXCLUDED.completed_waypoints,
                border_warned            = EXCLUDED.border_warned,
                updated_at               = NOW()
        """, (
            vehicle_name,
            state.get("qm_route", {}).get("trip_num") if state.get("qm_route") else None,
            state.get("last_trip_status"),
            state.get("briefing_sent_trip"),
            json.dumps(state.get("all_planned_stops") or []),
            state.get("planned_stop_index", 0),
            state.get("assigned_stop_name"),
            state.get("assigned_stop_lat"),
            state.get("assigned_stop_lng"),
            state.get("assigned_stop_card_price"),
            state.get("assigned_stop_net_price"),
            state.get("missed_stop_name"),
            state.get("missed_stop_card_price"),
            json.dumps(list(state.get("completed_waypoints") or [])),
            json.dumps(border_warned),
        ))


def load_trip_state(vehicle_name: str) -> dict:
    """Load persisted trip state for a truck. Returns {} if not found."""
    import json
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trip_state WHERE vehicle_name = %s",
            (vehicle_name,)
        )
        row = cur.fetchone()
    if not row:
        return {}
    result = {}
    try: result["briefing_sent_trip"]       = row["briefing_sent_trip"]
    except: pass
    try: result["all_planned_stops"]        = json.loads(row["all_planned_stops"] or "[]")
    except: result["all_planned_stops"]     = []
    try: result["planned_stop_index"]       = row["planned_stop_index"] or 0
    except: result["planned_stop_index"]    = 0
    try: result["assigned_stop_name"]       = row["assigned_stop_name"]
    except: pass
    try: result["assigned_stop_lat"]        = row["assigned_stop_lat"]
    except: pass
    try: result["assigned_stop_lng"]        = row["assigned_stop_lng"]
    except: pass
    try: result["assigned_stop_card_price"] = row["assigned_stop_card_price"]
    except: pass
    try: result["assigned_stop_net_price"]  = row["assigned_stop_net_price"]
    except: pass
    try: result["missed_stop_name"]         = row["missed_stop_name"]
    except: pass
    try: result["missed_stop_card_price"]   = row["missed_stop_card_price"]
    except: pass
    try:
        wps = json.loads(row["completed_waypoints"] or "[]")
        result["completed_waypoints"] = set(wps)
    except:
        result["completed_waypoints"] = set()
    # Restore border_warned_{STATE} keys into state
    try:
        bw = json.loads(row.get("border_warned") or "{}")
        for state_code, warned in bw.items():
            if warned:
                result[f"border_warned_{state_code}"] = True
    except:
        pass
    return result


def load_all_trip_states() -> dict:
    """Load all persisted trip states on bot startup. Returns {vehicle_name: state_dict}."""
    with db_cursor() as cur:
        cur.execute("SELECT vehicle_name FROM trip_state")
        rows = cur.fetchall()
    result = {}
    for row in rows:
        vname = row["vehicle_name"]
        result[vname] = load_trip_state(vname)
    return result
