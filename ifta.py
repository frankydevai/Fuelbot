"""
ifta.py — IFTA diesel tax rates and net price calculation

Rates are sourced from official IFTA-105 Q1 2026 form.
Auto-updated quarterly via scrape_and_update_ifta_rates().

Surcharge states (KY, VA, NY, NM, IN):
  These states have an ADDITIONAL surcharge on top of base rate.
  Surcharges are ALWAYS owed — never generate credits.
  Total effective rate = base + surcharge.
"""

import logging
import re
import requests
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Official Q1 2026 IFTA diesel rates per gallon
# Source: IFTA-105 (3/26) — https://www.tax.ny.gov/pdf/current_forms/motor/ifta105.pdf
IFTA_RATES = {
    "AL": 0.3100,
    "AZ": 0.2600,
    "AR": 0.2850,
    "CA": 0.9710,  # highest in US
    "CO": 0.3250,
    "CT": 0.4890,
    "DE": 0.2200,
    "FL": 0.4097,
    "GA": 0.3710,
    "ID": 0.3200,
    "IL": 0.7380,
    "IN": 0.6100,  # base 0.61 + surcharge 0.61 = effective 1.22 (highest overall)
    "IA": 0.3250,
    "KS": 0.2600,
    "KY": 0.2200,  # base rate (surcharge 0.105 added separately)
    "LA": 0.2000,
    "ME": 0.3120,
    "MD": 0.4675,
    "MA": 0.2400,
    "MI": 0.5240,
    "MN": 0.3260,
    "MS": 0.2100,
    "MO": 0.2950,
    "MT": 0.2975,
    "NE": 0.3180,
    "NV": 0.2700,
    "NH": 0.2220,
    "NJ": 0.5610,
    "NM": 0.2100,  # base rate (surcharge 0.01 added separately)
    "NY": 0.3805, # base rate (surcharge 0.0095 added separately)
    "NC": 0.4100,
    "ND": 0.2300,
    "OH": 0.4700,
    "OK": 0.1900,
    "OR": 0.0000,  # Oregon uses weight-mile tax instead
    "PA": 0.7410,  # second highest in US
    "RI": 0.4000,
    "SC": 0.2800,
    "SD": 0.2800,
    "TN": 0.2700,
    "TX": 0.2000,
    "UT": 0.3790,
    "VT": 0.3100,
    "VA": 0.3270,  # base rate (surcharge 0.143 added separately)
    "WA": 0.5840,
    "WV": 0.3570,
    "WI": 0.3290,
    "WY": 0.2400,
}

# Surcharge states — additional per-gallon charge ALWAYS owed
IFTA_SURCHARGES = {
    "KY": 0.1050,
    "VA": 0.1430,
    "NY": 0.0095,
    "NM": 0.0100,
    "IN": 0.6100,  # Indiana surcharge = same as base rate! Effective = $1.22/gal
}

# States with very few truck stops — need to fuel up BEFORE entering
LOW_STOP_STATES = {
    "MD": {"name": "Maryland",      "warn_miles": 150, "reason": "very few truck stops"},
    "NJ": {"name": "New Jersey",    "warn_miles": 100, "reason": "limited truck stops"},
    "PA": {"name": "Pennsylvania",  "warn_miles": 120, "reason": "limited stops on some corridors"},
    "WI": {"name": "Wisconsin",     "warn_miles": 150, "reason": "sparse truck stops"},
    "MT": {"name": "Montana",       "warn_miles": 200, "reason": "very sparse — long stretches"},
    "UT": {"name": "Utah",          "warn_miles": 150, "reason": "limited stops outside I-15"},
    "WY": {"name": "Wyoming",       "warn_miles": 200, "reason": "very sparse — long stretches"},
    "ID": {"name": "Idaho",         "warn_miles": 150, "reason": "limited stops"},
}

LOW_STOP_MIN_FUEL = {
    "MD": 50, "NJ": 50, "PA": 45, "WI": 50,
    "MT": 65, "UT": 55, "WY": 65, "ID": 55,
}

# Track when rates were last updated
_rates_updated: str = "2026-Q1"


# Fleet home state — set via IFTA_HOME_STATE env var (e.g. "FL", "TX", "OH")
# Defaults to None if not set — in that case net_price_after_ifta returns pump price unchanged
import os as _os
HOME_STATE = _os.environ.get("IFTA_HOME_STATE", "").upper().strip() or None
HOME_STATE_RATE = round(
    IFTA_RATES.get(HOME_STATE, 0.0) + IFTA_SURCHARGES.get(HOME_STATE, 0.0), 4
) if HOME_STATE else 0.0


def get_ifta_rate(state: str) -> float:
    """Get total IFTA diesel rate for a state (base + surcharge)."""
    state = state.upper().strip()
    base      = IFTA_RATES.get(state, 0.0)
    surcharge = IFTA_SURCHARGES.get(state, 0.0)
    return round(base + surcharge, 4)


def net_price_after_ifta(card_price: float, fuel_state: str,
                          retail_price: float = None) -> float:
    """
    True net cost per gallon = card_price + estimated IFTA settlement per gallon.

    IFTA settlement per gallon = home_state_rate - stop_state_rate

    Positive = you will OWE more at quarterly settlement (stop state has low tax)
    Negative = you will get a CREDIT (stop state has high tax, already paid more)

    Example (home=FL $0.40/gal):
      Stop in TX ($0.20): net = $4.32 + ($0.40 - $0.20) = $4.52  ← owes $0.20
      Stop in IL ($0.74): net = $4.50 + ($0.40 - $0.74) = $4.16  ← gets credit
      Stop in CA ($0.97): net = $6.09 + ($0.40 - $0.97) = $5.52  ← big credit

    If IFTA_HOME_STATE not set → returns card_price unchanged (no adjustment).
    """
    if not HOME_STATE:
        return round(card_price, 4)
    fuel_rate  = get_ifta_rate(fuel_state)
    adjustment = HOME_STATE_RATE - fuel_rate   # + means owe more, - means credit
    return round(card_price + adjustment, 4)


def ifta_adjustment_per_gallon(fuel_state: str) -> float:
    """
    Return the IFTA adjustment per gallon for a stop state.
    Positive = will owe this much extra at settlement.
    Negative = will receive this as credit.
    Returns 0 if no home state configured.
    """
    if not HOME_STATE:
        return 0.0
    fuel_rate = get_ifta_rate(fuel_state)
    return round(HOME_STATE_RATE - fuel_rate, 4)


def best_stop_after_ifta(stops: list[dict]) -> list[dict]:
    """Sort stops by IFTA net price. Adds ifta_rate and net_price fields."""
    result = []
    for stop in stops:
        price = stop.get("diesel_price") or 0
        state = stop.get("state", "")
        rate  = get_ifta_rate(state)
        net   = net_price_after_ifta(price, state)
        result.append({**stop, "ifta_rate": rate, "net_price": net})
    return sorted(result, key=lambda s: s["net_price"])


def get_route_states(route: dict) -> list[str]:
    """Extract ordered list of states from QM route stops."""
    states = []
    seen   = set()
    for stop in route.get("stops", []):
        state = stop.get("state", "").upper().strip()
        if state and state not in seen:
            states.append(state)
            seen.add(state)
    return states


def check_low_stop_states_ahead(route: dict, current_state: str) -> list[dict]:
    """Return warnings for low-stop states ahead on the route."""
    route_states = get_route_states(route)
    warnings     = []
    try:
        idx          = route_states.index(current_state.upper())
        ahead_states = route_states[idx + 1:]
    except ValueError:
        ahead_states = route_states
    for state in ahead_states:
        if state in LOW_STOP_STATES:
            warnings.append({
                "state":    state,
                "name":     LOW_STOP_STATES[state]["name"],
                "reason":   LOW_STOP_STATES[state]["reason"],
                "min_fuel": LOW_STOP_MIN_FUEL.get(state, 50),
            })
    return warnings


def format_ifta_savings(best: dict, alt: dict, gallons: float) -> str | None:
    """Format IFTA savings comparison message."""
    if not best or not alt:
        return None
    pump_diff = (alt.get("diesel_price", 0) or 0) - (best.get("diesel_price", 0) or 0)
    net_diff  = (alt.get("net_price", 0) or 0) - (best.get("net_price", 0) or 0)
    if net_diff <= 0.005:
        return None
    bs = best.get("state", "")
    as_ = alt.get("state", "")
    net_sav  = round(net_diff * gallons, 2)
    pump_sav = round(pump_diff * gallons, 2)
    if bs != as_:
        return (
            f"💵 Pump saves: *${pump_diff:.3f}/gal = ${pump_sav:.0f}*\n"
            f"📋 After IFTA: saves *${net_diff:.3f}/gal = ${net_sav:.0f}*\n"
            f"   ({bs} IFTA ${best.get('ifta_rate',0):.3f} vs {as_} IFTA ${alt.get('ifta_rate',0):.3f})"
        )
    return f"💵 Saves *${pump_diff:.3f}/gal × {gallons:.0f} gal = ${pump_sav:.0f}*"


# ---------------------------------------------------------------------------
# Auto-scraper — runs quarterly to update rates
# ---------------------------------------------------------------------------

def scrape_and_update_ifta_rates() -> dict:
    """
    Fetch latest IFTA diesel rates from NY state IFTA-105 PDF.
    NY publishes the official IFTA rate table each quarter.
    Returns updated rates dict or empty dict if failed.
    """
    global IFTA_RATES, _rates_updated

    # NY tax department publishes IFTA-105 with all 48 states
    # URL pattern: https://www.tax.ny.gov/pdf/current_forms/motor/ifta105.pdf
    SOURCES = [
        "https://www.tax.ny.gov/pdf/current_forms/motor/ifta105.pdf",
        "https://tax.colorado.gov/sites/tax/files/documents/IFTA_Rate_Table_Q1_2026.pdf",
    ]

    for url in SOURCES:
        try:
            resp = requests.get(url, timeout=15)
            if not resp.ok:
                continue

            # Parse PDF text for state rates
            # Pattern: state abbreviation followed by rate code and gallon amount
            text = _extract_text_from_pdf(resp.content)
            if not text:
                continue

            new_rates = _parse_ifta_rates_from_text(text)
            if len(new_rates) >= 40:  # at least 40 states found
                IFTA_RATES.update(new_rates)
                quarter = _current_quarter()
                _rates_updated = quarter
                log.info(f"IFTA rates updated from {url} — {len(new_rates)} states, {quarter}")
                return new_rates
        except Exception as e:
            log.warning(f"IFTA scrape failed for {url}: {e}")

    log.warning("IFTA auto-update failed — using cached rates")
    return {}


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf."""
    try:
        import io
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        text   = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    except Exception as e:
        log.warning(f"PDF text extraction failed: {e}")
        return ""


def _parse_ifta_rates_from_text(text: str) -> dict:
    """
    Parse state diesel rates from IFTA-105 PDF text.
    Looks for patterns like 'Alabama AL 123 .310'
    """
    rates = {}

    # Map full state names to abbreviations
    STATE_NAMES = {
        "Alabama": "AL", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
        "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL",
        "Georgia": "GA", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN",
        "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
        "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
        "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
        "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
        "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
        "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
        "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
        "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
        "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
        "Wisconsin": "WI", "Wyoming": "WY",
    }

    lines = text.split("\n")
    for line in lines:
        line = line.strip()
        # Pattern: "Alabama AL† 123 .31 .1127 ..."
        # Diesel rate is the first decimal after the rate code
        for name, abbr in STATE_NAMES.items():
            if name in line or f" {abbr}" in line or f" {abbr}†" in line:
                # Extract the diesel rate (first .XXX after state code)
                match = re.search(rf'{abbr}[†\*]?\s+\d{{2,3}}\s+(\.\d{{2,4}})', line)
                if match:
                    try:
                        rate = float(match.group(1))
                        if 0.0 <= rate <= 1.5:  # sanity check
                            rates[abbr] = rate
                    except ValueError:
                        pass

    return rates


def _current_quarter() -> str:
    """Return current quarter string like '2026-Q1'."""
    now = datetime.now(timezone.utc)
    q   = (now.month - 1) // 3 + 1
    return f"{now.year}-Q{q}"


def should_update_rates() -> bool:
    """Check if rates should be updated (new quarter started)."""
    return _rates_updated != _current_quarter()


def get_rates_info() -> str:
    """Return info about current IFTA rates."""
    return f"IFTA rates: {_rates_updated} | {len(IFTA_RATES)} states loaded"
