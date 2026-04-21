"""
price_updater.py — Handle daily fuel price file uploads

Accepts the EFS CSV format:
  Station, Address, City, State, longitude, latitude, Retail price, Discounted price

Admin sends this file to the bot every day in Telegram.
Bot auto-detects it and reloads all station prices.
"""

import logging
log = logging.getLogger(__name__)


def update_from_file(file_bytes: bytes, filename: str) -> tuple[int, str]:
    """
    Parse uploaded file and update fuel prices in DB.
    Supports the daily EFS CSV format and cleaned CSV variants.
    """
    fname = filename.lower().strip()

    if fname.endswith('.csv'):
        try:
            from database import import_efs_csv
            return import_efs_csv(file_bytes)
        except Exception as e:
            log.error(f"EFS CSV import error: {e}", exc_info=True)
            return 0, f"❌ Failed to import CSV: `{e}`"

    return 0, (
        f"❌ Unsupported file: `{filename}`\n"
        f"Please send a fuel price CSV with station, city/state, coordinates, and card price columns."
    )
