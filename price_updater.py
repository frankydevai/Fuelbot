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
    Supports the daily EFS CSV format.
    """
    fname = filename.lower().strip()

    if fname.endswith('.csv'):
        try:
            # Check if it's the EFS format
            sample = file_bytes[:300].decode('utf-8-sig', errors='ignore')
            if 'Station' in sample or 'Discounted price' in sample or 'Retail price' in sample:
                from database import import_efs_csv
                return import_efs_csv(file_bytes)
        except Exception as e:
            log.error(f"EFS CSV import error: {e}", exc_info=True)
            return 0, f"❌ Failed to import CSV: `{e}`"

    return 0, (
        f"❌ Unsupported file: `{filename}`\n"
        f"Please send the daily EFS CSV file with columns:\n"
        f"Station, Address, City, State, longitude, latitude, Retail price, Discounted price"
    )
