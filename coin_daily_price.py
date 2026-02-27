"""
Collect daily price data for top coins from CoinGecko market_chart API.
Save raw data as JSON, then process and store into Crypto_prices SQLite table.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from sql_operation import create_crypto_prices_table, get_connection, insert_crypto_prices

# Top coins by CoinGecko id (market cap order)
TOP_COIN_IDS = [
    "bitcoin",
    "ethereum",
    "tether",
    "binancecoin",
    "solana",
    "ripple",
    "usd-coin",
    "cardano",
    "avalanche-2",
    "dogecoin",
]

MARKET_CHART_URL = (
    "https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    "?vs_currency=usd&days=365"
)
DELAY_SECONDS = 15
RETRY_DELAY = 60
MAX_RETRIES = 3
RAW_JSON_FILE = "coin_daily_prices_raw.json"
PROCESSED_JSON_FILE = "coin_daily_prices.json"
PROJECT_DIR = Path(__file__).resolve().parent


def fetch_market_chart(coin_id: str) -> dict | None:
    """Fetch market_chart for one coin (with retries on 429)."""
    url = MARKET_CHART_URL.format(coin_id=coin_id)
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url)
            if response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    print(f"  Rate limited. Waiting {RETRY_DELAY}s before retry...")
                    time.sleep(RETRY_DELAY)
                else:
                    response.raise_for_status()
            else:
                response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"  Error fetching {coin_id}: {e}")
            return None
    return None


def fetch_all_daily_prices() -> dict:
    """Fetch market_chart for all top coins and return { coin_id: api_response }."""
    result = {}
    for i, coin_id in enumerate(TOP_COIN_IDS, 1):
        print(f"  Fetching {i}/{len(TOP_COIN_IDS)}: {coin_id}...")
        data = fetch_market_chart(coin_id)
        if data is not None:
            result[coin_id] = data
        if i < len(TOP_COIN_IDS):
            time.sleep(DELAY_SECONDS)
    return result


def save_raw_to_json(data: dict, filepath: Path) -> None:
    """Write raw API responses to a JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Raw data saved to {filepath} ({len(data)} coins).")


def process_prices_to_rows(raw_data: dict) -> list[dict]:
    """
    Convert raw market_chart responses to a list of records:
    [{ "coin_id", "date", "price_usd" }, ...].
    Uses one price per day (last point per calendar day in UTC).
    """
    rows = []
    for coin_id, api_data in raw_data.items():
        prices = api_data.get("prices") or []
        # Group by date (UTC day) and keep last price of the day
        by_date = {}
        for ts_ms, price in prices:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            day = dt.strftime("%Y-%m-%d")
            by_date[day] = round(float(price), 6)
        for date, price_usd in sorted(by_date.items()):
            rows.append({"coin_id": coin_id, "date": date, "price_usd": price_usd})
    return rows


def save_processed_to_json(rows: list[dict], filepath: Path) -> None:
    """Write processed price rows to JSON."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"Processed data saved to {filepath} ({len(rows)} rows).")


def load_processed_json(filepath: Path) -> list[dict]:
    """Load processed price rows from JSON."""
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def store_prices_to_sqlite(rows: list[dict]) -> int:
    """Convert list of dicts to (coin_id, date, price_usd) tuples and insert into Crypto_prices."""
    tuples = [
        (r["coin_id"], r["date"], r["price_usd"])
        for r in rows
    ]
    return insert_crypto_prices(tuples)


def load_json_and_store_to_db(filepath: Path | None = None) -> int:
    """
    Load processed price rows from JSON and insert into Crypto_prices.
    Use this to (re)load from coin_daily_prices.json without re-fetching the API.
    """
    path = filepath or (PROJECT_DIR / PROCESSED_JSON_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Processed JSON not found: {path}")
    rows = load_processed_json(path)
    return store_prices_to_sqlite(rows)


def main() -> None:
    print("Fetching daily price data from CoinGecko (market_chart, 365 days)...")
    raw_data = fetch_all_daily_prices()
    if not raw_data:
        print("No data fetched. Exiting.")
        return

    raw_path = PROJECT_DIR / RAW_JSON_FILE
    save_raw_to_json(raw_data, raw_path)

    rows = process_prices_to_rows(raw_data)
    processed_path = PROJECT_DIR / PROCESSED_JSON_FILE
    save_processed_to_json(rows, processed_path)

    print("Storing data into Crypto_prices table...")
    create_crypto_prices_table()
    count = store_prices_to_sqlite(rows)
    print(f"Done. {count} rows in Crypto_prices.")


if __name__ == "__main__":
    main()
