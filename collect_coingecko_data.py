"""
Fetch cryptocurrency market data from CoinGecko API and store as JSON.
"""

import json
import time
import requests

BASE_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=inr&per_page=250&order=market_cap_desc&page={page}&sparkline=False"
)
NUM_PAGES = 5
DELAY_SECONDS = 15  # Wait between requests to avoid rate limit (429)
RETRY_DELAY = 60   # Wait before retry when rate limited
MAX_RETRIES = 3
OUTPUT_FILE = "coingecko_markets.json"


def fetch_page(page: int) -> list:
    """Fetch one page of market data from CoinGecko API (with retries on 429)."""
    url = BASE_URL.format(page=page)
    for attempt in range(MAX_RETRIES):
        response = requests.get(url)
        if response.status_code == 429:
            if attempt < MAX_RETRIES - 1:
                print(f"  Rate limited. Waiting {RETRY_DELAY}s before retry {attempt + 2}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY)
            else:
                response.raise_for_status()
        else:
            response.raise_for_status()
            return response.json()
    return response.json()  # unreachable if MAX_RETRIES > 0


def fetch_all_pages() -> list:
    """Fetch all pages and return combined list of coins."""
    all_data = []
    for page in range(1, NUM_PAGES + 1):
        print(f"  Fetching page {page}/{NUM_PAGES}...")
        all_data.extend(fetch_page(page))
        if page < NUM_PAGES:
            print(f"  Waiting {DELAY_SECONDS}s before next request...")
            time.sleep(DELAY_SECONDS)
    return all_data


def save_to_json(data: list, filepath: str = OUTPUT_FILE) -> None:
    """Write data to a JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to {filepath} ({len(data)} coins)")


def main() -> None:
    print("Fetching market data from CoinGecko API (all 5 pages)...")
    data = fetch_all_pages()
    save_to_json(data)
    print("Done.")


if __name__ == "__main__":
    main()
