"""
Download historical daily stock/index data for selected tickers via Yahoo Finance,
filter Jan 2020 – Sept 2025, and store in the stock_price SQLite table.
"""

import time
import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

from sql_operation import create_stock_price_table, insert_stock_prices

USE_INSECURE_SESSION = True

TICKERS = ["^GSPC", "^IXIC", "^NSEI"]
DATE_START = "2020-01-01"
DATE_END = "2025-10-01"   # slightly extended (Yahoo end date is exclusive)

DELAY_BETWEEN_TICKERS = 3
RATE_LIMIT_RETRIES = 4
RATE_LIMIT_WAIT_SEC = 45


def _get_session():
    """Create curl_cffi session with Chrome impersonation."""
    if not USE_INSECURE_SESSION:
        return None
    try:
        from curl_cffi import requests as curl_requests
        session = curl_requests.Session(impersonate="chrome110")
        session.verify = False
        return session
    except Exception as e:
        print("Failed to create custom session:", e)
        return None


def download_ticker_history(ticker: str, start: str, end: str, session=None) -> pd.DataFrame:
    """Download daily OHLCV for one ticker."""
    obj = yf.Ticker(ticker, session=session)

    df = obj.history(
        start=start,
        end=end,
        auto_adjust=False,   # safer for indices
        actions=False
    )

    print(f"{ticker} → Retrieved {len(df)} rows")

    if df.empty:
        return df

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    df = df[["open", "high", "low", "close", "volume"]]
    df.index = pd.to_datetime(df.index).tz_localize(None)

    # Remove ^ from ticker
    df["ticker"] = ticker.replace("^", "")

    return df


def download_all_tickers(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    session = _get_session()
    frames = []

    for i, t in enumerate(tickers):
        if i > 0:
            time.sleep(DELAY_BETWEEN_TICKERS)

        for attempt in range(1, RATE_LIMIT_RETRIES + 1):
            try:
                print(f"Downloading {t}...")
                df = download_ticker_history(t, start, end, session=session)
                break
            except YFRateLimitError:
                if attempt < RATE_LIMIT_RETRIES:
                    print(f"Rate limited. Waiting {RATE_LIMIT_WAIT_SEC}s...")
                    time.sleep(RATE_LIMIT_WAIT_SEC)
                else:
                    print(f"Skipping {t} after retries.")
                    df = pd.DataFrame()

        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=0)
    combined = combined.dropna(subset=["open", "high", "low", "close", "volume"])
    combined["volume"] = combined["volume"].astype(int)

    return combined.sort_index()


def prepare_stock_price_rows(df: pd.DataFrame) -> list[tuple]:
    df = df.reset_index()

    date_col = "Date" if "Date" in df.columns else df.columns[0]
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["date"],
            round(float(r["open"]), 6),
            round(float(r["high"]), 6),
            round(float(r["low"]), 6),
            round(float(r["close"]), 6),
            int(r["volume"]),
            r["ticker"],
        ))

    return rows


def main() -> None:
    print(f"Downloading historical data for {TICKERS} ({DATE_START} – {DATE_END})...")

    df = download_all_tickers(TICKERS, DATE_START, DATE_END)

    if df.empty:
        print("No data retrieved. Exiting.")
        return

    rows = prepare_stock_price_rows(df)

    print(f"Inserting {len(rows)} rows into database...")
    create_stock_price_table()
    insert_stock_prices(rows)

    print("Done.")


if __name__ == "__main__":
    main()
