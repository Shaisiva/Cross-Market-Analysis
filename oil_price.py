"""
Fetch WTI daily oil prices from GitHub CSV, filter Jan 2020 – Jan 2026,
and store in the oil_price SQLite table (date PRIMARY KEY, price_usd).
"""

import pandas as pd
import requests

from sql_operation import create_oil_price_table, insert_oil_prices

WTI_CSV_URL = (
    "https://raw.githubusercontent.com/datasets/oil-prices/main/data/wti-daily.csv"
)
DATE_START = "2020-01-01"
DATE_END = "2026-01-31"


def fetch_wti_csv(verify_ssl: bool = True) -> pd.DataFrame:
    """Download WTI daily CSV and return as DataFrame with columns Date, Price."""
    response = requests.get(WTI_CSV_URL, verify=verify_ssl)
    response.raise_for_status()
    df = pd.read_csv(pd.io.common.BytesIO(response.content))
    return df


def load_wti_csv_from_file(filepath: str) -> pd.DataFrame:
    """Load WTI CSV from a local file (use if URL fetch fails due to SSL/proxy)."""
    return pd.read_csv(filepath)


def filter_date_range(
    df: pd.DataFrame,
    date_col: str = "Date",
    start: str = DATE_START,
    end: str = DATE_END,
) -> pd.DataFrame:
    """Filter rows where date is between start and end (inclusive)."""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    mask = (df[date_col] >= start_ts) & (df[date_col] <= end_ts)
    return df.loc[mask].sort_values(date_col).reset_index(drop=True)


def prepare_oil_price_rows(df: pd.DataFrame) -> list[tuple]:
    """
    Map DataFrame with Date, Price to list of (date_str, price_usd).
    date as YYYY-MM-DD, price rounded to 6 decimals.
    """
    rows = []
    for _, row in df.iterrows():
        date_str = pd.Timestamp(row["Date"]).strftime("%Y-%m-%d")
        price = round(float(row["Price"]), 6)
        rows.append((date_str, price))
    return rows


def main(
    *,
    csv_file: str | None = None,
    verify_ssl: bool = True,
) -> None:
    """
    Fetch WTI CSV (from URL or local file), filter Jan 2020 – Jan 2026, store in oil_price.
    If csv_file is set, load from that path; otherwise fetch from URL.
    Set verify_ssl=False if you get SSL errors (e.g. corporate proxy).
    """
    if csv_file:
        print(f"Loading WTI CSV from {csv_file}...")
        df = load_wti_csv_from_file(csv_file)
    else:
        print("Fetching WTI daily oil prices from GitHub...")
        try:
            df = fetch_wti_csv(verify_ssl=verify_ssl)
        except requests.exceptions.SSLError:
            print("SSL error. Retrying with verify_ssl=False...")
            df = fetch_wti_csv(verify_ssl=False)
    print(f"Filtering for {DATE_START} – {DATE_END}...")
    df_filtered = filter_date_range(df, start=DATE_START, end=DATE_END)
    rows = prepare_oil_price_rows(df_filtered)
    print(f"Storing {len(rows)} rows into oil_price table...")
    create_oil_price_table()
    insert_oil_prices(rows)
    print("Done.")


if __name__ == "__main__":
    main()
