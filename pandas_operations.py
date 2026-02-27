"""
Pandas operations on CoinGecko market data.
Loads coingecko_markets.json into a DataFrame for analysis.
"""

import pandas as pd

INPUT_JSON = "coingecko_markets.json"

COLUMNS = [
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "circulating_supply", "total_supply", "ath", "atl", "last_updated",
]


def load_dataframe(filepath: str = INPUT_JSON) -> pd.DataFrame:
    """Load CoinGecko market data from JSON into a pandas DataFrame."""
    df = pd.read_json(filepath)
    return df


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep selected columns and extract date from last_updated."""
    df = df[COLUMNS].copy()
    df["last_updated"] = pd.to_datetime(df["last_updated"]).dt.date
    return df


def main() -> None:
    print("Loading data into DataFrame...")
    df = load_dataframe()
    df = filter_columns(df)
    print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns\n")

    print("Columns:", list(df.columns))
    print("\n--- First 5 rows ---")
    print(df.head())

    print("\n--- Basic info ---")
    print(df.info())

    print("\n--- Numeric summary ---")
    print(df.describe())

    return df


if __name__ == "__main__":
    df = main()
