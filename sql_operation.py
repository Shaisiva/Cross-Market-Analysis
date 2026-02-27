"""
SQLite operations for CoinGecko cryptocurrency data.
Creates and manages the Cryptocurrencies table.
Loads the filtered DataFrame from pandas_operations and pushes it to Cryptocurrencies.
"""

import sqlite3

import pandas as pd

from pandas_operations import filter_columns, load_dataframe

DB_PATH = "cryptocurrencies.db"


def get_connection() -> sqlite3.Connection:
    """Return a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def create_cryptocurrencies_table(conn: sqlite3.Connection | None = None) -> None:
    """
    Create the Cryptocurrencies table with the schema:
    id (PK), symbol, name, current_price, market_cap, market_cap_rank,
    total_volume, circulating_supply, total_supply, ath, atl, last_updated.
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS Cryptocurrencies (
        id              VARCHAR(50) PRIMARY KEY,
        symbol          VARCHAR(10) NOT NULL,
        name            VARCHAR(100) NOT NULL,
        current_price   DECIMAL(18, 6),
        market_cap      DECIMAL(20, 6),
        market_cap_rank INTEGER,
        total_volume    DECIMAL(20, 6),
        circulating_supply DECIMAL(20, 6),
        total_supply    DECIMAL(20, 6),
        ath             DECIMAL(18, 6),
        atl             DECIMAL(18, 6),
        last_updated    TEXT
    );
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(create_sql)
        conn.commit()
        print("Table 'Cryptocurrencies' created successfully.")
    finally:
        if own_conn:
            conn.close()


def get_filtered_dataframe(filepath: str = "coingecko_markets.json") -> pd.DataFrame:
    """Load and filter CoinGecko data from JSON (same as pandas_operations pipeline)."""
    df = load_dataframe(filepath)
    return filter_columns(df)


def push_dataframe_to_table(df: pd.DataFrame, conn: sqlite3.Connection | None = None) -> int:
    """
    Push the DataFrame (filtered Cryptocurrencies columns) into the Cryptocurrencies table.
    Replaces existing rows so each run is a full refresh. Returns number of rows inserted.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        create_cryptocurrencies_table(conn)
        conn.execute("DELETE FROM Cryptocurrencies")
        df = df.copy()
        if "last_updated" in df.columns:
            df["last_updated"] = df["last_updated"].astype(str)
        df.to_sql("Cryptocurrencies", conn, if_exists="append", index=False)
        conn.commit()
        count = len(df)
        print(f"Pushed {count} rows to Cryptocurrencies.")
        return count
    finally:
        if own_conn:
            conn.close()


def load_and_push_to_cryptocurrencies(filepath: str = "coingecko_markets.json") -> int:
    """
    Load JSON via pandas_operations (filtered DataFrame), then push to Cryptocurrencies table.
    """
    df = get_filtered_dataframe(filepath)
    return push_dataframe_to_table(df)


def create_crypto_prices_table(conn: sqlite3.Connection | None = None) -> None:
    """
    Create the Crypto_prices table with the schema:
    coin_id (FK to Cryptocurrencies.id), date, price_usd.
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS Crypto_prices (
        coin_id   VARCHAR(50) NOT NULL,
        date      DATE NOT NULL,
        price_usd DECIMAL(18, 6) NOT NULL,
        PRIMARY KEY (coin_id, date),
        FOREIGN KEY (coin_id) REFERENCES Cryptocurrencies(id)
    );
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(create_sql)
        conn.commit()
        print("Table 'Crypto_prices' created successfully.")
    finally:
        if own_conn:
            conn.close()


def insert_crypto_prices(rows: list[tuple], conn: sqlite3.Connection | None = None) -> int:
    """
    Insert (coin_id, date, price_usd) rows into Crypto_prices.
    Uses REPLACE so re-running upserts by (coin_id, date). Returns number of rows.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        create_crypto_prices_table(conn)
        conn.executemany(
            "REPLACE INTO Crypto_prices (coin_id, date, price_usd) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()
        count = len(rows)
        print(f"Inserted {count} rows into Crypto_prices.")
        return count
    finally:
        if own_conn:
            conn.close()


def select_cryptocurrencies(limit: int | None = None) -> pd.DataFrame:
    """
    Run SELECT * FROM Cryptocurrencies and return the result as a DataFrame.
    Optionally pass limit=N to return only the first N rows.
    """
    query = "SELECT * FROM Cryptocurrencies"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def create_oil_price_table(conn: sqlite3.Connection | None = None) -> None:
    """
    Create the oil_price table with the schema:
    date (DATE PRIMARY KEY), price_usd (DECIMAL(18, 6) - WTI crude oil price per barrel USD).
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS oil_price (
        date      DATE PRIMARY KEY,
        price_usd DECIMAL(18, 6) NOT NULL
    );
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(create_sql)
        conn.commit()
        print("Table 'oil_price' created successfully.")
    finally:
        if own_conn:
            conn.close()


def insert_oil_prices(rows: list[tuple], conn: sqlite3.Connection | None = None) -> int:
    """
    Insert (date, price_usd) rows into oil_price.
    Uses REPLACE so re-running upserts by date. Returns number of rows.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        create_oil_price_table(conn)
        conn.executemany(
            "REPLACE INTO oil_price (date, price_usd) VALUES (?, ?)",
            rows,
        )
        conn.commit()
        count = len(rows)
        print(f"Inserted {count} rows into oil_price.")
        return count
    finally:
        if own_conn:
            conn.close()


def select_oil_price(limit: int | None = None) -> pd.DataFrame:
    """
    Run SELECT * FROM oil_price and return the result as a DataFrame.
    Optionally pass limit=N to return only the first N rows.
    """
    query = "SELECT * FROM oil_price ORDER BY date"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def create_stock_price_table(conn: sqlite3.Connection | None = None) -> None:
    """
    Create the stock_price table with the schema:
    date (trading date), open, high, low, close (DECIMAL 18,6), volume (BIGINT), ticker (VARCHAR 20).
    PRIMARY KEY (ticker, date).
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS stock_price (
        date   DATE NOT NULL,
        open   DECIMAL(18, 6) NOT NULL,
        high   DECIMAL(18, 6) NOT NULL,
        low    DECIMAL(18, 6) NOT NULL,
        close  DECIMAL(18, 6) NOT NULL,
        volume BIGINT NOT NULL,
        ticker VARCHAR(20) NOT NULL,
        PRIMARY KEY (ticker, date)
    );
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(create_sql)
        conn.commit()
        print("Table 'stock_price' created successfully.")
    finally:
        if own_conn:
            conn.close()


def insert_stock_prices(rows: list[tuple], conn: sqlite3.Connection | None = None) -> int:
    """
    Insert (date, open, high, low, close, volume, ticker) rows into stock_price.
    Uses REPLACE so re-running upserts by (ticker, date). Returns number of rows.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        create_stock_price_table(conn)
        conn.executemany(
            "REPLACE INTO stock_price (date, open, high, low, close, volume, ticker) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        count = len(rows)
        print(f"Inserted {count} rows into stock_price.")
        return count
    finally:
        if own_conn:
            conn.close()


def select_stock_price(
    ticker: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Run SELECT * FROM stock_price and return the result as a DataFrame.
    Optionally filter by ticker and/or limit rows. Results ordered by ticker, date.
    """
    query = "SELECT * FROM stock_price"
    params = []
    if ticker is not None:
        query += " WHERE ticker = ?"
        params.append(ticker)
    query += " ORDER BY ticker, date"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=params if params else None)
    finally:
        conn.close()


def run_query(query: str, params: list | tuple | None = None) -> pd.DataFrame:
    """
    Execute a SELECT query and return the result as a DataFrame.
    Use ? placeholders and pass params for parameterized queries.
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()


def main() -> None:
    create_cryptocurrencies_table()
    load_and_push_to_cryptocurrencies()
    print("\n--- Cryptocurrencies (first 5 rows) ---")
    print(select_cryptocurrencies(limit=5))
    create_oil_price_table()
    print("\n--- oil_price (first 10 rows) ---")
    print(select_oil_price(limit=10))
    create_stock_price_table()
    print("\n--- stock_price (first 10 rows) ---")
    print(select_stock_price(limit=10))


if __name__ == "__main__":
    main()
