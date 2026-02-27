"""
Streamlit app: Filters & Data Exploration, SQL Query Runner, Top 3 Crypto Analysis.
Uses Cryptocurrencies, Crypto_prices, oil_price, stock_price from sql_operation.
"""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from sql_operation import get_connection, run_query, select_cryptocurrencies, select_oil_price, select_stock_price

st.set_page_config(page_title="Market Data Report", layout="wide")

# ---------- Helpers ----------


def run_sql(query: str, params=None) -> pd.DataFrame:
    """Execute SELECT and return DataFrame. Handles empty result."""
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()


# ---------- Predefined queries for Page 2 ----------


def _queries_crypto():
    return {
        "Top 3 cryptocurrencies by market cap": """
            SELECT id, symbol, name, current_price, market_cap, market_cap_rank
            FROM Cryptocurrencies
            WHERE market_cap_rank IS NOT NULL
            ORDER BY market_cap_rank ASC
            LIMIT 3
        """,
        "Coins where circulating supply > 90% of total supply": """
            SELECT id, symbol, name, circulating_supply, total_supply,
                   ROUND(100.0 * circulating_supply / NULLIF(total_supply, 0), 2) AS pct_circulating
            FROM Cryptocurrencies
            WHERE total_supply IS NOT NULL AND total_supply > 0
              AND (1.0 * circulating_supply / total_supply) >= 0.9
            ORDER BY pct_circulating DESC
        """,
        "Coins within 10% of all-time high (ATH)": """
            SELECT id, symbol, name, current_price, ath,
                   ROUND(100.0 * current_price / NULLIF(ath, 0), 2) AS pct_of_ath
            FROM Cryptocurrencies
            WHERE ath IS NOT NULL AND ath > 0 AND current_price IS NOT NULL
              AND (1.0 * current_price / ath) >= 0.9
            ORDER BY pct_of_ath DESC
        """,
        "Average market cap rank of coins with volume > $1B": """
            SELECT ROUND(AVG(market_cap_rank), 2) AS avg_market_cap_rank
            FROM Cryptocurrencies
            WHERE total_volume >= 1e9 AND market_cap_rank IS NOT NULL
        """,
        "Most recently updated coin": """
            SELECT * FROM Cryptocurrencies
            ORDER BY last_updated DESC
            LIMIT 1
        """,
    }


def _queries_crypto_prices():
    """Queries on Crypto_prices; some need date params from app."""
    return {
        "Highest daily price of Bitcoin (last 365 days)": (
            """
            SELECT date, price_usd FROM Crypto_prices
            WHERE coin_id = 'bitcoin' AND date >= date('now', '-365 days')
            ORDER BY price_usd DESC LIMIT 1
            """,
            None,
        ),
        "Average daily price of Ethereum (past 1 year)": (
            """
            SELECT ROUND(AVG(price_usd), 2) AS avg_price FROM Crypto_prices
            WHERE coin_id = 'ethereum' AND date >= date('now', '-1 year')
            """,
            None,
        ),
        "Bitcoin daily price trend in January 2025": (
            """
            SELECT date, price_usd FROM Crypto_prices
            WHERE coin_id = 'bitcoin' AND date >= '2025-01-01' AND date < '2025-02-01'
            ORDER BY date
            """,
            None,
        ),
        "Coin with highest average price over 1 year": (
            """
            SELECT coin_id, ROUND(AVG(price_usd), 2) AS avg_price
            FROM Crypto_prices
            WHERE date >= date('now', '-1 year')
            GROUP BY coin_id
            ORDER BY avg_price DESC LIMIT 1
            """,
            None,
        ),
        "Bitcoin % change Sep 2024 vs Sep 2025": (
            """
            WITH sep24 AS (
                SELECT AVG(price_usd) AS avg_price FROM Crypto_prices
                WHERE coin_id = 'bitcoin' AND date >= '2024-09-01' AND date < '2024-10-01'
            ),
            sep25 AS (
                SELECT AVG(price_usd) AS avg_price FROM Crypto_prices
                WHERE coin_id = 'bitcoin' AND date >= '2025-09-01' AND date < '2025-10-01'
            )
            SELECT ROUND(100.0 * (sep25.avg_price - sep24.avg_price) / NULLIF(sep24.avg_price, 0), 2) AS pct_change
            FROM sep24, sep25
            """,
            None,
        ),
    }


def _queries_oil():
    return {
        "Highest oil price in the last 5 years": """
            SELECT date, price_usd FROM oil_price
            WHERE date >= date('now', '-5 years')
            ORDER BY price_usd DESC LIMIT 1
        """,
        "Average oil price per year": """
            SELECT strftime('%Y', date) AS year, ROUND(AVG(price_usd), 2) AS avg_price
            FROM oil_price
            GROUP BY year ORDER BY year
        """,
        "Oil prices during COVID crash (Mar–Apr 2020)": """
            SELECT date, price_usd FROM oil_price
            WHERE date >= '2020-03-01' AND date < '2020-05-01'
            ORDER BY date
        """,
        "Lowest oil price in the last 10 years": """
            SELECT date, price_usd FROM oil_price
            WHERE date >= date('now', '-10 years')
            ORDER BY price_usd ASC LIMIT 1
        """,
        "Oil price volatility (max - min per year)": """
            SELECT strftime('%Y', date) AS year,
                   ROUND(MIN(price_usd), 2) AS min_price,
                   ROUND(MAX(price_usd), 2) AS max_price,
                   ROUND(MAX(price_usd) - MIN(price_usd), 2) AS volatility
            FROM oil_price
            GROUP BY year ORDER BY year
        """,
    }


def _queries_stock(ticker: str):
    return {
        "All stock prices for a given ticker": (
            "SELECT * FROM stock_price WHERE ticker = ? ORDER BY date",
            [ticker],
        ),
        "Highest closing price for NASDAQ (^IXIC)": (
            "SELECT date, close FROM stock_price WHERE ticker = '^IXIC' ORDER BY close DESC LIMIT 1",
            None,
        ),
        "Top 5 days with highest (high - low) for S&P 500 (^GSPC)": (
            "SELECT date, open, high, low, close, (high - low) AS price_range FROM stock_price WHERE ticker = '^GSPC' ORDER BY (high - low) DESC LIMIT 5",
            None,
        ),
        "Monthly average closing price per ticker": (
            "SELECT ticker, strftime('%Y-%m', date) AS month, ROUND(AVG(close), 2) AS avg_close FROM stock_price GROUP BY ticker, month ORDER BY ticker, month",
            None,
        ),
        "Average trading volume of NSEI in 2024": (
            "SELECT ROUND(AVG(volume), 0) AS avg_volume FROM stock_price WHERE ticker = '^NSEI' AND date >= '2024-01-01' AND date < '2025-01-01'",
            None,
        ),
    }


def _queries_join():
    return {
        "Bitcoin vs Oil average price in 2025": """
            SELECT
                (SELECT ROUND(AVG(price_usd), 2) FROM Crypto_prices WHERE coin_id = 'bitcoin' AND date >= '2025-01-01') AS btc_avg_2025,
                (SELECT ROUND(AVG(price_usd), 2) FROM oil_price WHERE date >= '2025-01-01') AS oil_avg_2025
        """,
        "Bitcoin vs S&P 500 (correlation idea: same-date comparison)": """
            SELECT b.date, b.price_usd AS btc_price, s.close AS sp500_close
            FROM (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'bitcoin') b
            INNER JOIN (SELECT date, close FROM stock_price WHERE ticker = '^GSPC') s ON b.date = s.date
            ORDER BY b.date
        """,
        "Ethereum vs NASDAQ daily prices for 2025": """
            SELECT e.date, e.price_usd AS eth_price, n.close AS nasdaq_close
            FROM (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'ethereum') e
            INNER JOIN (SELECT date, close FROM stock_price WHERE ticker = '^IXIC') n ON e.date = n.date
            WHERE e.date >= '2025-01-01'
            ORDER BY e.date
        """,
        "Days when oil spiked vs Bitcoin price change": """
            WITH oil_daily AS (
                SELECT date, price_usd AS oil_price,
                       LAG(price_usd) OVER (ORDER BY date) AS prev_oil
                FROM oil_price
            ),
            oil_spike AS (
                SELECT date, oil_price, prev_oil, (oil_price - prev_oil) AS oil_change
                FROM oil_daily WHERE prev_oil IS NOT NULL
                ORDER BY (oil_price - prev_oil) DESC LIMIT 20
            ),
            btc AS (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'bitcoin')
            SELECT o.date, o.oil_price, o.oil_change, b.price_usd AS btc_price
            FROM oil_spike o LEFT JOIN btc b ON o.date = b.date
        """,
        "Top 3 coins daily price vs Nifty (^NSEI)": """
            WITH top3 AS (
                SELECT id FROM Cryptocurrencies WHERE market_cap_rank IS NOT NULL ORDER BY market_cap_rank LIMIT 3
            ),
            nifty AS (SELECT date, close AS nifty_close FROM stock_price WHERE ticker = '^NSEI')
            SELECT p.coin_id, p.date, p.price_usd, n.nifty_close
            FROM Crypto_prices p
            INNER JOIN top3 t ON p.coin_id = t.id
            LEFT JOIN nifty n ON p.date = n.date
            ORDER BY p.coin_id, p.date
        """,
        "S&P 500 (^GSPC) vs crude oil on same dates": """
            SELECT s.date, s.close AS sp500_close, o.price_usd AS oil_price
            FROM stock_price s
            INNER JOIN oil_price o ON s.date = o.date
            WHERE s.ticker = '^GSPC'
            ORDER BY s.date
        """,
        "Bitcoin closing price vs crude oil (same date)": """
            SELECT b.date, b.price_usd AS btc_close, o.price_usd AS oil_close
            FROM (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'bitcoin') b
            INNER JOIN oil_price o ON b.date = o.date
            ORDER BY b.date
        """,
        "NASDAQ (^IXIC) vs Ethereum price trends": """
            SELECT e.date, e.price_usd AS eth_price, n.close AS nasdaq_close
            FROM (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'ethereum') e
            INNER JOIN (SELECT date, close FROM stock_price WHERE ticker = '^IXIC') n ON e.date = n.date
            ORDER BY e.date
        """,
        "Top 3 crypto + stock indices for 2025": """
            WITH top3 AS (SELECT id FROM Cryptocurrencies WHERE market_cap_rank IS NOT NULL ORDER BY market_cap_rank LIMIT 3),
            prices AS (
                SELECT coin_id, date, price_usd FROM Crypto_prices WHERE date >= '2025-01-01'
                AND coin_id IN (SELECT id FROM top3)
            ),
            stocks AS (
                SELECT date, ticker, close FROM stock_price
                WHERE date >= '2025-01-01' AND ticker IN ('^GSPC', '^IXIC', '^NSEI')
            )
            SELECT p.date, p.coin_id, p.price_usd, s.ticker, s.close AS stock_close
            FROM prices p
            LEFT JOIN stocks s ON p.date = s.date
            ORDER BY p.date, p.coin_id
        """,
        "Multi-join: stock, oil, Bitcoin daily": """
            SELECT s.date, s.ticker, s.close AS stock_close, o.price_usd AS oil_price, b.price_usd AS btc_price
            FROM stock_price s
            LEFT JOIN oil_price o ON s.date = o.date
            LEFT JOIN (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'bitcoin') b ON s.date = b.date
            WHERE s.ticker = '^GSPC'
            ORDER BY s.date
        """,
    }


def get_all_query_names_and_sql(ticker: str = "^GSPC"):
    """Returns list of (display_name, sql, params)."""
    out = []
    for name, sql in _queries_crypto().items():
        out.append((f"1. Cryptocurrencies: {name}", sql, None))
    for name, (sql, params) in _queries_crypto_prices().items():
        out.append((f"2. Crypto_prices: {name}", sql, params))
    for name, sql in _queries_oil().items():
        out.append((f"3. Oil: {name}", sql, None))
    for name, val in _queries_stock(ticker).items():
        sql, params = (val if isinstance(val, tuple) else (val, None))
        out.append((f"4. Stock: {name}", sql, params))
    for name, sql in _queries_join().items():
        out.append((f"5. Join: {name}", sql, None))
    return out


# ---------- Page 1: Filters & Data Exploration ----------


def page_filters_exploration():
    st.header("Filters & Data Exploration")
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("Start date", value=datetime.now().date() - timedelta(days=365))
    with col2:
        end = st.date_input("End date", value=datetime.now().date())
    if start > end:
        st.warning("Start date must be before end date.")
        return
    start_s, end_s = start.isoformat(), end.isoformat()

    # Averages in range
    st.subheader("Average values in selected date range")
    btc = run_sql(
        "SELECT ROUND(AVG(price_usd), 2) AS avg_price FROM Crypto_prices WHERE coin_id = 'bitcoin' AND date >= ? AND date <= ?",
        [start_s, end_s],
    )
    oil = run_sql("SELECT ROUND(AVG(price_usd), 2) AS avg_price FROM oil_price WHERE date >= ? AND date <= ?", [start_s, end_s])
    sp = run_sql(
        "SELECT ROUND(AVG(close), 2) AS avg_close FROM stock_price WHERE ticker = '^GSPC' AND date >= ? AND date <= ?",
        [start_s, end_s],
    )
    nifty = run_sql(
        "SELECT ROUND(AVG(close), 2) AS avg_close FROM stock_price WHERE ticker = '^NSEI' AND date >= ? AND date <= ?",
        [start_s, end_s],
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bitcoin avg price (USD)", btc["avg_price"].iloc[0] if len(btc) and btc["avg_price"].iloc[0] is not None else "—")
    c2.metric("Oil avg price (USD)", oil["avg_price"].iloc[0] if len(oil) and oil["avg_price"].iloc[0] is not None else "—")
    c3.metric("S&P 500 avg close", sp["avg_close"].iloc[0] if len(sp) and sp["avg_close"].iloc[0] is not None else "—")
    c4.metric("NIFTY (^NSEI) avg close", nifty["avg_close"].iloc[0] if len(nifty) and nifty["avg_close"].iloc[0] is not None else "—")

    st.subheader("Daily market snapshot (Bitcoin, Oil, S&P 500, NIFTY)")
    snapshot_sql = """
        SELECT o.date,
               b.price_usd AS btc_price,
               o.price_usd AS oil_price,
               s.close AS sp500_close,
               n.close AS nifty_close
        FROM oil_price o
        LEFT JOIN (SELECT date, price_usd FROM Crypto_prices WHERE coin_id = 'bitcoin') b ON o.date = b.date
        LEFT JOIN (SELECT date, close FROM stock_price WHERE ticker = '^GSPC') s ON o.date = s.date
        LEFT JOIN (SELECT date, close FROM stock_price WHERE ticker = '^NSEI') n ON o.date = n.date
        WHERE o.date >= ? AND o.date <= ?
        ORDER BY o.date
    """
    snapshot = run_sql(snapshot_sql, [start_s, end_s])
    if snapshot.empty:
        st.info("No data in this date range.")
    else:
        st.dataframe(snapshot, use_container_width=True)


# ---------- Page 2: SQL Query Runner ----------


def page_sql_runner():
    st.header("SQL Query Runner")
    ticker = st.sidebar.selectbox("Ticker (for ticker-specific queries)", ["^GSPC", "^IXIC", "^NSEI"], index=0)
    options = get_all_query_names_and_sql(ticker)
    names = [x[0] for x in options]
    selected = st.selectbox("Select a predefined query", names)
    if st.button("Run Query"):
        idx = names.index(selected)
        _, sql, params = options[idx]
        try:
            df = run_sql(sql, params)
            st.dataframe(df, use_container_width=True)
            st.caption(f"Rows: {len(df)}")
        except Exception as e:
            st.error(str(e))


# ---------- Page 3: Top 3 Crypto Analysis ----------


def page_top3_crypto():
    st.header("Top 3 Crypto Analysis")
    top3_df = run_sql("""
        SELECT id, symbol, name, market_cap_rank
        FROM Cryptocurrencies
        WHERE market_cap_rank IS NOT NULL
        ORDER BY market_cap_rank ASC
        LIMIT 3
    """)
    if top3_df.empty:
        st.warning("No top 3 coins found in Cryptocurrencies table.")
        return
    coin_options = list(top3_df.apply(lambda r: f"{r['name']} ({r['id']})", axis=1))
    coin_id_map = dict(zip(coin_options, top3_df["id"]))
    selected_label = st.selectbox("Select cryptocurrency", coin_options)
    coin_id = coin_id_map[selected_label]

    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("Start date", key="p3_start", value=datetime.now().date() - timedelta(days=90))
    with col2:
        end = st.date_input("End date", key="p3_end", value=datetime.now().date())
    if start > end:
        st.warning("Start date must be before end date.")
        return

    df = run_sql(
        "SELECT date, price_usd FROM Crypto_prices WHERE coin_id = ? AND date >= ? AND date <= ? ORDER BY date",
        [coin_id, start.isoformat(), end.isoformat()],
    )
    if df.empty:
        st.info("No daily prices in this range.")
        return
    st.subheader("Daily price trend")
    st.line_chart(df.set_index("date")["price_usd"])
    st.subheader("Daily price table")
    st.dataframe(df, use_container_width=True)


# ---------- Main ----------


def main():
    st.sidebar.title("Market Data Report")
    page = st.sidebar.radio(
        "Page",
        ["Filters & Data Exploration", "SQL Query Runner", "Top 3 Crypto Analysis"],
        index=0,
    )
    if page == "Filters & Data Exploration":
        page_filters_exploration()
    elif page == "SQL Query Runner":
        page_sql_runner()
    else:
        page_top3_crypto()


if __name__ == "__main__":
    main()
