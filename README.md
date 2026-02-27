# Cross-Market-Analysis
Cross-Market Analysis: Crypto, Oil &amp; Stocks with SQL and Python

# Data Collection 022025

A data pipeline that collects cryptocurrency (CoinGecko), oil (WTI), and stock/index (Yahoo Finance) data into a SQLite database, with a Streamlit app for exploration and SQL-based analytics.

---

## Prerequisites

- **Python 3.10+** (3.11 or 3.12 recommended)
- **pip** (or `py -m pip` on Windows)

---

## 1. Clone and set up the project

```bash
cd DataCollection022025
```

Create and activate a virtual environment (recommended):

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

This installs: `requests`, `pandas`, `yfinance`, `streamlit`.

---

## 3. Database and data collection (order matters)

All scripts write to **`cryptocurrencies.db`** in the project root. Run in this order so that tables exist and referenced data is available.

### Step 3.1 – Cryptocurrency snapshot (CoinGecko)

1. **Fetch market data from CoinGecko API** and save as JSON:
   ```bash
   python collect_coingecko_data.py
   ```
   - Output: `coingecko_markets.json`
   - Uses a delay between pages to avoid rate limits (429).

2. **Load into the database** (creates `Cryptocurrencies` table and fills it from the JSON):
   ```bash
   python sql_operation.py
   ```
   - Or in code: call `load_and_push_to_cryptocurrencies()` from `sql_operation`.

### Step 3.2 – Daily crypto prices (CoinGecko)

Collect daily price history for top coins and store in `Crypto_prices`:

```bash
python coin_daily_price.py
```

- Fetches from CoinGecko market_chart API for a set of coin IDs (e.g. bitcoin, ethereum).
- Writes to table `Crypto_prices` (coin_id, date, price_usd).

### Step 3.3 – Oil prices (WTI)

Download WTI daily prices from a public CSV and load into `oil_price`:

```bash
python oil_price.py
```

- Source: GitHub `datasets/oil-prices` (WTI daily).
- Table: `oil_price` (date, price_usd).

### Step 3.4 – Stock/index prices (Yahoo Finance)

Download historical daily OHLCV for selected tickers and store in `stock_price`:

```bash
python stock_price.py
```

- Tickers: **^GSPC** (S&P 500), **^IXIC** (NASDAQ), **^NSEI** (Nifty).
- Date range: Jan 2020 – Sept 2025 (configurable in script).
- Table: `stock_price` (date, open, high, low, close, volume, ticker).

---

## 4. Run the Streamlit report app

Use **two separate commands** (do not pass `streamlit run` to `pip install`):

```bash
pip install -r requirements.txt
streamlit run data_report.py
```

Or, if dependencies are already installed:

```bash
streamlit run data_report.py
```

The app opens in the browser with three pages:

| Page | Description |
|------|-------------|
| **Filters & Data Exploration** | Pick a date range; see average Bitcoin, Oil, S&P 500, and NIFTY for that range, plus a daily snapshot table (JOIN of all four). |
| **SQL Query Runner** | Choose a predefined query (crypto, crypto_prices, oil, stock, join/cross-market) and run it; results shown in a table. |
| **Top 3 Crypto Analysis** | Select one of the top 3 coins by market cap, set a date range, view daily price trend (line chart) and table. |

---

## 5. Project structure (main files)

| File | Purpose |
|------|---------|
| `requirements.txt` | Python dependencies (requests, pandas, yfinance, streamlit). |
| `sql_operation.py` | SQLite helpers: create/insert/select for `Cryptocurrencies`, `Crypto_prices`, `oil_price`, `stock_price`; `run_query()` for custom SQL. |
| `collect_coingecko_data.py` | Fetches CoinGecko markets API → `coingecko_markets.json`. |
| `pandas_operations.py` | Load/filter CoinGecko JSON for DB load. |
| `coin_daily_price.py` | Fetches daily prices for top coins → `Crypto_prices`. |
| `oil_price.py` | Fetches WTI CSV → `oil_price`. |
| `stock_price.py` | Fetches Yahoo Finance history → `stock_price`. |
| `data_report.py` | Streamlit app (Filters, SQL Runner, Top 3 Crypto). |
| `cryptocurrencies.db` | SQLite database (created when you run the scripts above). |

---

## 6. Database tables (summary)

- **Cryptocurrencies** – id, symbol, name, current_price, market_cap, market_cap_rank, total_volume, circulating_supply, total_supply, ath, atl, last_updated.
- **Crypto_prices** – coin_id, date, price_usd (daily prices for selected coins).
- **oil_price** – date, price_usd (WTI per barrel).
- **stock_price** – date, open, high, low, close, volume, ticker (e.g. ^GSPC, ^IXIC, ^NSEI).

---

## 7. Optional: run only SQL operations

If you already have `coingecko_markets.json` and want to (re)load only the Cryptocurrencies table:

```bash
python -c "from sql_operation import load_and_push_to_cryptocurrencies; load_and_push_to_cryptocurrencies()"
```

---

# Issues and solutions

Common issues encountered during setup and usage, and how to fix them.

---

### 1. **Import "yfinance" could not be resolved (linter/IDE)**

- **Cause:** The type checker or IDE cannot find the `yfinance` package (e.g. wrong interpreter or package not installed).
- **Solution:**
  - Install in the environment used by the IDE: `pip install yfinance` (or `pip install -r requirements.txt`).
  - In `stock_price.py`, the line `import yfinance as yf  # type: ignore[import-untyped]` suppresses the linter warning when the package is installed but the checker still does not see it.

---

### 2. **ModuleNotFoundError: No module named 'yfinance'**

- **Cause:** The Python interpreter used to run the script does not have `yfinance` installed.
- **Solution:** Install dependencies in that environment:
  ```bash
  pip install -r requirements.txt
  ```
  Or only yfinance:
  ```bash
  pip install yfinance
  ```
  On Windows, if `pip` is not on PATH, use:
  ```bash
  py -m pip install -r requirements.txt
  ```

---

### 3. **SSL certificate problem: self signed certificate in certificate chain (yfinance/curl_cffi)**

- **Cause:** Corporate proxy or firewall performs HTTPS inspection with a self-signed certificate, so the default SSL verification fails when calling Yahoo Finance.
- **Solution:** In `stock_price.py`, SSL verification is disabled for the Yahoo session when `USE_INSECURE_SESSION = True`. The script creates a `curl_cffi` session with `verify=False` and passes it to `yf.Ticker(..., session=session)`. Use this only in trusted environments (e.g. behind company firewall). Set `USE_INSECURE_SESSION = False` when you do not need to bypass SSL verification.

---

### 4. **YFRateLimitError: Too Many Requests. Rate limited.**

- **Cause:** Yahoo Finance (or an intermediate proxy) is rate-limiting requests.
- **Solution:** In `stock_price.py`, retry and throttling are implemented:
  - **DELAY_BETWEEN_TICKERS** (e.g. 3 seconds) between each ticker.
  - **RATE_LIMIT_RETRIES** (e.g. 4) attempts per ticker on rate limit.
  - **RATE_LIMIT_WAIT_SEC** (e.g. 45) seconds wait before each retry.
  If rate limits persist, increase `RATE_LIMIT_WAIT_SEC` (e.g. 60–90) or `DELAY_BETWEEN_TICKERS` (e.g. 5).

---

### 5. **pip install -r requirements.txt streamlit run data_report.py fails**

- **Cause:** A single command was used; `pip install` treats every word after `requirements.txt` as a package name, so it tries to install `streamlit`, `run`, and `data_report.py`, leading to errors like “No matching distribution found for data_report.py”.
- **Solution:** Use two separate commands:
  ```bash
  pip install -r requirements.txt
  streamlit run data_report.py
  ```
  Do not pass `streamlit run data_report.py` to `pip install`.

---

### 6. **CoinGecko 429 (rate limit)**

- **Cause:** Too many requests to the CoinGecko API in a short time.
- **Solution:** `collect_coingecko_data.py` and `coin_daily_price.py` use delays (e.g. 15 s between requests) and retries with a longer wait (e.g. 60 s) on 429. If you still hit limits, increase delays or reduce the number of pages/coins per run.

---

### 7. **Oil/oil_price: URL or SSL error when fetching WTI CSV**

- **Cause:** Network or corporate proxy blocking or altering HTTPS to the CSV URL.
- **Solution:** In `oil_price.py`, `fetch_wti_csv(verify_ssl=False)` can be used to disable SSL verification for the request. Alternatively, download the WTI CSV manually from the project’s documented URL and use `load_wti_csv_from_file(filepath)` to load from a local file.

---

### 8. **Streamlit or database not found**

- **Cause:** Running `streamlit` or scripts from a different working directory or environment where `cryptocurrencies.db` or the project root is not in use.
- **Solution:** Run all commands from the project root `DataCollection022025`, and use the same virtual environment where you ran `pip install -r requirements.txt`. For Streamlit, run: `streamlit run data_report.py` from the project root so that `sql_operation` finds `cryptocurrencies.db` in the current directory.

---

### 9. **Empty or missing data in the report app**

- **Cause:** Tables have not been created or populated, or the date range has no data.
- **Solution:** Run the data collection steps in order (see **Section 3**): `collect_coingecko_data.py` → `sql_operation.py` (for Cryptocurrencies) → `coin_daily_price.py` → `oil_price.py` → `stock_price.py`. Ensure each step completes without errors. For Filters & Data Exploration, pick a date range that overlaps your loaded data (e.g. 2020–2025 for stocks and oil).

---

*Last updated for DataCollection022025 (Feb 2025).*

