# Data Collection 022025 — Presentation

<!--
  How to use this for your PPT:
  1. Generate .pptx: pip install python-pptx && python build_presentation.py
     → Creates DataCollection022025_presentation.pptx
  2. Copy each "---" separated block as one slide into PowerPoint/Google Slides.
  3. Or install "Marp for VS Code" and export to PDF/PPT: https://marp.app/
  4. Or use Marp CLI: npx @marp-team/marp-cli PRESENTATION.md --pptx
-->

---
<!-- _class: lead -->
# Data Collection 022025
## Crypto, Oil & Stock Data Pipeline + Analytics Dashboard

**Project overview & outcomes**

---

# Agenda

1. Project objectives & scope  
2. Architecture & data flow  
3. Data sources & collection  
4. Database design  
5. Streamlit analytics app  
6. Outcomes & deliverables  
7. Challenges & solutions  
8. Demo & next steps  

---

# 1. Project objectives & scope

- **Goal:** Build an end-to-end pipeline that collects multi-asset market data and provides SQL-based analytics and visualization.
- **Scope:**
  - **Cryptocurrencies** — snapshot (market cap, ATH, supply) + daily prices for top coins  
  - **Oil** — WTI crude daily prices  
  - **Stocks/Indices** — S&P 500 (^GSPC), NASDAQ (^IXIC), Nifty (^NSEI) daily OHLCV  
- **Outcome:** Single SQLite database + Streamlit app for filters, predefined SQL queries, and cross-market analysis.

---

# 2. High-level architecture

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  CoinGecko API  │   │  WTI CSV (GitHub)│   │ Yahoo Finance   │
│  (crypto snapshot│   │  (oil daily)    │   │ (stocks/indices)│
│  + daily prices)│   │                 │   │                 │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Python scripts (collect_*, oil_price, stock_price,          │
│  pandas_operations, sql_operation)                            │
└─────────────────────────────┬─────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  SQLite: cryptocurrencies.db                                 │
│  Tables: Cryptocurrencies, Crypto_prices, oil_price, stock_price
└─────────────────────────────┬─────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Streamlit app (data_report.py) — Filters, SQL Runner,       │
│  Top 3 Crypto analysis                                       │
└─────────────────────────────────────────────────────────────┘
```

---

# 3. Data sources & collection

| Source | Script | Data | Period / scope |
|--------|--------|------|----------------|
| **CoinGecko API** | `collect_coingecko_data.py` | Market snapshot (cap, rank, ATH, supply) | Current snapshot → JSON |
| **CoinGecko API** | `coin_daily_price.py` | Daily USD price per coin | Top coins, configurable days |
| **GitHub (WTI CSV)** | `oil_price.py` | WTI crude $/barrel daily | e.g. Jan 2020 – Jan 2026 |
| **Yahoo Finance** | `stock_price.py` | OHLCV for ^GSPC, ^IXIC, ^NSEI | e.g. Jan 2020 – Sept 2025 |

All pipelines include error handling, retries, and (where needed) rate-limit and SSL workarounds.

---

# 4. Database design (SQLite)

- **Single file:** `cryptocurrencies.db`

| Table | Key columns | Purpose |
|-------|-------------|---------|
| **Cryptocurrencies** | id (PK), symbol, name, market_cap_rank, current_price, ath, atl, circulating_supply, total_supply, last_updated | Snapshot of coins from CoinGecko |
| **Crypto_prices** | (coin_id, date) PK, price_usd | Daily closing price per coin |
| **oil_price** | date (PK), price_usd | WTI daily price |
| **stock_price** | (ticker, date) PK, open, high, low, close, volume | Daily OHLCV per ticker |

- **sql_operation.py** provides: table creation, insert/upsert, select helpers, and `run_query()` for custom SQL.

---

# 5. Streamlit app — Three pages

**Page 1: Filters & Data Exploration**
- Date range picker (start / end).
- **Metrics:** Average Bitcoin price, Oil price, S&P 500 close, NIFTY close in that range.
- **Daily snapshot table:** JOIN of Bitcoin, Oil, S&P 500, NIFTY by date (one row per day).

**Page 2: SQL Query Runner**
- Dropdown of **predefined queries** (Cryptocurrencies, Crypto_prices, Oil, Stock, Join/cross-market).
- Optional ticker selector for stock queries.
- “Run Query” → results in a table.

**Page 3: Top 3 Crypto Analysis**
- Select one of top 3 coins by market cap.
- Date range filter.
- **Line chart** of daily price + **daily price table**.

---

# 6. Predefined analytics (examples)

- **Crypto:** Top 3 by market cap; coins with circulating supply > 90% of total; coins within 10% of ATH; avg market cap rank (volume > $1B); most recently updated coin.
- **Crypto_prices:** Bitcoin max daily (365d); Ethereum avg (1y); Bitcoin trend Jan 2025; coin with highest avg price (1y); Bitcoin % change Sep 2024 vs Sep 2025.
- **Oil:** Max/min in 5y/10y; avg per year; COVID period (Mar–Apr 2020); volatility (max−min per year).
- **Stocks:** All prices per ticker; NASDAQ max close; top 5 days (high−low) for S&P 500; monthly avg close; NSEI avg volume 2024.
- **Joins:** Bitcoin vs Oil 2025; Bitcoin vs S&P; Ethereum vs NASDAQ; oil spike vs Bitcoin; top 3 coins vs Nifty; S&P vs oil; multi-join (stock + oil + Bitcoin).

---

# 7. Outcomes & deliverables

- **Pipeline:** Automated collection from CoinGecko, WTI CSV, and Yahoo Finance into one SQLite DB.
- **Data quality:** Date filtering, type safety (DECIMAL/BIGINT), primary keys and (where used) foreign keys.
- **Analytics:** 25+ predefined SQL queries covering single-asset and cross-market questions.
- **UI:** Streamlit app with date filters, metrics, tables, and charts.
- **Documentation:** README with setup, run order, and issue/solution notes.
- **Robustness:** Retries, rate-limit handling, and optional SSL bypass for corporate environments.

---

# 8. Challenges & solutions (summary)

| Issue | Solution |
|-------|----------|
| **yfinance not found (linter/runtime)** | Install with `pip install -r requirements.txt`; use same env for run and IDE. |
| **SSL certificate error (proxy)** | Use `curl_cffi` session with `verify=False` in `stock_price.py` when `USE_INSECURE_SESSION = True`. |
| **Yahoo rate limit (429)** | Retries with backoff (e.g. 45s) and delay between tickers (e.g. 3s). |
| **CoinGecko 429** | Delays between requests (e.g. 15s) and retries with longer wait. |
| **Wrong pip command** | Use two commands: `pip install -r requirements.txt` then `streamlit run data_report.py`. |

---

# 9. Tech stack

- **Language:** Python 3.10+  
- **Libraries:** requests, pandas, yfinance, streamlit  
- **Database:** SQLite3  
- **APIs / sources:** CoinGecko (REST), GitHub (WTI CSV), Yahoo Finance (via yfinance)  
- **Front-end:** Streamlit (metrics, date filters, dataframes, line chart)  

---

# 10. How to run (recap)

```bash
# 1. Setup
pip install -r requirements.txt

# 2. Collect data (order matters)
python collect_coingecko_data.py
python sql_operation.py
python coin_daily_price.py
python oil_price.py
python stock_price.py

# 3. Launch app
streamlit run data_report.py
```

---

# 11. Demo flow suggestion

1. Show **Filters & Data Exploration** — pick a date range, show averages and daily snapshot table.  
2. Show **SQL Query Runner** — run 2–3 queries (e.g. top 3 crypto, oil volatility, Bitcoin vs Oil).  
3. Show **Top 3 Crypto** — select a coin, set range, show line chart and table.  
4. Optionally show **README** and **Issues & solutions** section for reproducibility.

---

<!-- _class: lead -->
# Thank you

**Q & A**

---

# Backup: Project file map

| File | Role |
|------|------|
| `collect_coingecko_data.py` | Fetch CoinGecko markets → JSON |
| `pandas_operations.py` | Load/filter CoinGecko JSON |
| `coin_daily_price.py` | Daily crypto prices → Crypto_prices |
| `oil_price.py` | WTI CSV → oil_price |
| `stock_price.py` | Yahoo Finance → stock_price |
| `sql_operation.py` | DB create/insert/select/run_query |
| `data_report.py` | Streamlit 3-page app |
| `README.md` | Setup, run order, issues & solutions |
