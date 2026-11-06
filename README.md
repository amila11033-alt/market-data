# market-data

Free daily market data for the Narrative momentum scanner dashboard.

A GitHub Actions workflow (`.github/workflows/fetch.yml`) runs after each US close,
downloads daily prices from Yahoo Finance with `yfinance` (no API key; Stooq CSV as a
fallback), and force-pushes the results to the `data` branch:

- `weekly.json`: weekly bars (adjusted), newest first, for the curated tickers in
  `tickers.csv` plus S&P 500, Nasdaq-100 and S&P/ASX 200 members
- `daily.json`: daily bars for benchmarks (QQQ, SPY, VIX, 10-year yield, ASX 200 …)
- `universe.json`, `meta.json`

Edit `tickers.csv` to change the curated themes. Run it by hand from the Actions tab
(“Fetch market data” → Run workflow).

`scanner/engine.py` is the scoring engine the daily Claude run uses.
