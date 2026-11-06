#!/usr/bin/env python3
"""Daily market-data fetch, run by GitHub Actions after the US close.

Sources (no API key): Yahoo Finance via yfinance, with Stooq CSV as a fallback
for US tickers Yahoo fails on. Universe = tickers.csv (curated themes and
benchmarks) + S&P 500 + Nasdaq-100 + S&P/ASX 200 constituents from Wikipedia.

Writes to out/:
  weekly.json    {symbol: {d,o,h,l,c,v}} weekly bars, newest first (adjusted)
  daily.json     {symbol: {d,o,h,l,c,v}} daily bars for benchmarks, newest first
  universe.json  [{symbol, market, theme, name, sector, index, curated}]
  meta.json      run time, latest bar date, counts, failures
"""
import datetime as dt
import io
import json
import os
import sys
import time

import pandas as pd
import requests
import yfinance as yf

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
OUT = "out"
WEEKS_STOCK, WEEKS_BENCH, DAYS_BENCH = 110, 175, 320
DAILY_SYMS = ["QQQ", "SPY", "SMH", "RSP", "IWM", "HYG", "IEF", "^NDX", "^GSPC", "^VIX", "^TNX", "^AXJO"]


def log(*a):
    print(*a, flush=True)


def wiki_tables(url):
    html = requests.get(url, headers=UA, timeout=30).text
    return pd.read_html(io.StringIO(html))


def pick(tables, cols, lo, hi):
    for t in tables:
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [" ".join(str(x) for x in c) for c in t.columns]
        hit = next((c for c in t.columns for want in cols if str(c).split("[")[0].strip() == want), None)
        hit = hit or next((c for c in t.columns for want in cols if want.lower() in str(c).lower()), None)
        if hit is not None and lo <= len(t) <= hi:
            return t, hit
    return None, None


def index_members():
    members = {}
    try:
        t, c = pick(wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"), ["Symbol"], 450, 560)
        for _, r in t.iterrows():
            s = str(r[c]).strip().replace(".", "-")
            members[s] = {"symbol": s, "market": "US", "name": str(r.get("Security", s)), "sector": str(r.get("GICS Sector", "")), "index": "S&P 500"}
        log("S&P 500 members:", len(t))
    except Exception as e:
        log("S&P 500 list failed:", e)
    try:
        t = None
        for url in ["https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
                    "https://en.wikipedia.org/wiki/List_of_Nasdaq-100_companies",
                    "https://en.wikipedia.org/wiki/Nasdaq-100"]:
            try:
                t, c = pick(wiki_tables(url), ["Ticker", "Symbol"], 90, 130)
            except Exception as e:
                log("  ", url, e)
            if t is not None:
                break
        n = 0
        for _, r in t.iterrows():
            s = str(r[c]).strip().replace(".", "-")
            sec = str(r.get("GICS Sector", r.get("Industry", "")))
            if s in members:
                members[s]["index"] += ", Nasdaq-100"
            else:
                members[s] = {"symbol": s, "market": "US", "name": str(r.get("Company", s)), "sector": sec, "index": "Nasdaq-100"}
            n += 1
        log("Nasdaq-100 members:", n)
    except Exception as e:
        log("Nasdaq-100 list failed:", e)
    try:
        t, c = pick(wiki_tables("https://en.wikipedia.org/wiki/S%26P/ASX_200"), ["Code", "ASX code", "Ticker"], 150, 260)
        for _, r in t.iterrows():
            s = str(r[c]).strip().upper() + ".AX"
            members[s] = {"symbol": s, "market": "ASX", "name": str(r.get("Company", s)), "sector": str(r.get("Sector", r.get("GICS Sector", ""))), "index": "ASX 200"}
        log("ASX 200 members:", len(t))
    except Exception as e:
        log("ASX 200 list failed:", e)
    return members


def build_universe():
    cur = pd.read_csv("tickers.csv")
    uni = index_members()
    if len(uni) < 300 and os.path.exists("universe_last.json"):
        log("index lists incomplete; reusing last universe")
        for u in json.load(open("universe_last.json")):
            uni.setdefault(u["symbol"], u)
    for _, r in cur.iterrows():
        s = r["symbol"]
        base = uni.get(s, {"symbol": s, "market": r["market"], "name": r["name"], "sector": "", "index": ""})
        base.update({"theme": r["theme"], "name": r["name"], "curated": True, "market": r["market"]})
        uni[s] = base
    for u in uni.values():
        u.setdefault("theme", "")
        u.setdefault("curated", False)
    return uni


def to_bars(df):
    df = df.dropna(subset=["Close"])
    df = df[df["Close"] > 0]
    return df


def weekly(df, n):
    per = df.index.to_period("W-FRI")
    g = df.groupby(per)
    w = pd.DataFrame({
        "d": df.index.to_series().groupby(per).max(),
        "o": g["Open"].first(), "h": g["High"].max(), "l": g["Low"].min(),
        "c": g["Close"].last(), "v": g["Volume"].sum(),
    }).dropna(subset=["c"])
    w = w.iloc[::-1].head(n)
    return pack(w["d"], w)


def daily(df, n):
    d = df.iloc[::-1].head(n)
    w = pd.DataFrame({"o": d["Open"], "h": d["High"], "l": d["Low"], "c": d["Close"], "v": d["Volume"]})
    return pack(d.index.to_series(), w)


def pack(dates, w):
    r = lambda x: None if pd.isna(x) else round(float(x), 4)
    return {"d": [x.strftime("%Y-%m-%d") for x in dates],
            "o": [r(x) for x in w["o"]], "h": [r(x) for x in w["h"]], "l": [r(x) for x in w["l"]],
            "c": [r(x) for x in w["c"]], "v": [None if pd.isna(x) else int(x) for x in w["v"]]}


def yahoo_batch(syms, period):
    got = {}
    for i in range(0, len(syms), 60):
        chunk = syms[i:i + 60]
        for attempt in range(3):
            try:
                df = yf.download(chunk, period=period, interval="1d", auto_adjust=True, group_by="ticker",
                                 threads=True, progress=False)
                break
            except Exception as e:
                log("batch error", e, "retrying")
                time.sleep(10 * (attempt + 1))
                df = None
        if df is None or df.empty:
            continue
        for s in chunk:
            try:
                sub = df[s] if isinstance(df.columns, pd.MultiIndex) else df
                sub = to_bars(sub)
                if len(sub) > 30:
                    got[s] = sub
            except Exception:
                pass
        time.sleep(2)
    return got


def stooq(sym):
    url = f"https://stooq.com/q/d/l/?s={sym.lower()}.us&i=d"
    t = requests.get(url, headers=UA, timeout=20).text
    if not t.startswith("Date"):
        return None
    df = pd.read_csv(io.StringIO(t), parse_dates=["Date"], index_col="Date")
    return to_bars(df.loc[df.index >= df.index.max() - pd.Timedelta(days=900)])


def main():
    os.makedirs(OUT, exist_ok=True)
    uni = build_universe()
    syms = sorted(uni)
    bench = [s for s in syms if uni[s].get("theme") == "Benchmark"]
    stocks = [s for s in syms if s not in bench]
    log("universe:", len(syms), "symbols")
    data = yahoo_batch(stocks, "2y")
    data.update(yahoo_batch(bench, "4y"))
    missing = [s for s in syms if s not in data]
    if missing:
        log("retrying", len(missing), "individually")
        for s in missing:
            try:
                df = to_bars(yf.Ticker(s).history(period="4y" if s in bench else "2y", auto_adjust=True))
                if len(df) > 30:
                    data[s] = df
                    continue
            except Exception:
                pass
            if not s.endswith(".AX") and not s.startswith("^"):
                try:
                    df = stooq(s)
                    if df is not None and len(df) > 30:
                        data[s] = df
                        log("stooq fallback ok", s)
                except Exception:
                    pass
            time.sleep(0.5)
    failed = [s for s in syms if s not in data]
    wk = {s: weekly(df, WEEKS_BENCH if s in bench else WEEKS_STOCK) for s, df in data.items()}
    dy = {s: daily(data[s], DAYS_BENCH) for s in DAILY_SYMS if s in data}
    last = wk["QQQ"]["d"][0] if "QQQ" in wk else max((v["d"][0] for v in wk.values()), default=None)
    json.dump(wk, open(f"{OUT}/weekly.json", "w"), separators=(",", ":"))
    json.dump(dy, open(f"{OUT}/daily.json", "w"), separators=(",", ":"))
    ulist = [dict(uni[s], has_data=s in data) for s in syms]
    json.dump(ulist, open(f"{OUT}/universe.json", "w"), separators=(",", ":"))
    meta = {"generated_utc": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "last_bar": last,
            "n_universe": len(syms), "n_ok": len(data), "failed": failed, "source": "Yahoo Finance via yfinance; Stooq fallback"}
    json.dump(meta, open(f"{OUT}/meta.json", "w"), indent=1)
    log(json.dumps({k: v for k, v in meta.items() if k != "failed"}), "failed:", len(failed))
    if len(data) < 0.6 * len(syms) or "QQQ" not in data:
        log("too few symbols fetched; not publishing")
        sys.exit(1)


if __name__ == "__main__":
    main()
