#!/usr/bin/env python3
"""Narrative-momentum scanner + market regime engine (v2: GitHub/Yahoo data).

Prices come from the market-data repo's `data` branch, refreshed daily by a
GitHub Actions workflow that pulls Yahoo Finance (no API key). Small persistent
state (signal history, regime log, manual extras) is kept in state.json and is
shipped inside the dashboard's history.json between runs. Stdlib only.

Commands
  load <file>                restore state.json from a history.json (raw or tool-result wrapper)
  fetch                      download weekly/daily/universe/meta from GitHub into ./ghdata
  run                        compute every output into out/ (needs ./ghdata)
  add <SYM> <theme> <name>   track an extra US ticker that is not in the GitHub data (priced via Alpha Vantage)
  drop <SYM>                 stop tracking an extra ticker
  plan [budget]              extras that need an Alpha Vantage refresh today (default 8)
  ingest <file> <SYM>        parse an Alpha Vantage weekly-adjusted result for an extra ticker
  narr <file>                validate a narratives JSON (list of rows) -> out/narratives.json
"""
import json, math, os, sys, datetime as dt, re, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "state.json")
OUT = os.path.join(HERE, "out")
GHDATA = os.path.join(HERE, "ghdata")
DATA_URL = "https://raw.githubusercontent.com/amila11033-alt/market-data/data/"
KEEP_BENCH, KEEP_STOCK = 170, 80
MAX_SPARK_EXTRA = 60

REGIME_GUIDE = {
    "Confirmed uptrend": ("80–100%", "Trend-following conditions. Add to leaders on pullbacks and new breakouts."),
    "Uptrend under pressure": ("50–70%", "Trend intact but stressed. Hold leaders, tighten stops, be selective with new buys."),
    "Correction": ("20–40%", "Index below its 40-week MA. Avoid new positions except the strongest relative-strength names; wait for a follow-through day."),
    "Rally attempt": ("25–50%", "Follow-through day after a low. Pilot positions only in names that held up best."),
    "Early bear warning": ("10–30%", "Long-term trend turning. Protect capital; raise cash; watch for leadership that ignores the selling."),
    "Bear-market rally": ("10–25%", "Below a falling 40-week MA but bouncing. Rallies in bear markets are sharp and unreliable."),
    "Bear market": ("0–10%", "Falling 40-week MA, deep drawdown. Cash or hedges; build a watchlist of names basing for the next cycle."),
}


# ---------------------------------------------------------------- helpers
def today():
    """Run date in Melbourne time (the user's day), falling back to UTC+11."""
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("Australia/Melbourne")).date().isoformat()
    except Exception:
        return (dt.datetime.utcnow() + dt.timedelta(hours=11)).date().isoformat()


PERSIST = ["version", "created", "extras", "extra_series", "fetched", "regime_log", "prev_signals",
           "signal_since", "signal_before"]


def load_state():
    if not os.path.exists(STATE):
        return {"version": 2, "created": today(), "extras": {}, "extra_series": {}, "fetched": {},
                "regime_log": [], "prev_signals": {}, "signal_since": {}, "signal_before": {}}
    with open(STATE) as f:
        s = json.load(f)
    for k, v in [("extras", {}), ("extra_series", {}), ("fetched", {}), ("regime_log", []), ("prev_signals", {}),
                 ("signal_since", {}), ("signal_before", {})]:
        s.setdefault(k, v)
    return s


def save_state(s):
    with open(STATE, "w") as f:
        json.dump({k: s[k] for k in PERSIST if k in s}, f, separators=(",", ":"))


def read_payload(path):
    """Return the text inside a raw file or a saved tool-result wrapper."""
    raw = open(path, encoding="utf-8").read()
    s = raw.lstrip()
    if s.startswith("[") and '"type"' in s[:200]:
        try:
            raw = "".join(b.get("text", "") for b in json.loads(s) if isinstance(b, dict))
        except Exception:
            pass
    return raw


def parse_av(text):
    """Parse an Alpha Vantage CSV or JSON time series into rows, newest first."""
    t = text.strip()
    if t.startswith("{"):
        obj = json.loads(t)
        if "error" in obj or "Information" in obj or "Note" in obj or not obj:
            raise ValueError("Alpha Vantage error: " + json.dumps(obj)[:300])
        key = next((k for k in obj if "Time Series" in k), None)
        if key is None:
            raise ValueError("no time series in JSON")
        rows = []
        for d, v in obj[key].items():
            r = {"timestamp": d}
            for k2, val in v.items():
                r[re.sub(r"^\d+\.\s*", "", k2)] = val
            rows.append(r)
        rows.sort(key=lambda r: r["timestamp"], reverse=True)
        return rows
    lines = [l for l in t.replace("\r\n", "\n").split("\n") if l.strip()]
    if not lines or not lines[0].startswith("timestamp"):
        raise ValueError("not an Alpha Vantage CSV: " + t[:200])
    head = [h.strip() for h in lines[0].split(",")]
    rows = [dict(zip(head, l.split(","))) for l in lines[1:] if len(l.split(",")) == len(head)]
    rows.sort(key=lambda r: r["timestamp"], reverse=True)
    return rows


def fnum(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def mean(a):
    a = [x for x in a if x is not None]
    return sum(a) / len(a) if a else None


def rnd(x, n=4):
    return None if x is None else round(x, n)


# ---------------------------------------------------------------- commands
def cmd_load(path):
    text = read_payload(path).strip()
    i = text.find('{"rows"')
    obj, _ = json.JSONDecoder().raw_decode(text[i if i >= 0 else 0:])
    st = obj.get("state")
    if not st:
        raise SystemExit("history file has no state")
    s = load_state() if False else st
    # v1 state carried full series for every ticker: keep only what v2 persists
    s.setdefault("extras", {}); s.setdefault("extra_series", {})
    save_state(s)
    print("restored state; last run", (s.get("regime_log") or [{}])[-1].get("date"), "| extras:", " ".join(s["extras"]) or "none")


def cmd_fetch():
    os.makedirs(GHDATA, exist_ok=True)
    for name in ["meta.json", "universe.json", "weekly.json", "daily.json"]:
        url = DATA_URL + name + "?t=" + str(int(dt.datetime.now(dt.timezone.utc).timestamp()))
        req = urllib.request.Request(url, headers={"User-Agent": "narrative-scanner", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
        open(os.path.join(GHDATA, name), "wb").write(data)
    meta = json.load(open(os.path.join(GHDATA, "meta.json")))
    print("GitHub data generated", meta.get("generated_utc"), "| last bar", meta.get("last_bar"),
          "| symbols", meta.get("n_ok"), "of", meta.get("n_universe"), "| failed", len(meta.get("failed", [])))


def cmd_plan(budget=8):
    s = load_state()
    syms = sorted(s["extras"], key=lambda k: s["fetched"].get(k, "0000"))
    print(" ".join(syms[: int(budget)]) or "(no extras to refresh)")


def cmd_ingest(path, sym, kind="weekly"):
    s = load_state()
    rows = parse_av(read_payload(path))[:KEEP_STOCK]
    ser = {k: [] for k in "dchlv"}
    for r in rows:
        c = fnum(r.get("close"))
        ac = fnum(r.get("adjusted close")) or c
        f = (ac / c) if c else 1.0
        ser["d"].append(r["timestamp"]); ser["c"].append(rnd(ac, 4))
        ser["h"].append(rnd((fnum(r.get("high")) or c) * f, 4)); ser["l"].append(rnd((fnum(r.get("low")) or c) * f, 4))
        ser["v"].append(fnum(r.get("volume")))
    s["extra_series"][sym.upper()] = ser
    s["fetched"][sym.upper()] = today()
    save_state(s)
    print(sym, "weekly", len(rows), "rows, latest", rows[0]["timestamp"])


def cmd_add(sym, theme, name):
    s = load_state()
    s["extras"][sym.upper()] = {"theme": theme, "name": name, "added": today()}
    save_state(s)
    print("added", sym.upper(), "to", theme, "(check it is not already in ghdata/universe.json; fetch it via Alpha Vantage)")


def cmd_drop(sym):
    s = load_state()
    for k in ("extras", "extra_series", "fetched"):
        s[k].pop(sym.upper(), None)
    save_state(s)
    print("dropped", sym.upper())


def load_market(s):
    """Merge GitHub data (and Alpha Vantage extras) into an in-memory working state."""
    gh = lambda n: json.load(open(os.path.join(GHDATA, n)))
    meta, uni, wk, dy = gh("meta.json"), gh("universe.json"), gh("weekly.json"), gh("daily.json")
    s["meta"] = meta
    s["universe"], s["series"] = {}, {}
    for u in uni:
        sym = u["symbol"]
        if sym not in wk:
            continue
        keep = KEEP_BENCH if u.get("theme") == "Benchmark" else KEEP_STOCK
        w = wk[sym]
        s["series"][sym] = {k: w[k][:keep] for k in "dchlv"}
        s["universe"][sym] = {"theme": u.get("theme") or "", "name": u.get("name", sym), "curated": bool(u.get("curated")),
                              "market": u.get("market", "US"), "sector": u.get("sector") or "", "index": u.get("index") or ""}
    for sym, info in s["extras"].items():
        if sym in s["series"]:
            s["universe"][sym]["theme"] = info["theme"]; s["universe"][sym]["curated"] = True
        elif sym in s["extra_series"]:
            s["series"][sym] = s["extra_series"][sym]
            s["universe"][sym] = {"theme": info["theme"], "name": info["name"], "curated": True, "market": "US", "sector": "", "index": "Extra (Alpha Vantage)"}
    s["daily"] = dy
    tnx = dy.get("^TNX")
    if tnx:
        v = [x / 10 if x and x > 20 else x for x in tnx["c"]]
        s["us10y"] = {"d": tnx["d"], "v": v}
    else:
        s["us10y"] = {"d": [], "v": []}
    return s


# ---------------------------------------------------------------- analytics
def chron(ser):
    """oldest-first arrays"""
    return {k: list(reversed(v)) for k, v in ser.items()}


def sma(a, k, off=0):
    n = len(a)
    if n < k + off:
        return None
    return sum(a[n - k - off: n - off]) / k


def rsi(c, k=14):
    if len(c) < k + 2:
        return None
    gains, losses = [], []
    for i in range(1, len(c)):
        ch = c[i] - c[i - 1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    ag = sum(gains[:k]) / k; al = sum(losses[:k]) / k
    for i in range(k, len(gains)):
        ag = (ag * (k - 1) + gains[i]) / k
        al = (al * (k - 1) + losses[i]) / k
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def ret(c, k):
    return c[-1] / c[-1 - k] - 1 if len(c) > k and c[-1 - k] else None


def stage_at(c, i):
    """Weinstein-style stage using the 30-week MA at index i (oldest-first)."""
    if i < 33:
        return None
    ma = sum(c[i - 29: i + 1]) / 30
    ma4 = sum(c[i - 33: i - 3]) / 30
    sl = ma / ma4 - 1
    px = c[i]
    if px > ma and sl > 0.005:
        return 2
    if px < ma and sl < -0.005:
        return 4
    if px < ma:
        return 3
    return 1


def partial_week_factor(last_date):
    d = dt.date.fromisoformat(last_date)
    wd = d.weekday()  # Mon=0
    return 5.0 / (wd + 1) if wd < 4 else 1.0


def bench_ret_at(bench, date, k):
    """return of a benchmark over k weeks ending at the bar dated <= date"""
    if not bench:
        return None
    d = bench["d"]
    idx = None
    for i in range(len(d) - 1, -1, -1):
        if d[i] <= date:
            idx = i
            break
    if idx is None or idx - k < 0:
        return None
    return bench["c"][idx] / bench["c"][idx - k] - 1


def stock_metrics(sym, ser, qqq):
    w = chron(ser)
    c, h, l, v, d = w["c"], w["h"], w["l"], w["v"], w["d"]
    n = len(c)
    if n < 20:
        return None
    m = {"as_of": d[-1], "price": c[-1], "weeks": n}
    for k, lab in [(1, "r1w"), (4, "r4w"), (13, "r13w"), (26, "r26w"), (39, "r39w"), (52, "r52w")]:
        m[lab] = ret(c, k)
    ma10, ma30, ma40 = sma(c, 10), sma(c, 30), sma(c, 40)
    ma30_4, ma40_8 = sma(c, 30, 4), sma(c, 40, 8)
    m["ma30_slope"] = (ma30 / ma30_4 - 1) if ma30 and ma30_4 else None
    m["ma40_slope"] = (ma40 / ma40_8 - 1) if ma40 and ma40_8 else None
    m["d10"] = c[-1] / ma10 - 1 if ma10 else None
    m["d30"] = c[-1] / ma30 - 1 if ma30 else None
    m["d40"] = c[-1] / ma40 - 1 if ma40 else None
    hi52 = max(h[-52:]); lo52 = min(l[-52:])
    m["off_high"] = c[-1] / hi52 - 1
    m["above_low"] = c[-1] / lo52 - 1
    m["rsi14w"] = rsi(c[-60:])
    pf = partial_week_factor(d[-1])
    vv = v[:-1] + [v[-1] * pf] if v[-1] is not None else v
    m["vol_ratio"] = (mean(vv[-4:]) / mean(vv[-26:])) if len(vv) >= 26 and mean(vv[-26:]) else None
    up = sum(vv[i] for i in range(n - 10, n) if i > 0 and c[i] > c[i - 1] and vv[i])
    dn = sum(vv[i] for i in range(n - 10, n) if i > 0 and c[i] < c[i - 1] and vv[i])
    m["ud_vol"] = (up / dn) if dn else (3.0 if up else None)
    prior_hi = max(h[-27:-1]) if n > 27 else max(h[:-1])
    m["breakout"] = bool(c[-1] >= prior_hi * 0.98)
    m["new_high"] = bool(c[-1] >= hi52 * 0.97)
    if n >= 60:
        base_low = min(l[-52:-8]); prior_peak = max(h[: n - 26])
        m["from_base"] = bool(base_low / prior_peak - 1 <= -0.25)
    else:
        m["from_base"] = False
    st = stage_at(c, n - 1)
    m["stage"] = st
    k = 0
    for i in range(n - 1, 32, -1):
        if stage_at(c, i) == 2:
            k += 1
        else:
            break
    m["weeks_stage2"] = k
    # relative strength vs QQQ at the same date
    for kk, lab in [(4, "rs4"), (13, "rs13"), (26, "rs26")]:
        br = bench_ret_at(qqq, d[-1], kk)
        m[lab] = (m["r%dw" % kk] - br) if (m.get("r%dw" % kk) is not None and br is not None) else None
    parts = [(m["r13w"], .4), (m["r26w"], .2), (m["r39w"], .2), (m["r52w"], .2)]
    tw = sum(wt for x, wt in parts if x is not None)
    m["rs_composite"] = sum(x * wt for x, wt in parts if x is not None) / tw if tw else None
    m["spark"] = [round(x, 2) for x in c[-52:]]
    m["spark_d"] = d[-52:]
    return m


def score(m):
    e = 0.0
    st = m["stage"]; w2 = m["weeks_stage2"]
    if st == 2:
        e += 20
        e += 10 if 1 <= w2 <= 12 else (5 if w2 <= 26 else 0)
    if st == 1 and (m["d30"] or 0) > 0:
        e += 8
    rs13 = m.get("rs13")
    if rs13 is not None and rs13 > 0:
        e += min(15, rs13 * 100 / 2)
    if (m.get("rs4") or 0) > 0 and (m.get("r4w") or 0) > 0:
        e += 5
    pct = m.get("rs_pct") or 0
    e += 10 if pct >= 80 else (5 if pct >= 60 else 0)
    if m["breakout"]:
        e += 10
    if m["new_high"]:
        e += 5
    if (m.get("vol_ratio") or 0) > 1.25:
        e += 5
    if (m.get("ud_vol") or 0) > 1.3:
        e += 5
    d40 = m.get("d40")
    if d40 is not None:
        e += 10 if 0 < d40 <= .35 else (3 if 0 < d40 <= .6 else (-5 if d40 > .6 else -10))
    if m["from_base"]:
        e += 5
    m["early_score"] = int(max(0, min(100, round(e))))
    # holder's exit (slow, for 1-2 year positions): weekly close >3% below a falling 40-week MA
    m["trend_exit"] = bool((d40 or 0) < -0.03 and (m.get("ma40_slope") or 0) < 0)
    cr = 0.0
    if d40 is not None:
        cr += (25 if d40 > .5 else 0) + (15 if d40 > .8 else 0) + (10 if d40 > 1.2 else 0)
    r52 = m.get("r52w")
    if r52 is not None:
        cr += (15 if r52 > 1 else 0) + (10 if r52 > 2 else 0)
    rs_ = m.get("rsi14w") or 0
    cr += (10 if rs_ > 70 else 0) + (10 if rs_ > 80 else 0)
    if (m.get("r13w") or 0) > .5:
        cr += 10
    if (m.get("vol_ratio") or 0) > 2 and (m.get("r4w") or 0) > .15:
        cr += 10
    m["crowd_score"] = int(max(0, min(100, round(cr))))
    # signal
    strong_before = (r52 or 0) > .3 or m["off_high"] > -.5
    if st == 4 or ((m["d30"] or 0) < 0 and m["off_high"] < -.25):
        sig = "Breaking down" if strong_before and m["off_high"] > -.6 else "Downtrend"
    elif st == 3:
        sig = "Topping"
    elif m["crowd_score"] >= 60 and st == 2:
        sig = "Extended / crowded"
    elif m["early_score"] >= 65:
        sig = "Early trend"
    elif m["early_score"] >= 45:
        sig = "Emerging"
    elif st == 2:
        sig = "Established trend"
    elif st == 1:
        sig = "Basing"
    else:
        sig = "Neutral"
    m["signal"] = sig


def theme_stage(t):
    if t["n_data"] == 0:
        return "Awaiting data"
    share_early = t.get("n_early", 0) / t["n_data"]
    if (t["pct_stage2"] >= .4 or share_early >= .34) and (t["avg_early"] or 0) >= 45 and (t["avg_d40"] or 0) < .35 and (t["avg_rs13"] or 0) > 0:
        return "Early (ignition)"
    if t["pct_breaking"] >= .5 or ((t["avg_rs13"] or 0) < -.10 and t["pct_stage2"] < .3):
        return "Unwinding"
    if (t["avg_crowd"] or 0) >= 50 or (t["avg_d40"] or 0) > .5:
        return "Crowded / late"
    if t["pct_stage2"] >= .4 and (t["avg_early"] or 0) >= 50 and (t["avg_d40"] or 0) < .35:
        return "Early (ignition)"
    if t["pct_stage2"] >= .6 and (t["avg_rs13"] or 0) > .05:
        return "Accelerating"
    if t["pct_stage2"] >= .4:
        return "Maturing"
    return "Dormant / basing"


def classify(c, ma10, ma40, slope40, dd, dist=0, ftd=False):
    above40 = c > ma40; above10 = c > ma10
    rising40 = slope40 > 0.005; falling40 = slope40 < -0.005
    if above40 and above10 and rising40 and dd > -0.08 and dist <= 4:
        return "Confirmed uptrend"
    if above40:
        return "Uptrend under pressure"
    if dd <= -0.20 or (falling40 and ma10 < ma40):
        return "Bear-market rally" if above10 else "Bear market"
    if ftd and above10:
        return "Rally attempt"
    if ma10 < ma40 or not rising40:
        return "Early bear warning"
    return "Correction"


REGIME_SCORE = {"Confirmed uptrend": 3, "Uptrend under pressure": 2, "Rally attempt": 1, "Correction": 0,
                "Early bear warning": -1, "Bear-market rally": -2, "Bear market": -3}


def daily_signals(dd_):
    """distribution days, follow-through day, realised vol, 21EMA, 50DMA from daily rows (newest first)."""
    if not dd_ or len(dd_.get("c", [])) < 26:
        return {}
    d = list(reversed(dd_["d"])); c = list(reversed(dd_["c"])); v = list(reversed(dd_["v"]))
    n = len(c)
    dist = 0; dist_dates = []
    for i in range(max(1, n - 25), n):
        if c[i] <= c[i - 1] * 0.998 and v[i] > v[i - 1]:
            dist += 1; dist_dates.append(d[i])
    rets = [math.log(c[i] / c[i - 1]) for i in range(n - 20, n)]
    mu = sum(rets) / len(rets)
    vol = math.sqrt(sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)) * math.sqrt(252)
    ema = c[0]; a = 2 / 22
    for x in c[1:]:
        ema = a * x + (1 - a) * ema
    lo_i = min(range(n - 20, n), key=lambda i: c[i])
    ftd = None
    for i in range(lo_i + 4, n):
        if c[i] / c[i - 1] - 1 >= 0.0125 and v[i] > v[i - 1] and min(c[lo_i:i + 1]) >= c[lo_i]:
            ftd = d[i]; break
    sma50 = sum(c[-50:]) / 50 if n >= 50 else None
    return {"dist_days": dist, "dist_dates": dist_dates, "vol20": vol, "ema21": ema, "sma50": sma50,
            "ftd": ftd, "last_daily": d[-1], "daily_close": c[-1], "day_chg": c[-1] / c[-2] - 1,
            "low_date": d[lo_i]}



def ser_rel(a, b, k):
    """k-week return of a minus b (weekly series dicts, newest first)"""
    if not a or not b:
        return None
    ca, cb = chron(a)["c"], chron(b)["c"]
    ra, rb = ret(ca, k), ret(cb, k)
    return (ra - rb) if ra is not None and rb is not None else None


def cmd_run():
    s = load_market(load_state())
    os.makedirs(OUT, exist_ok=True)
    run_date = today()
    U, S = s["universe"], s["series"]
    if "QQQ" not in S:
        raise SystemExit("QQQ weekly series missing from GitHub data")
    q = chron(S["QQQ"]); qc = q["c"]
    axjo = chron(S["^AXJO"]) if "^AXJO" in S else None
    spyw = chron(S["SPY"]) if "SPY" in S else None
    # ---------------- per-stock metrics (every non-benchmark name)
    rows = []
    for sym, info in U.items():
        if info["theme"] == "Benchmark":
            continue
        # theme names vs QQQ (the tech tape); other US index names vs SPY (the broad market); ASX vs ASX 200
        bench = axjo if (info["market"] == "ASX" and axjo) else (q if info["curated"] or not spyw else spyw)
        m = stock_metrics(sym, S[sym], bench)
        r = {"sym": sym, "name": info["name"], "theme": info["theme"] or info["sector"] or "Other",
             "curated": info["curated"], "market": info["market"], "sector": info["sector"], "index": info["index"]}
        if m:
            r.update(m)
        rows.append(r)
    for mk in ("US", "ASX"):
        grp = sorted([r for r in rows if r["market"] == mk and r.get("rs_composite") is not None], key=lambda r: r["rs_composite"])
        for i, r in enumerate(grp):
            r["rs_pct"] = int(round(1 + 98 * i / max(1, len(grp) - 1)))
    for r in rows:
        if r.get("price") is not None:
            score(r)
        else:
            r.update({"signal": "Awaiting data", "early_score": None, "crowd_score": None, "trend_exit": None})
        r["age_days"] = (dt.date.fromisoformat(run_date) - dt.date.fromisoformat(r["as_of"])).days if r.get("as_of") else None
    # ---------------- signal changes
    prev, since, before = s["prev_signals"], s["signal_since"], s["signal_before"]
    changes = []
    for r in rows:
        if r["signal"] == "Awaiting data":
            continue
        p = prev.get(r["sym"])
        if p and p != r["signal"]:
            changes.append({"sym": r["sym"], "theme": r["theme"], "from": p, "to": r["signal"], "curated": r["curated"]})
            since[r["sym"]] = run_date; before[r["sym"]] = p
        elif r["sym"] not in since:
            since[r["sym"]] = run_date
        r["signal_since"] = since.get(r["sym"]); r["prev_signal"] = before.get(r["sym"])
    s["prev_signals"] = {r["sym"]: r["signal"] for r in rows if r["signal"] != "Awaiting data"}
    # ---------------- themes (curated only)
    themes = []
    for th in sorted(set(r["theme"] for r in rows if r["curated"])):
        mem = [r for r in rows if r["curated"] and r["theme"] == th]
        dat = [r for r in mem if r.get("price") is not None]
        t = {"theme": th, "market": mem[0]["market"], "n": len(mem), "n_data": len(dat)}
        t["pct_stage2"] = sum(1 for r in dat if r["stage"] == 2) / len(dat) if dat else 0
        t["pct_above40"] = sum(1 for r in dat if (r.get("d40") or -1) > 0) / len(dat) if dat else 0
        t["pct_breaking"] = sum(1 for r in dat if r["signal"] in ("Breaking down", "Downtrend")) / len(dat) if dat else 0
        for k, src in [("avg_r13", "r13w"), ("avg_rs13", "rs13"), ("avg_early", "early_score"), ("avg_crowd", "crowd_score"),
                       ("avg_d40", "d40"), ("avg_off_high", "off_high")]:
            t[k] = mean([r.get(src) for r in dat])
        t["n_early"] = sum(1 for r in dat if r["signal"] in ("Early trend", "Emerging"))
        t["n_crowded"] = sum(1 for r in dat if r["signal"] == "Extended / crowded")
        t["stage"] = theme_stage(t)
        t["score"] = round((t["avg_early"] or 0) * .5 + t["pct_stage2"] * 30 + max(-20, min(20, (t["avg_rs13"] or 0) * 100)) - t["pct_breaking"] * 25, 1) if dat else None
        best = sorted(dat, key=lambda r: -(r.get("early_score") or 0))[:3]
        t["leaders"] = ", ".join(r["sym"] for r in best)
        t["members"] = ", ".join(r["sym"] for r in mem)
        t["as_of"] = max((r["as_of"] for r in dat), default=None)
        themes.append(t)
    themes.sort(key=lambda t: -(t["score"] if t["score"] is not None else -999))
    # ---------------- regime
    dq = s["daily"].get("QQQ")
    dsig = daily_signals(dq)
    ma10, ma40 = sma(qc, 10), sma(qc, 40)
    slope40 = ma40 / sma(qc, 40, 8) - 1
    hi52 = max(qc[-52:]); dd = qc[-1] / hi52 - 1
    spy = chron(S["SPY"]) if "SPY" in S else None
    ftd_recent = bool(dsig.get("ftd"))
    label = classify(qc[-1], ma10, ma40, slope40, dd, dsig.get("dist_days", 0), ftd_recent)

    def breadth(pred):
        g = [r for r in rows if pred(r) and r.get("d40") is not None and r["age_days"] is not None and r["age_days"] <= 10]
        return (sum(1 for r in g if r["d40"] > 0) / len(g), len(g)) if g else (None, 0)
    b_tech, n_tech = breadth(lambda r: r["curated"] and r["market"] == "US")
    b_sp, n_sp = breadth(lambda r: "S&P 500" in (r["index"] or ""))
    b_ndx, n_ndx = breadth(lambda r: "Nasdaq-100" in (r["index"] or ""))
    b_asx, n_asx = breadth(lambda r: "ASX 200" in (r["index"] or ""))
    smh_rel = ser_rel(S.get("SMH"), S.get("QQQ"), 13)
    rsp_rel = ser_rel(S.get("RSP"), S.get("SPY"), 13)
    credit = ser_rel(S.get("HYG"), S.get("IEF"), 13)
    y = s["us10y"]
    y10 = y["v"][0] if y.get("v") else None
    y10_chg = (y["v"][0] - y["v"][20]) if y.get("v") and len(y["v"]) > 20 else None
    vix_d = s["daily"].get("^VIX")
    vix = vix_d["c"][0] if vix_d else None
    vix_avg = mean(vix_d["c"][:50]) if vix_d else None
    spy_d40 = spy["c"][-1] / sma(spy["c"], 40) - 1 if spy else None
    spy_dd = spy["c"][-1] / max(spy["c"][-52:]) - 1 if spy else None
    axjo_d40 = axjo["c"][-1] / sma(axjo["c"], 40) - 1 if axjo else None
    axjo_dd = axjo["c"][-1] / max(axjo["c"][-52:]) - 1 if axjo else None
    expo, advice = REGIME_GUIDE[label]
    cur = [r for r in rows if r["curated"]]
    disc = [r for r in rows if not r["curated"]]
    reg = {"as_of": q["d"][-1], "run_date": run_date, "regime": label, "regime_score": REGIME_SCORE[label],
           "exposure": expo, "advice": advice,
           "qqq_close": qc[-1], "qqq_ma10w": ma10, "qqq_ma40w": ma40, "qqq_d40": qc[-1] / ma40 - 1,
           "qqq_d10": qc[-1] / ma10 - 1, "qqq_ma40_slope": slope40, "qqq_dd": dd, "qqq_high52": hi52,
           "qqq_r1w": ret(qc, 1), "qqq_r13w": ret(qc, 13), "spy_d40": spy_d40, "spy_dd": spy_dd,
           "dist_days": dsig.get("dist_days"), "ftd_date": dsig.get("ftd"), "vol20": dsig.get("vol20"),
           "qqq_day_chg": dsig.get("day_chg"), "last_daily": dsig.get("last_daily"),
           "qqq_vs_50d": (dsig["daily_close"] / dsig["sma50"] - 1) if dsig.get("sma50") else None,
           "us10y": y10, "us10y_chg20": y10_chg, "us10y_date": y["d"][0] if y.get("d") else None,
           "vix": vix, "vix_avg50": vix_avg,
           "breadth_above40": b_tech, "breadth_n": n_tech, "sp500_breadth40": b_sp, "sp500_n": n_sp,
           "ndx_breadth40": b_ndx, "asx200_breadth40": b_asx,
           "smh_rel13": smh_rel, "rsp_rel13": rsp_rel, "credit_rel13": credit,
           "asx200_d40": axjo_d40, "asx200_dd": axjo_dd,
           "n_universe": len(cur), "n_with_data": sum(1 for r in cur if r.get("price") is not None),
           "n_scanned": sum(1 for r in rows if r.get("price") is not None),
           "data_generated": s["meta"].get("generated_utc"), "data_failed": len(s["meta"].get("failed", [])),
           "n_early": sum(1 for r in cur if r["signal"] == "Early trend"),
           "n_emerging": sum(1 for r in cur if r["signal"] == "Emerging"),
           "n_crowded": sum(1 for r in cur if r["signal"] == "Extended / crowded"),
           "n_breaking": sum(1 for r in cur if r["signal"] in ("Breaking down", "Downtrend")),
           "n_disc_early": sum(1 for r in disc if r["signal"] in ("Early trend", "Emerging"))}

    def st(ok, warn):
        return "ok" if ok else ("warn" if warn else "bad")
    nz = lambda v: v if v is not None else 0
    checks = [
        {"check": "Nasdaq-100 vs 40-week MA (≈200-day)", "value": reg["qqq_d40"], "fmt": "pct", "status": st(reg["qqq_d40"] > 0, reg["qqq_d40"] > -.03), "rule": "Above = long-term uptrend"},
        {"check": "40-week MA slope (8 weeks)", "value": slope40, "fmt": "pct", "status": st(slope40 > .01, slope40 > -.005), "rule": "Rising >1% healthy; falling = bear risk"},
        {"check": "Nasdaq-100 vs 10-week MA (≈50-day)", "value": reg["qqq_d10"], "fmt": "pct", "status": st(reg["qqq_d10"] > 0, reg["qqq_d10"] > -.02), "rule": "Below = intermediate trend broken"},
        {"check": "Drawdown from 52-week high", "value": dd, "fmt": "pct", "status": st(dd > -.05, dd > -.10), "rule": ">10% correction, >20% bear"},
        {"check": "Distribution days (last 25 sessions)", "value": dsig.get("dist_days"), "fmt": "int", "status": st(nz(dsig.get("dist_days")) <= 3, nz(dsig.get("dist_days")) <= 5), "rule": "6+ = institutions selling"},
        {"check": "Follow-through day since recent low", "value": 1 if ftd_recent else 0, "fmt": "yesno", "status": "ok" if ftd_recent else ("warn" if dd > -.05 else "bad"), "rule": "Needed to confirm a new uptrend"},
        {"check": "VIX", "value": vix, "fmt": "num1", "status": st(nz(vix) < 20, nz(vix) < 30) if vix is not None else "warn", "rule": "Above 30 = fear; spikes often mark lows"},
        {"check": "S&P 500 vs 40-week MA", "value": spy_d40, "fmt": "pct", "status": st(nz(spy_d40) > 0, nz(spy_d40) > -.03), "rule": "Confirms or contradicts Nasdaq"},
        {"check": "S&P 500 breadth: % above 40-week MA", "value": b_sp, "fmt": "pct0", "status": st(nz(b_sp) > .6, nz(b_sp) > .4), "rule": "Below 40% = weak internals"},
        {"check": "Tech theme breadth: % above 40-week MA", "value": b_tech, "fmt": "pct0", "status": st(nz(b_tech) > .6, nz(b_tech) > .4), "rule": "The scanner's US theme names"},
        {"check": "Semis vs Nasdaq-100 (13 weeks)", "value": smh_rel, "fmt": "pct", "status": st(nz(smh_rel) > 0, nz(smh_rel) > -.05), "rule": "AI-hardware leadership check"},
        {"check": "Equal-weight vs cap-weight S&P (13 weeks)", "value": rsp_rel, "fmt": "pct", "status": "ok" if nz(rsp_rel) > -.03 else "warn", "rule": "Positive = rotation away from mega-caps"},
        {"check": "Credit: high yield vs Treasuries (13 weeks)", "value": credit, "fmt": "pct", "status": st(nz(credit) > -.01, nz(credit) > -.03), "rule": "Falling = credit stress building"},
        {"check": "US 10-year yield", "value": (y10 / 100) if y10 is not None else None, "fmt": "pct2", "status": st(nz(y10) < 4.5 and nz(y10_chg) < .25, nz(y10) < 5.0), "rule": "Above 5% pressures growth multiples"},
        {"check": "S&P/ASX 200 vs 40-week MA", "value": axjo_d40, "fmt": "pct", "status": st(nz(axjo_d40) > 0, nz(axjo_d40) > -.03), "rule": "Local market trend (for ASX names)"},
    ]
    for i, ch in enumerate(checks):
        ch["id"] = i + 1
    # ---------------- regime log + weekly regime history for chart
    log = [x for x in s.get("regime_log", []) if x["date"] != run_date]
    log.append({"date": run_date, "as_of": reg["as_of"], "regime": label, "score": REGIME_SCORE[label], "qqq": qc[-1]})
    s["regime_log"] = log[-400:]
    idx = []
    n = len(qc)
    for i in range(max(52, n - 104), n):
        cc = qc[: i + 1]
        m10, m40 = sma(cc, 10), sma(cc, 40)
        sl = m40 / sma(cc, 40, 8) - 1 if sma(cc, 40, 8) else 0
        d_ = cc[-1] / max(cc[-52:]) - 1
        lab = classify(cc[-1], m10, m40, sl, d_)
        idx.append({"date": q["d"][i], "qqq": cc[-1], "ma10": rnd(m10, 2), "ma40": rnd(m40, 2), "regime": lab,
                    "regime_score": REGIME_SCORE[lab], "dd": rnd(d_, 4)})
    # ---------------- outputs
    num_keys = ["price", "r1w", "r4w", "r13w", "r26w", "r52w", "rs4", "rs13", "rs26", "d10", "d30", "d40",
                "ma30_slope", "ma40_slope", "off_high", "above_low", "rsi14w", "vol_ratio", "ud_vol", "rs_composite"]
    order = {"Early trend": 0, "Emerging": 1, "Established trend": 2, "Basing": 3, "Extended / crowded": 4, "Topping": 5,
             "Neutral": 6, "Breaking down": 7, "Downtrend": 8, "Awaiting data": 9}
    scan = []
    for r in rows:
        o = {k: r.get(k) for k in ["sym", "name", "theme", "signal", "stage", "weeks_stage2", "early_score", "crowd_score",
                                   "rs_pct", "breakout", "new_high", "from_base", "trend_exit", "as_of", "age_days",
                                   "prev_signal", "signal_since", "curated", "market", "sector", "index"]}
        for k in num_keys:
            o[k] = rnd(r.get(k), 4)
        scan.append(o)
    scan.sort(key=lambda o: (0 if o["curated"] else 1, order.get(o["signal"], 9), -(o["early_score"] or 0)))
    disc_top = sorted([r for r in rows if not r["curated"] and r["signal"] in ("Early trend", "Emerging")],
                      key=lambda r: -(r["early_score"] or 0))[:MAX_SPARK_EXTRA]
    spark_syms = {r["sym"] for r in rows if r["curated"]} | {r["sym"] for r in disc_top}
    spark = [{"sym": r["sym"], "date": d_, "close": c_} for r in rows if r["sym"] in spark_syms
             for d_, c_ in zip(r.get("spark_d") or [], r.get("spark") or [])]
    for coll in (themes, checks):
        for t in coll:
            for k in list(t):
                if isinstance(t[k], float):
                    t[k] = rnd(t[k], 5)
    for k in list(reg):
        if isinstance(reg[k], float):
            reg[k] = rnd(reg[k], 5)
    w = lambda name, obj: json.dump(obj, open(os.path.join(OUT, name), "w"), separators=(",", ":"))
    w("regime.json", [reg]); w("checks.json", checks); w("index.json", idx); w("themes.json", themes); w("scan.json", scan)
    save_state(s)
    persisted = {k: s[k] for k in PERSIST if k in s}
    w("history.json", {"rows": spark, "state": persisted, "saved": run_date})
    # ---------------- brief for the researcher
    pc = lambda v: "n/a" if v is None else "%+.1f%%" % (v * 100)
    p0 = lambda v: "n/a" if v is None else "%.0f%%" % (v * 100)
    L = ["REGIME %s (score %+d) as of %s | exposure %s" % (label, REGIME_SCORE[label], reg["as_of"], expo),
         "QQQ %.2f vs40w %s vs10w %s dd %s 40w-slope %s | dist days %s | FTD %s | VIX %s | 10y %s (20d %s)" % (
             qc[-1], pc(reg["qqq_d40"]), pc(reg["qqq_d10"]), pc(dd), pc(slope40), dsig.get("dist_days"), dsig.get("ftd"),
             "n/a" if vix is None else "%.1f" % vix, y10, y10_chg),
         "Breadth above 40w: S&P 500 %s | Nasdaq-100 %s | tech themes %s | ASX 200 %s" % (p0(b_sp), p0(b_ndx), p0(b_tech), p0(b_asx)),
         "SMH vs QQQ 13w %s | RSP vs SPY 13w %s | HYG vs IEF 13w %s | ASX 200 vs 40w %s" % (pc(smh_rel), pc(rsp_rel), pc(credit), pc(axjo_d40)),
         "Data: GitHub/Yahoo generated %s, %d names scanned, %d failed" % (reg["data_generated"], reg["n_scanned"], reg["data_failed"]),
         "", "THEMES:"]
    for t in themes:
        L.append("  %-26s %-18s score %s stage2 %s rs13 %s leaders %s (%d/%d)" % (t["theme"], t["stage"], t["score"], p0(t["pct_stage2"]), pc(t["avg_rs13"]), t["leaders"], t["n_data"], t["n"]))
    L += ["", "THEME NAMES – EARLY / EMERGING:"]
    L += ["  %-8s %-24s %-12s early %s crowd %s r13 %s d40 %s" % (o["sym"], o["theme"], o["signal"], o["early_score"], o["crowd_score"], pc(o["r13w"]), pc(o["d40"])) for o in scan if o["curated"] and o["signal"] in ("Early trend", "Emerging")]
    L += ["", "DISCOVERY (index names outside the themes) – EARLY / EMERGING, top 25:"]
    L += ["  %-8s %-28s %-12s early %s r13 %s d40 %s %s" % (r["sym"], (r["sector"] or "")[:28], r["signal"], r["early_score"], pc(r.get("r13w")), pc(r.get("d40")), r["index"]) for r in disc_top[:25]]
    L += ["", "CROWDED:"] + ["  %s (%s)" % (o["sym"], o["theme"]) for o in scan if o["curated"] and o["signal"] == "Extended / crowded"]
    L += ["", "BREAKING DOWN (themes):"] + ["  %s (%s, off high %s)" % (o["sym"], o["theme"], pc(o["off_high"])) for o in scan if o["curated"] and o["signal"] in ("Breaking down", "Downtrend")]
    L += ["", "SIGNAL CHANGES (themes):"] + ["  %s %s -> %s" % (c_["sym"], c_["from"], c_["to"]) for c_ in changes if c_["curated"]]
    L += ["", "SIGNAL CHANGES (discovery, to early/emerging):"] + ["  %s %s -> %s" % (c_["sym"], c_["from"], c_["to"]) for c_ in changes if not c_["curated"] and c_["to"] in ("Early trend", "Emerging")]
    L += ["", "NO DATA: " + " ".join(o["sym"] for o in scan if o["curated"] and o["signal"] == "Awaiting data")]
    open(os.path.join(OUT, "brief.txt"), "w").write("\n".join(L))
    print("\n".join(L))


def cmd_narr(path):
    rows = json.load(open(path))
    assert isinstance(rows, list) and rows, "narratives must be a non-empty list"
    req = {"id", "kind", "title", "body"}
    for i, r in enumerate(rows):
        miss = req - set(r)
        assert not miss, "row %d missing %s" % (i, miss)
        r.setdefault("stage", ""); r.setdefault("tickers", ""); r.setdefault("sources", "")
        r.setdefault("updated", today())
        if isinstance(r["sources"], list):
            r["sources"] = " ".join(r["sources"])
    os.makedirs(OUT, exist_ok=True)
    json.dump(rows, open(os.path.join(OUT, "narratives.json"), "w"), separators=(",", ":"))
    print("narratives ok:", len(rows), "rows")


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        print(__doc__); sys.exit(0)
    {"load": lambda: cmd_load(a[1]), "fetch": lambda: cmd_fetch(), "run": lambda: cmd_run(),
     "plan": lambda: cmd_plan(*(a[1:2] or [8])), "ingest": lambda: cmd_ingest(a[1], a[2]),
     "add": lambda: cmd_add(a[1], a[2], a[3]), "drop": lambda: cmd_drop(a[1]), "narr": lambda: cmd_narr(a[1])}[a[0]]()
