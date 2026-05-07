#!/usr/bin/env python3
"""Build static breadth snapshot for stock.zhixingshe.cc.

Free-source design:
- S&P 500 constituents: datasets/s-and-p-500-companies raw CSV (GitHub)
- Nasdaq-100 proxy: Invesco QQQ official holdings API (tracks Nasdaq-100; includes cash/other rows filtered out)
- Prices: Yahoo Finance spark endpoint in batches (1y daily)

Output: data/snapshots/market-breadth-latest.json
"""
from __future__ import annotations

import csv
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "snapshots" / "market-breadth-latest.json"

SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
QQQ_HOLDINGS_URL = "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/holdings/fund?idType=ticker&interval=monthly&productType=ETF"
YAHOO_SPARK = "https://query1.finance.yahoo.com/v7/finance/spark"
UA = "stock-breadth-snapshot/1.0 (+https://stock.zhixingshe.cc)"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def yahoo_symbol(symbol: str) -> str:
    # Yahoo uses BRK-B / BF-B instead of BRK.B / BF.B.
    return symbol.strip().upper().replace(".", "-")


def load_sp500() -> list[dict]:
    rows = list(csv.DictReader(fetch_text(SP500_URL).splitlines()))
    out = []
    for row in rows:
        sym = (row.get("Symbol") or "").strip().upper()
        if sym:
            out.append({
                "symbol": sym,
                "yahooSymbol": yahoo_symbol(sym),
                "name": row.get("Security") or sym,
                "sector": row.get("GICS Sector") or None,
            })
    return out


def load_qqq() -> tuple[list[dict], str | None]:
    data = json.loads(fetch_text(QQQ_HOLDINGS_URL))
    out = []
    for row in data.get("holdings", []):
        sym = (row.get("ticker") or "").strip().upper()
        typ = row.get("securityTypeName") or ""
        if not sym or "Common" not in typ:
            continue
        out.append({
            "symbol": sym,
            "yahooSymbol": yahoo_symbol(sym),
            "name": row.get("issuerName") or sym,
            "weightPct": row.get("percentageOfTotalNetAssets"),
        })
    return out, data.get("effectiveDate") or data.get("effectiveBusinessDate")


def chunks(items: list[str], n: int):
    for i in range(0, len(items), n):
        yield items[i:i+n]


def fetch_spark_batch(symbols: list[str]) -> dict[str, dict]:
    qs = urllib.parse.urlencode({
        "symbols": ",".join(symbols),
        "range": "1y",
        "interval": "1d",
    })
    url = f"{YAHOO_SPARK}?{qs}"
    raw = fetch_text(url, timeout=45)
    data = json.loads(raw)
    out = {}
    for item in data.get("spark", {}).get("result", []):
        sym = item.get("symbol")
        resp = (item.get("response") or [{}])[0]
        if sym and resp:
            out[sym.upper()] = resp
    return out


def fetch_prices(symbols: list[str], batch_size: int = 15, workers: int = 4) -> dict[str, dict]:
    batches = list(chunks(sorted(set(symbols)), batch_size))
    merged: dict[str, dict] = {}
    errors = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_spark_batch, b): b for b in batches}
        for fut in as_completed(futs):
            b = futs[fut]
            try:
                merged.update(fut.result())
            except Exception as e:  # keep partial snapshot useful
                errors.append({"batch": b[:3] + (["..."] if len(b) > 3 else []), "error": str(e)})
            time.sleep(0.05)
    return {"data": merged, "errors": errors}


def sma(values: list[float], n: int) -> float | None:
    vals = [v for v in values[-n:] if isinstance(v, (int, float)) and math.isfinite(v)]
    if len(vals) < n * 0.8:
        return None
    return mean(vals)


def analyze_symbol(row: dict, spark: dict) -> dict | None:
    sym = row["symbol"]
    ysym = row["yahooSymbol"].upper()
    resp = spark.get(ysym)
    if not resp:
        return None
    ts = resp.get("timestamp") or []
    quote = (resp.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    pairs = [(t, c, highs[i] if i < len(highs) else c, lows[i] if i < len(lows) else c)
             for i, (t, c) in enumerate(zip(ts, closes))
             if isinstance(c, (int, float)) and math.isfinite(c)]
    if len(pairs) < 60:
        return None
    last_t, last, _, _ = pairs[-1]
    prev = pairs[-2][1] if len(pairs) >= 2 else None
    close_vals = [p[1] for p in pairs]
    high_vals = [p[2] for p in pairs if isinstance(p[2], (int, float)) and math.isfinite(p[2])]
    low_vals = [p[3] for p in pairs if isinstance(p[3], (int, float)) and math.isfinite(p[3])]
    ma20 = sma(close_vals, 20)
    ma50 = sma(close_vals, 50)
    ma200 = sma(close_vals, 200)
    high52 = max(high_vals) if high_vals else max(close_vals)
    low52 = min(low_vals) if low_vals else min(close_vals)
    return {
        "symbol": sym,
        "yahooSymbol": ysym,
        "name": row.get("name"),
        "sector": row.get("sector"),
        "weightPct": row.get("weightPct"),
        "close": round(last, 4),
        "changePct": round((last / prev - 1) * 100, 2) if prev else None,
        "above20": bool(ma20 is not None and last >= ma20),
        "above50": bool(ma50 is not None and last >= ma50),
        "above200": bool(ma200 is not None and last >= ma200),
        "nearHigh": bool(high52 and last >= high52 * 0.95),
        "newHigh": bool(high52 and last >= high52 * 0.999),
        "nearLow": bool(low52 and last <= low52 * 1.10),
        "newLow": bool(low52 and last <= low52 * 1.001),
        "advanced": bool(prev and last > prev),
        "declined": bool(prev and last < prev),
        "updatedAt": datetime.fromtimestamp(last_t, timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def pct(n: int, d: int) -> float:
    return round(n / d * 100, 1) if d else 0.0


def universe_status(stats: dict) -> str:
    if stats["coveragePct"] < 70:
        return "pending"
    if stats["above50Pct"] < 45 or stats["above200Pct"] < 50 or stats["newLowPct"] >= 10 or stats["declinePct"] >= 65:
        return "distribution"
    if stats["above50Pct"] >= 70 and stats["above200Pct"] >= 70 and stats["newHighPct"] >= 8 and stats["advancePct"] >= 55:
        return "accumulation"
    return "neutral"


def analyze_universe(name: str, source: str, rows: list[dict], spark: dict, source_updated: str | None = None) -> dict:
    analyzed = [a for r in rows if (a := analyze_symbol(r, spark))]
    d = len(analyzed)
    counts = {
        "above20": sum(x["above20"] for x in analyzed),
        "above50": sum(x["above50"] for x in analyzed),
        "above200": sum(x["above200"] for x in analyzed),
        "nearHigh": sum(x["nearHigh"] for x in analyzed),
        "newHigh": sum(x["newHigh"] for x in analyzed),
        "nearLow": sum(x["nearLow"] for x in analyzed),
        "newLow": sum(x["newLow"] for x in analyzed),
        "advanced": sum(x["advanced"] for x in analyzed),
        "declined": sum(x["declined"] for x in analyzed),
    }
    stats = {
        "name": name,
        "source": source,
        "sourceUpdatedAt": source_updated,
        "requested": len(rows),
        "sampleSize": d,
        "coveragePct": pct(d, len(rows)),
        "above20Pct": pct(counts["above20"], d),
        "above50Pct": pct(counts["above50"], d),
        "above200Pct": pct(counts["above200"], d),
        "nearHighPct": pct(counts["nearHigh"], d),
        "newHighPct": pct(counts["newHigh"], d),
        "nearLowPct": pct(counts["nearLow"], d),
        "newLowPct": pct(counts["newLow"], d),
        "advancePct": pct(counts["advanced"], d),
        "declinePct": pct(counts["declined"], d),
        "counts": counts,
        "updatedAt": sorted([x["updatedAt"] for x in analyzed])[-1] if analyzed else None,
        "leaders": [x["symbol"] for x in analyzed if x["above50"] and x["nearHigh"]][:12],
        "laggards": [x["symbol"] for x in analyzed if (not x["above50"] or x["nearLow"] or x["newLow"])][:12],
    }
    stats["status"] = universe_status(stats)
    stats["value"] = f"MA50上方 {stats['above50Pct']}% · MA200上方 {stats['above200Pct']}% · 新高/新低 {counts['newHigh']}/{counts['newLow']} · 上涨/下跌 {counts['advanced']}/{counts['declined']}"
    return stats


def main() -> int:
    generated = utc_now()
    sp500 = load_sp500()
    qqq, qqq_date = load_qqq()
    symbols = [r["yahooSymbol"] for r in sp500 + qqq]
    fetched = fetch_prices(symbols)
    spark = fetched["data"]

    snapshot = {
        "schemaVersion": 1,
        "generatedAt": generated,
        "description": "Daily static market breadth snapshot for stock.zhixingshe.cc. Free public sources; not investment advice.",
        "sources": {
            "sp500Constituents": SP500_URL,
            "qqqHoldings": QQQ_HOLDINGS_URL,
            "prices": "Yahoo Finance spark endpoint",
        },
        "dataStatus": "Cached",
        "universes": {
            "sp500": analyze_universe("S&P 500 宽度快照", SP500_URL, sp500, spark),
            "nasdaq100": analyze_universe("Nasdaq-100 / QQQ 持仓宽度快照", QQQ_HOLDINGS_URL, qqq, spark, qqq_date),
        },
        "fetchErrors": fetched["errors"],
        "limitations": "S&P500 constituents come from a public GitHub dataset; Nasdaq-100 uses Invesco QQQ holdings as a practical proxy and can include non-index cash/other rows filtered out. Yahoo spark is a free public endpoint and may throttle. Breadth signals show market structure, not institutional intent by themselves.",
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "out": str(OUT),
        "generatedAt": generated,
        "sp500": snapshot["universes"]["sp500"],
        "nasdaq100": snapshot["universes"]["nasdaq100"],
        "fetchErrors": len(fetched["errors"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
