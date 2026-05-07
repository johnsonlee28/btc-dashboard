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
import html
import json
import math
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "snapshots" / "market-breadth-latest.json"

SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
QQQ_HOLDINGS_URL = "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/holdings/fund?idType=ticker&interval=monthly&productType=ETF"
YAHOO_SPARK = "https://query1.finance.yahoo.com/v7/finance/spark"
FINRA_MARGIN_URL = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
FINRA_SHORT_SALE_URL_TEMPLATE = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"
AAII_SENTIMENT_URL = "https://www.aaii.com/sentimentsurvey"
UA = "stock-breadth-snapshot/1.0 (+https://stock.zhixingshe.cc)"
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_text(url: str, timeout: int = 30, headers: dict | None = None) -> str:
    req_headers = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
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
    last_error = None
    for attempt in range(3):
        try:
            raw = fetch_text(url, timeout=45)
            data = json.loads(raw)
            out = {}
            for item in data.get("spark", {}).get("result", []):
                sym = item.get("symbol")
                resp = (item.get("response") or [{}])[0]
                if sym and resp:
                    out[sym.upper()] = resp
            return out
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1))
    raise last_error


def fetch_prices(symbols: list[str], batch_size: int = 10, workers: int = 2) -> dict[str, dict]:
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
            time.sleep(0.15)
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


def parse_number(text: str) -> float | None:
    cleaned = re.sub(r"[^0-9.\-]", "", html.unescape(text or ""))
    if cleaned in {"", ".", "-"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_finra_margin() -> dict | None:
    try:
        text = fetch_text(FINRA_MARGIN_URL, timeout=45, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.finra.org/"})
    except Exception:
        text = fetch_text(FINRA_MARGIN_URL, timeout=45, headers={"User-Agent": UA, "Referer": "https://www.finra.org/"})
    rows = []
    for row_html in re.findall(r"<tr>(.*?)</tr>", text, flags=re.I | re.S):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.I | re.S)
        if len(cells) < 4:
            continue
        month = re.sub(r"<.*?>", "", cells[0]).strip()
        debit = parse_number(cells[1])
        cash_credit = parse_number(cells[2])
        margin_credit = parse_number(cells[3])
        if re.match(r"^[A-Z][a-z]{2}-\d{2}$", month) and debit is not None:
            rows.append({
                "month": month,
                "debitBalanceMillions": int(debit),
                "cashCreditBalanceMillions": int(cash_credit) if cash_credit is not None else None,
                "securitiesCreditBalanceMillions": int(margin_credit) if margin_credit is not None else None,
            })
    if not rows:
        return None
    latest = rows[0]
    latest_val = latest["debitBalanceMillions"]
    yoy_row = rows[12] if len(rows) >= 13 else None
    yoy = ((latest_val / yoy_row["debitBalanceMillions"] - 1) * 100) if yoy_row and yoy_row["debitBalanceMillions"] else None
    peak12 = max(r["debitBalanceMillions"] for r in rows[:12])
    drawdown = ((latest_val / peak12 - 1) * 100) if peak12 else None
    if yoy is not None and yoy >= 25 and (drawdown is None or drawdown > -10):
        status = "distribution"
    elif (yoy is not None and yoy < 0) or (drawdown is not None and drawdown <= -10):
        status = "accumulation"
    else:
        status = "neutral"
    return {
        "name": "FINRA Margin Debt 自动快照",
        "source": FINRA_MARGIN_URL,
        "sourceType": "official_html",
        "updatedAt": latest["month"],
        "value": f"{latest['month']} {latest_val:,}M" + (f" · YoY {yoy:+.1f}%" if yoy is not None else "") + (f" · 距12月峰 {drawdown:+.1f}%" if drawdown is not None else ""),
        "status": status,
        "latest": latest,
        "yoyPct": round(yoy, 1) if yoy is not None else None,
        "drawdownFrom12mPeakPct": round(drawdown, 1) if drawdown is not None else None,
        "rows": rows[:13],
        "threshold": "YoY > +25% 且未明显出清：杠杆偏热/派发风险；YoY < 0 或距12月峰回撤 ≤ -10%：去杠杆/承接改善；其他中性。",
    }


def fetch_aaii_html() -> str:
    headers = {
        "User-Agent": BROWSER_UA,
        "Referer": AAII_SENTIMENT_URL,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    text = fetch_text(AAII_SENTIMENT_URL, timeout=45, headers=headers)
    if "Pardon Our Interruption" not in text and ("bullTotalCnt" in text or "dataChart5" in text):
        return text
    # AAII/Imperva sometimes blocks Python urllib by TLS/HTTP fingerprint while curl passes.
    # GitHub Actions has curl by default; keep this as a no-secret, no-browser fallback.
    if not shutil.which("curl"):
        return text
    cmd = [
        "curl", "-sS", "--max-time", "45",
        "-A", BROWSER_UA,
        "-H", f"Referer: {AAII_SENTIMENT_URL}",
        "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "-H", "Accept-Language: en-US,en;q=0.9",
        AAII_SENTIMENT_URL,
    ]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=55)
    if result.returncode == 0 and result.stdout:
        return result.stdout
    return text


def parse_aaii_sentiment() -> dict | None:
    text = fetch_aaii_html()
    def pick_var(name: str) -> float | None:
        m = re.search(rf"var\s+{re.escape(name)}\s*=\s*([0-9.\-]+)", text)
        return float(m.group(1)) if m else None
    bullish = pick_var("bullTotalCnt")
    neutral = pick_var("neutralTotalCnt")
    bearish = pick_var("bearTotalCnt")
    dates = re.findall(r'"date_"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"', text)
    as_of = dates[-1] if dates else None
    if bullish is None or bearish is None:
        # Fallback to the latest dataChart5 row (integer precision).
        rows = re.findall(r'\{\s*"date_"\s*:\s*"([0-9-]+)".*?"bullish"\s*:\s*"([0-9.\-]+)".*?"bearish"\s*:\s*"([0-9.\-]+)".*?"neutral"\s*:\s*"([0-9.\-]+)".*?spread\s*:\s*"?([0-9.\-]+)"?', text, flags=re.S)
        if not rows:
            return None
        as_of, b, br, n, _ = rows[-1]
        bullish, bearish, neutral = float(b), float(br), float(n)
    spread = bullish - bearish
    if spread > 10 or bullish >= 50:
        status = "distribution"
    elif spread < -10 or bearish >= 50:
        status = "accumulation"
    else:
        status = "neutral"
    return {
        "name": "AAII Bull-Bear Spread 自动快照",
        "source": AAII_SENTIMENT_URL,
        "sourceType": "official_html_js",
        "updatedAt": as_of,
        "value": f"Bull {bullish:.1f}% · Bear {bearish:.1f}% · Spread {spread:+.1f}",
        "status": status,
        "bullishPct": round(bullish, 1),
        "neutralPct": round(neutral, 1) if neutral is not None else None,
        "bearishPct": round(bearish, 1),
        "spreadPct": round(spread, 1),
        "threshold": "Bull-Bear Spread > +10 或 Bullish ≥50%：散户乐观偏热/派发风险；Spread < -10 或 Bearish ≥50%：悲观拥挤/恐慌释放；其他中性。",
        "limitations": "AAII 页面受 Cloudflare 保护，需浏览器 UA/Referer；页面内变量为最新读数，dataChart5 仅约 52 周历史。",
    }


def parse_finra_short_sale_file(date_token: str) -> dict[str, dict] | None:
    url = FINRA_SHORT_SALE_URL_TEMPLATE.format(date=date_token)
    try:
        text = fetch_text(url, timeout=45, headers={"User-Agent": UA, "Accept": "text/plain,*/*"})
    except Exception:
        return None
    rows: dict[str, dict] = {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("Date|Symbol|ShortVolume"):
        return None
    for line in lines[1:]:
        parts = line.split("|")
        if len(parts) < 5:
            continue
        symbol = parts[1].strip().upper()
        short_volume = parse_number(parts[2])
        total_volume = parse_number(parts[4])
        if not symbol or short_volume is None or total_volume is None or total_volume <= 0:
            continue
        rows[symbol] = {
            "symbol": symbol,
            "shortVolume": float(short_volume),
            "totalVolume": float(total_volume),
            "ratioPct": round(short_volume / total_volume * 100, 1),
        }
    return rows or None


def recent_weekday_tokens(max_days: int = 45) -> list[str]:
    today = datetime.now(timezone.utc).date()
    tokens = []
    # FINRA daily short-sale files are posted after the US session; start from yesterday UTC
    # and walk back enough calendar days to cover weekends/holidays.
    for delta in range(1, max_days + 1):
        day = today - timedelta(days=delta)
        if day.weekday() < 5:
            tokens.append(day.strftime("%Y%m%d"))
    return tokens


def aggregate_short_rows(rows: dict[str, dict], symbols: set[str] | None = None, min_total: float = 0) -> dict:
    selected = [r for sym, r in rows.items() if (symbols is None or sym in symbols) and r.get("totalVolume", 0) >= min_total]
    short_total = sum(r["shortVolume"] for r in selected)
    volume_total = sum(r["totalVolume"] for r in selected)
    return {
        "tickerCount": len(selected),
        "shortVolume": round(short_total, 2),
        "totalVolume": round(volume_total, 2),
        "ratioPct": round(short_total / volume_total * 100, 1) if volume_total else None,
    }


def short_ratio_status(ratio: float | None, market_avg_20d: float | None) -> str:
    if ratio is None:
        return "pending"
    if ratio >= 58 or (market_avg_20d is not None and ratio >= market_avg_20d + 6):
        return "distribution"
    if ratio <= 42 or (market_avg_20d is not None and ratio <= market_avg_20d - 6):
        return "accumulation"
    return "neutral"


def parse_finra_short_sale_volume(sp500_rows: list[dict], qqq_rows: list[dict]) -> dict | None:
    history = []
    errors = []
    for token in recent_weekday_tokens(45):
        rows = parse_finra_short_sale_file(token)
        if rows:
            history.append({"date": token, "rows": rows})
            if len(history) >= 20:
                break
        else:
            errors.append(token)
    if not history:
        return None
    latest = history[0]
    latest_rows = latest["rows"]
    sp500_symbols = {r["yahooSymbol"].upper() for r in sp500_rows}
    qqq_symbols = {r["yahooSymbol"].upper() for r in qqq_rows}
    market_now = aggregate_short_rows(latest_rows)
    market_history = [aggregate_short_rows(day["rows"]) for day in history]
    ratios = [x["ratioPct"] for x in market_history if x.get("ratioPct") is not None]
    avg20 = round(mean(ratios), 1) if ratios else None
    sp500_now = aggregate_short_rows(latest_rows, sp500_symbols)
    qqq_now = aggregate_short_rows(latest_rows, qqq_symbols)
    abnormal = sorted(
        [r for r in latest_rows.values() if r["totalVolume"] >= 1_000_000 and r["ratioPct"] >= 65],
        key=lambda r: (r["ratioPct"], r["totalVolume"]),
        reverse=True,
    )[:12]
    status = short_ratio_status(market_now.get("ratioPct"), avg20)
    date_iso = f"{latest['date'][:4]}-{latest['date'][4:6]}-{latest['date'][6:]}"
    return {
        "name": "FINRA 场外短量占比自动快照",
        "source": FINRA_SHORT_SALE_URL_TEMPLATE.format(date=latest["date"]),
        "sourceType": "official_txt",
        "updatedAt": date_iso,
        "value": f"市场 {market_now['ratioPct']}% · 20日均 {avg20}% · S&P500 {sp500_now['ratioPct']}% · QQQ {qqq_now['ratioPct']}%",
        "status": status,
        "marketRatioPct": market_now.get("ratioPct"),
        "marketAverage20dPct": avg20,
        "sp500RatioPct": sp500_now.get("ratioPct"),
        "qqqRatioPct": qqq_now.get("ratioPct"),
        "latest": {
            "date": date_iso,
            "market": market_now,
            "sp500": sp500_now,
            "qqq": qqq_now,
        },
        "history": [{"date": f"{x['date'][:4]}-{x['date'][4:6]}-{x['date'][6:]}", "marketRatioPct": aggregate_short_rows(x["rows"]).get("ratioPct")} for x in history],
        "abnormalTickers": [{"symbol": r["symbol"], "ratioPct": r["ratioPct"], "totalVolume": round(r["totalVolume"], 0)} for r in abnormal],
        "missingRecentFiles": errors[:8],
        "threshold": "FINRA 场外 short/total ≥58% 或高于20日均值6个百分点：场外短量涌入/派发风险；≤42% 或低于20日均值6个百分点：空头压力缓和/承接改善；其他中性。",
        "limitations": "FINRA Daily Short Sale Volume 仅覆盖 TRF/ADF/ORF 等场外报告口径，不等同于全市场 short interest 或机构空头仓位；做市商对冲会造成噪声。",
    }


def collect_macro_indicators(fetch_errors: list[dict]) -> dict:
    indicators = {}
    for key, parser in [("finra_margin_debt", parse_finra_margin), ("aaii_bull_bear", parse_aaii_sentiment)]:
        try:
            item = parser()
            if item:
                indicators[key] = item
            else:
                fetch_errors.append({"source": key, "error": "parser returned no data"})
        except Exception as e:
            fetch_errors.append({"source": key, "error": str(e)})
    return indicators


def main() -> int:
    generated = utc_now()
    sp500 = load_sp500()
    qqq, qqq_date = load_qqq()
    symbols = [r["yahooSymbol"] for r in sp500 + qqq]
    fetched = fetch_prices(symbols)
    spark = fetched["data"]
    fetch_errors = list(fetched["errors"])
    macro_indicators = collect_macro_indicators(fetch_errors)
    try:
        short_sale = parse_finra_short_sale_volume(sp500, qqq)
        if short_sale:
            macro_indicators["finra_short_sale_volume"] = short_sale
        else:
            fetch_errors.append({"source": "finra_short_sale_volume", "error": "parser returned no data"})
    except Exception as e:
        fetch_errors.append({"source": "finra_short_sale_volume", "error": str(e)})

    snapshot = {
        "schemaVersion": 1,
        "generatedAt": generated,
        "description": "Daily static market breadth snapshot for stock.zhixingshe.cc. Free public sources; not investment advice.",
        "sources": {
            "sp500Constituents": SP500_URL,
            "qqqHoldings": QQQ_HOLDINGS_URL,
            "prices": "Yahoo Finance spark endpoint",
            "finraMarginDebt": FINRA_MARGIN_URL,
            "finraShortSaleVolume": "https://cdn.finra.org/equity/regsho/daily/CNMSshvolYYYYMMDD.txt",
            "aaiiSentiment": AAII_SENTIMENT_URL,
        },
        "dataStatus": "Cached",
        "universes": {
            "sp500": analyze_universe("S&P 500 宽度快照", SP500_URL, sp500, spark),
            "nasdaq100": analyze_universe("Nasdaq-100 / QQQ 持仓宽度快照", QQQ_HOLDINGS_URL, qqq, spark, qqq_date),
        },
        "macroIndicators": macro_indicators,
        "fetchErrors": fetch_errors,
        "limitations": "S&P500 constituents come from a public GitHub dataset; Nasdaq-100 uses Invesco QQQ holdings as a practical proxy and can include non-index cash/other rows filtered out. Yahoo spark is a free public endpoint and may throttle. Breadth signals show market structure, not institutional intent by themselves.",
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "out": str(OUT),
        "generatedAt": generated,
        "sp500": snapshot["universes"]["sp500"],
        "nasdaq100": snapshot["universes"]["nasdaq100"],
        "macroIndicators": snapshot["macroIndicators"],
        "fetchErrors": len(snapshot["fetchErrors"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
