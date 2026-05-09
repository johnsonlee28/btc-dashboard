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
import io
import json
import math
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
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
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_ARCHIVE_TXT_URL_TEMPLATE = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{accession}.txt"
SEC_FTD_PAGE_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"
SEC_FTD_BASE_URL = "https://www.sec.gov"
BARCHART_QUOTE_URL_TEMPLATE = "https://www.barchart.com/stocks/quotes/{symbol}"
BARCHART_NYSE_BREADTH_SYMBOLS = {
    "advancers": "$ADVN",
    "decliners": "$DECN",
    "adRatio": "$ADRN",
    "newHighs52w": "$MAHN",
    "newLows52w": "$MALN",
}
SEC_FORM4_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "AVGO", "ORCL", "PLTR", "CRM", "AMD", "GE", "IBM", "QCOM",
    "NOW", "ADBE", "APP", "ISRG", "PDD", "NEE", "CEG", "VST", "OKLO",
]
SEC_13F_MANAGERS = [
    {"name": "Citadel Advisors", "cik": "0001423053"},
    {"name": "Renaissance Technologies", "cik": "0001037389"},
    {"name": "ARK Investment Management", "cik": "0001697748"},
    {"name": "Millennium Management", "cik": "0001273087"},
    {"name": "D. E. Shaw", "cik": "0001009207"},
    {"name": "Coatue Management", "cik": "0001135730"},
    {"name": "Tiger Global Management", "cik": "0001167483"},
    {"name": "Berkshire Hathaway", "cik": "0001067983"},
]
SEC_13F_TARGETS = {
    "AAPL": {"cusips": ["037833100"], "patterns": ["APPLE INC"]},
    "MSFT": {"cusips": ["594918104"], "patterns": ["MICROSOFT CORP"]},
    "NVDA": {"cusips": ["67066G104"], "patterns": ["NVIDIA CORP"]},
    "AMZN": {"cusips": ["023135106"], "patterns": ["AMAZON COM INC"]},
    "GOOGL": {"cusips": ["02079K305", "02079K107"], "patterns": ["ALPHABET INC"]},
    "META": {"cusips": ["30303M102"], "patterns": ["META PLATFORMS INC"]},
    "TSLA": {"cusips": ["88160R101"], "patterns": ["TESLA INC"]},
    "AVGO": {"cusips": ["11135F101"], "patterns": ["BROADCOM INC"]},
    "ORCL": {"cusips": ["68389X105"], "patterns": ["ORACLE CORP"]},
    "PLTR": {"cusips": ["69608A108"], "patterns": ["PALANTIR TECHNOLOGIES"]},
    "CRM": {"cusips": ["79466L302"], "patterns": ["SALESFORCE INC"]},
    "AMD": {"cusips": ["007903107"], "patterns": ["ADVANCED MICRO DEVICES"]},
    "IBM": {"cusips": ["459200101"], "patterns": ["INTL BUSINESS MACHS", "INTERNATIONAL BUSINESS"]},
    "QCOM": {"cusips": ["747525103"], "patterns": ["QUALCOMM INC"]},
    "NOW": {"cusips": ["81762P102"], "patterns": ["SERVICENOW INC"]},
    "ADBE": {"cusips": ["00724F101"], "patterns": ["ADOBE INC"]},
    "APP": {"cusips": ["03831W108"], "patterns": ["APPLOVIN CORP"]},
    "ISRG": {"cusips": ["46120E602"], "patterns": ["INTUITIVE SURGICAL"]},
    "PDD": {"cusips": ["722304102"], "patterns": ["PDD HOLDINGS"]},
    "NEE": {"cusips": ["65339F101"], "patterns": ["NEXTERA ENERGY"]},
    "CEG": {"cusips": ["21037T109"], "patterns": ["CONSTELLATION ENERGY"]},
    "VST": {"cusips": ["92840M102"], "patterns": ["VISTRA CORP"]},
    "OKLO": {"cusips": ["02156V109"], "patterns": ["OKLO INC"]},
}
UA = "stock-breadth-snapshot/1.0 (+https://stock.zhixingshe.cc; contact: admin@zhixingshe.cc)"
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


def fetch_bytes(url: str, timeout: int = 45, headers: dict | None = None) -> bytes:
    req_headers = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


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


def load_sec_ticker_cik_map() -> dict[str, dict]:
    data = json.loads(fetch_text(SEC_COMPANY_TICKERS_URL, timeout=30, headers={"User-Agent": UA, "Accept": "application/json"}))
    out = {}
    for row in data.values():
        ticker = (row.get("ticker") or "").strip().upper().replace(".", "-")
        cik = row.get("cik_str")
        if ticker and cik:
            out[ticker] = {
                "cik": str(cik),
                "cik10": str(cik).zfill(10),
                "title": row.get("title") or ticker,
            }
    return out


def sec_recent_form4_filings(cik10: str, limit: int = 8) -> list[dict]:
    data = json.loads(fetch_text(SEC_SUBMISSIONS_URL_TEMPLATE.format(cik10=cik10), timeout=30, headers={"User-Agent": UA, "Accept": "application/json"}))
    recent = data.get("filings", {}).get("recent", {})
    filings = []
    for form, accession, filing_date, report_date in zip(
        recent.get("form", []),
        recent.get("accessionNumber", []),
        recent.get("filingDate", []),
        recent.get("reportDate", []),
    ):
        if form == "4":
            filings.append({
                "accessionNumber": accession,
                "filingDate": filing_date,
                "reportDate": report_date,
            })
        if len(filings) >= limit:
            break
    return filings


def extract_ownership_xml(submission_text: str) -> str | None:
    m = re.search(r"<ownershipDocument[\s\S]*?</ownershipDocument>", submission_text)
    return m.group(0) if m else None


def xml_text(node, path: str) -> str | None:
    found = node.find(path)
    return found.text.strip() if found is not None and found.text else None


def parse_float_safe(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text.upper() in {"N/A", "NA", "-"}:
        return None
    try:
        out = float(text)
        return out if math.isfinite(out) else None
    except Exception:
        return None


def parse_sec_form4_transactions(ticker: str, cik: str, filing: dict) -> list[dict]:
    accession = filing["accessionNumber"]
    url = SEC_ARCHIVE_TXT_URL_TEMPLATE.format(cik=str(int(cik)), accession_nodash=accession.replace("-", ""), accession=accession)
    text = fetch_text(url, timeout=30, headers={"User-Agent": UA, "Accept": "text/plain,*/*"})
    xml = extract_ownership_xml(text)
    if not xml:
        return []
    root = ET.fromstring(xml)
    owner_name = xml_text(root, "./reportingOwner/reportingOwnerId/rptOwnerName")
    owner_title = xml_text(root, "./reportingOwner/reportingOwnerRelationship/officerTitle")
    is_director = xml_text(root, "./reportingOwner/reportingOwnerRelationship/isDirector") == "1"
    is_officer = xml_text(root, "./reportingOwner/reportingOwnerRelationship/isOfficer") == "1"
    out = []
    for tx in root.findall("./nonDerivativeTable/nonDerivativeTransaction"):
        code = xml_text(tx, "./transactionCoding/transactionCode")
        acq_disp = xml_text(tx, "./transactionAmounts/transactionAcquiredDisposedCode/value")
        shares = parse_float_safe(xml_text(tx, "./transactionAmounts/transactionShares/value"))
        price = parse_float_safe(xml_text(tx, "./transactionAmounts/transactionPricePerShare/value")) or 0.0
        tx_date = xml_text(tx, "./transactionDate/value") or filing.get("reportDate") or filing.get("filingDate")
        if code not in {"S", "P"} or shares is None or shares <= 0:
            continue
        value = shares * price
        direction = "sale" if code == "S" or acq_disp == "D" else "purchase"
        out.append({
            "ticker": ticker,
            "transactionDate": tx_date,
            "filingDate": filing.get("filingDate"),
            "accessionNumber": accession,
            "ownerName": owner_name,
            "ownerTitle": owner_title,
            "isDirector": is_director,
            "isOfficer": is_officer,
            "code": code,
            "direction": direction,
            "shares": round(shares, 2),
            "price": round(price, 4) if price else None,
            "valueUsd": round(value, 2),
            "sourceUrl": url,
        })
    return out


def sec_form4_status(sell_value: float, buy_value: float, sale_tickers: int, buy_tickers: int) -> str:
    net_sell = sell_value - buy_value
    if sell_value >= 50_000_000 and net_sell >= 25_000_000 and sale_tickers >= 3:
        return "distribution"
    if buy_value >= 5_000_000 and buy_value >= sell_value * 1.5 and buy_tickers >= 2:
        return "accumulation"
    return "neutral"


def collect_sec_form4_insider_activity(fetch_errors: list[dict]) -> dict | None:
    cik_map = load_sec_ticker_cik_map()
    since_date = (datetime.now(timezone.utc).date() - timedelta(days=90)).isoformat()
    transactions = []
    missing = []
    for ticker in SEC_FORM4_TICKERS:
        info = cik_map.get(ticker)
        if not info:
            missing.append(ticker)
            continue
        try:
            filings = sec_recent_form4_filings(info["cik10"], limit=8)
            time.sleep(0.12)
            for filing in filings:
                if filing.get("filingDate") and filing["filingDate"] < since_date:
                    continue
                try:
                    transactions.extend(parse_sec_form4_transactions(ticker, info["cik"], filing))
                except Exception as e:
                    fetch_errors.append({"source": "sec_form4_insider_activity", "ticker": ticker, "accession": filing.get("accessionNumber"), "error": str(e)[:180]})
                time.sleep(0.12)
        except Exception as e:
            fetch_errors.append({"source": "sec_form4_insider_activity", "ticker": ticker, "error": str(e)[:180]})
    sales = [t for t in transactions if t["direction"] == "sale"]
    purchases = [t for t in transactions if t["direction"] == "purchase"]
    sell_value = sum(t["valueUsd"] for t in sales)
    buy_value = sum(t["valueUsd"] for t in purchases)
    sale_by_ticker = {}
    buy_by_ticker = {}
    for t in sales:
        sale_by_ticker[t["ticker"]] = sale_by_ticker.get(t["ticker"], 0) + t["valueUsd"]
    for t in purchases:
        buy_by_ticker[t["ticker"]] = buy_by_ticker.get(t["ticker"], 0) + t["valueUsd"]
    status = sec_form4_status(sell_value, buy_value, len(sale_by_ticker), len(buy_by_ticker))
    top_sales = sorted(sales, key=lambda x: x["valueUsd"], reverse=True)[:12]
    top_purchases = sorted(purchases, key=lambda x: x["valueUsd"], reverse=True)[:8]
    value = f"90日卖出 ${sell_value/1_000_000:.1f}M · 买入 ${buy_value/1_000_000:.1f}M · 卖出股票 {len(sale_by_ticker)}只 · 买入股票 {len(buy_by_ticker)}只"
    latest_date = max([t["filingDate"] for t in transactions if t.get("filingDate")] or [utc_now()[:10]])
    return {
        "name": "SEC Form 4 内部人交易快照",
        "source": "https://www.sec.gov/edgar/search/",
        "sourceType": "official_sec_form4",
        "updatedAt": latest_date,
        "value": value,
        "status": status,
        "lookbackDays": 90,
        "tickerUniverse": SEC_FORM4_TICKERS,
        "coveredTickers": [t for t in SEC_FORM4_TICKERS if t in cik_map],
        "missingTickers": missing,
        "sellValueUsd": round(sell_value, 2),
        "buyValueUsd": round(buy_value, 2),
        "saleTickerCount": len(sale_by_ticker),
        "buyTickerCount": len(buy_by_ticker),
        "topSaleTickers": sorted([{"ticker": k, "valueUsd": round(v, 2)} for k, v in sale_by_ticker.items()], key=lambda x: x["valueUsd"], reverse=True)[:10],
        "topBuyTickers": sorted([{"ticker": k, "valueUsd": round(v, 2)} for k, v in buy_by_ticker.items()], key=lambda x: x["valueUsd"], reverse=True)[:10],
        "topSales": top_sales,
        "topPurchases": top_purchases,
        "transactionCount": len(transactions),
        "threshold": "90日内部人公开市场卖出≥$50M、净卖出≥$25M、且涉及≥3只样本股：内部人卖出证据偏强；公开市场买入≥$5M、买入额≥卖出额1.5倍、且涉及≥2只样本股：内部人买入/承接证据偏强；其他中性。",
        "limitations": "SEC Form 4 只覆盖公司内部人交易，不等于机构13F持仓或暗池/期权流；大型科技公司高管10b5-1计划性卖出很常见，必须结合价格、宽度、成交量和公告背景判断。",
    }


def local_name(tag: str) -> str:
    return tag.rsplit('}', 1)[-1] if '}' in tag else tag


def child_text_local(node, name: str) -> str | None:
    for child in list(node):
        if local_name(child.tag) == name:
            if name == "shrsOrPrnAmt":
                for sub in list(child):
                    if local_name(sub.tag) == "sshPrnamt" and sub.text:
                        return sub.text.strip()
            return child.text.strip() if child.text else None
    return None


def recent_13f_filings(cik10: str, limit: int = 2) -> list[dict]:
    data = json.loads(fetch_text(SEC_SUBMISSIONS_URL_TEMPLATE.format(cik10=cik10), timeout=30, headers={"User-Agent": UA, "Accept": "application/json"}))
    recent = data.get("filings", {}).get("recent", {})
    filings = []
    for form, accession, filing_date, report_date in zip(
        recent.get("form", []), recent.get("accessionNumber", []), recent.get("filingDate", []), recent.get("reportDate", [])
    ):
        if form in {"13F-HR", "13F-HR/A"}:
            filings.append({"form": form, "accessionNumber": accession, "filingDate": filing_date, "reportDate": report_date})
        if len(filings) >= limit:
            break
    return filings


def match_13f_target(name: str, cusip: str) -> str | None:
    n = re.sub(r"\s+", " ", (name or "").upper()).strip()
    c = (cusip or "").upper().strip()
    for ticker, spec in SEC_13F_TARGETS.items():
        if c and c in spec.get("cusips", []):
            return ticker
        if any(p in n for p in spec.get("patterns", [])):
            return ticker
    return None


def extract_information_table_xml(submission_text: str) -> str | None:
    m = re.search(r"<informationTable[\s\S]*?</informationTable>", submission_text)
    return m.group(0) if m else None


def parse_13f_target_holdings(cik: str, filing: dict) -> dict[str, dict]:
    accession = filing["accessionNumber"]
    url = SEC_ARCHIVE_TXT_URL_TEMPLATE.format(cik=str(int(cik)), accession_nodash=accession.replace("-", ""), accession=accession)
    text = fetch_text(url, timeout=45, headers={"User-Agent": UA, "Accept": "text/plain,*/*"})
    xml = extract_information_table_xml(text)
    if not xml:
        return {}
    root = ET.fromstring(xml)
    holdings: dict[str, dict] = {}
    for row in root.iter():
        if local_name(row.tag) != "infoTable":
            continue
        put_call = child_text_local(row, "putCall")
        if put_call:
            continue  # keep common-stock exposure separate from option positions
        issuer = child_text_local(row, "nameOfIssuer") or ""
        cusip = child_text_local(row, "cusip") or ""
        ticker = match_13f_target(issuer, cusip)
        if not ticker:
            continue
        value_usd = parse_float_safe(child_text_local(row, "value")) or 0.0
        shares = parse_float_safe(child_text_local(row, "shrsOrPrnAmt")) or 0.0
        current = holdings.setdefault(ticker, {"ticker": ticker, "issuer": issuer, "cusip": cusip, "valueUsd": 0.0, "shares": 0.0})
        # Recent SEC 13F XML information tables expose value at dollar scale in the
        # flattened/as-filed data we parse from EDGAR txt submissions. Do not multiply by 1000.
        current["valueUsd"] += value_usd
        current["shares"] += shares
    for item in holdings.values():
        item["valueUsd"] = round(item["valueUsd"], 2)
        item["shares"] = round(item["shares"], 2)
    return holdings


def collect_sec_13f_institutional_sample(fetch_errors: list[dict]) -> dict | None:
    managers = []
    changed_rows = []
    increased = decreased = unchanged = 0
    current_value = previous_value = 0.0
    for manager in SEC_13F_MANAGERS:
        cik = manager["cik"]
        try:
            filings = recent_13f_filings(cik, limit=2)
            time.sleep(0.15)
            if len(filings) < 2:
                fetch_errors.append({"source": "sec_13f_institutional_sample", "manager": manager["name"], "error": "less than two 13F filings"})
                continue
            current_filing, previous_filing = filings[0], filings[1]
            current = parse_13f_target_holdings(cik, current_filing)
            time.sleep(0.15)
            previous = parse_13f_target_holdings(cik, previous_filing)
            time.sleep(0.15)
            manager_current_value = sum(x["valueUsd"] for x in current.values())
            manager_previous_value = sum(x["valueUsd"] for x in previous.values())
            current_value += manager_current_value
            previous_value += manager_previous_value
            tickers = sorted(set(current) | set(previous))
            manager_changes = []
            for ticker in tickers:
                cur = current.get(ticker, {"shares": 0, "valueUsd": 0, "issuer": None})
                prev = previous.get(ticker, {"shares": 0, "valueUsd": 0})
                share_change = cur["shares"] - prev["shares"]
                if abs(share_change) < 1:
                    unchanged += 1
                    direction = "flat"
                elif share_change > 0:
                    increased += 1
                    direction = "increase"
                else:
                    decreased += 1
                    direction = "decrease"
                pct_change = round(share_change / prev["shares"] * 100, 1) if prev.get("shares") else (100.0 if share_change > 0 else None)
                row = {
                    "manager": manager["name"],
                    "ticker": ticker,
                    "direction": direction,
                    "shares": round(cur["shares"], 2),
                    "previousShares": round(prev["shares"], 2),
                    "shareChange": round(share_change, 2),
                    "shareChangePct": pct_change,
                    "valueUsd": round(cur["valueUsd"], 2),
                    "previousValueUsd": round(prev["valueUsd"], 2),
                }
                manager_changes.append(row)
                if direction != "flat":
                    changed_rows.append(row)
            managers.append({
                "name": manager["name"],
                "cik": cik,
                "currentFilingDate": current_filing.get("filingDate"),
                "currentReportDate": current_filing.get("reportDate"),
                "previousFilingDate": previous_filing.get("filingDate"),
                "previousReportDate": previous_filing.get("reportDate"),
                "targetValueUsd": round(manager_current_value, 2),
                "previousTargetValueUsd": round(manager_previous_value, 2),
                "targetTickerCount": len(current),
                "changes": sorted(manager_changes, key=lambda x: abs(x.get("valueUsd", 0)), reverse=True)[:12],
            })
        except Exception as e:
            fetch_errors.append({"source": "sec_13f_institutional_sample", "manager": manager["name"], "error": str(e)[:180]})
    total_changes = increased + decreased + unchanged
    if not managers:
        return None
    decrease_ratio = decreased / total_changes * 100 if total_changes else 0
    increase_ratio = increased / total_changes * 100 if total_changes else 0
    value_change_pct = round((current_value / previous_value - 1) * 100, 1) if previous_value else None
    if decrease_ratio >= 60 and value_change_pct is not None and value_change_pct <= -10:
        status = "distribution"
    elif increase_ratio >= 60 and value_change_pct is not None and value_change_pct >= 10:
        status = "accumulation"
    else:
        status = "neutral"
    latest_date = max([m["currentFilingDate"] for m in managers if m.get("currentFilingDate")] or [utc_now()[:10]])
    top_decreases = sorted([r for r in changed_rows if r["direction"] == "decrease"], key=lambda x: abs(x["shareChange"]), reverse=True)[:12]
    top_increases = sorted([r for r in changed_rows if r["direction"] == "increase"], key=lambda x: abs(x["shareChange"]), reverse=True)[:12]
    return {
        "name": "SEC 13F 样本机构持仓慢变量",
        "source": "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets",
        "sourceType": "official_sec_13f",
        "updatedAt": latest_date,
        "value": f"样本机构AI/Mag7持仓 ${current_value/1_000_000_000:.1f}B · 较上季 {value_change_pct:+.1f}% · 增/减 {increased}/{decreased}",
        "status": status,
        "managerCount": len(managers),
        "currentValueUsd": round(current_value, 2),
        "previousValueUsd": round(previous_value, 2),
        "valueChangePct": value_change_pct,
        "increasedPositions": increased,
        "decreasedPositions": decreased,
        "unchangedPositions": unchanged,
        "managers": managers,
        "topIncreases": top_increases,
        "topDecreases": top_decreases,
        "threshold": "样本持仓市值较上季下降≥10%、且减持位置占比≥60%：机构样本持仓收缩/派发压力；上升≥10%、且增持位置占比≥60%：机构样本暴露增加/承接改善；否则中性。",
        "limitations": "13F 是季度披露，最多滞后45天；只覆盖样本机构多头持仓，不含空头、衍生品完整风险、盘中交易或非13F证券。CUSIP/ticker 映射采用白名单与发行人名称匹配，适合慢变量观察，不适合短线交易信号。",
    }


def latest_sec_ftd_zip_urls(limit: int = 2) -> list[str]:
    html_text = fetch_text(SEC_FTD_PAGE_URL, timeout=45, headers={"User-Agent": UA, "Accept": "text/html,*/*"})
    links = re.findall(r'href="([^"]*cnsfails\d{6}[ab](?:_\d+)?\.zip)"', html_text, flags=re.I)
    seen = []
    for link in links:
        url = urllib.parse.urljoin(SEC_FTD_BASE_URL, link)
        if url not in seen:
            seen.append(url)
    return seen[:limit]


def parse_sec_ftd_zip(url: str, target_symbols: set[str]) -> dict:
    raw = fetch_bytes(url, timeout=90, headers={"User-Agent": UA, "Accept": "application/zip,*/*"})
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = [n for n in zf.namelist() if not n.endswith('/')]
        if not names:
            return {"url": url, "rows": [], "latestDate": None}
        with zf.open(names[0]) as f:
            text = f.read().decode("latin-1", "ignore")
    reader = csv.DictReader(text.splitlines(), delimiter="|")
    for row in reader:
        symbol = (row.get("SYMBOL") or row.get("Symbol") or "").strip().upper()
        if symbol not in target_symbols:
            continue
        date_raw = (row.get("SETTLEMENT DATE") or row.get("SETTLEMENTDATE") or row.get("Date") or "").strip()
        quantity = parse_float_safe(row.get("QUANTITY (FAILS)") or row.get("QUANTITY") or row.get("FAILS")) or 0
        price = parse_float_safe(row.get("PRICE")) or 0
        rows.append({
            "date": date_raw,
            "symbol": symbol,
            "cusip": (row.get("CUSIP") or "").strip(),
            "issuer": (row.get("DESCRIPTION") or row.get("ISSUER") or "").strip(),
            "fails": int(quantity),
            "price": round(price, 4),
            "notionalUsd": round(quantity * price, 2),
        })
    latest = max([r["date"] for r in rows] or [None])
    return {"url": url, "rows": rows, "latestDate": latest}


def collect_sec_ftd_settlement_pressure(fetch_errors: list[dict]) -> dict | None:
    targets = set(SEC_FORM4_TICKERS)
    urls = latest_sec_ftd_zip_urls(limit=4)
    if not urls:
        return None
    parsed = []
    for url in urls:
        try:
            parsed.append(parse_sec_ftd_zip(url, targets))
            time.sleep(0.1)
        except Exception as e:
            fetch_errors.append({"source": "sec_ftd_settlement_pressure", "url": url, "error": str(e)[:180]})
    all_rows = [r for p in parsed for r in p.get("rows", [])]
    if not all_rows:
        return None
    latest_date = max(r["date"] for r in all_rows if r.get("date"))
    latest_rows = [r for r in all_rows if r.get("date") == latest_date]
    by_symbol: dict[str, dict] = {}
    history_by_symbol: dict[str, list[dict]] = {}
    for row in all_rows:
        history_by_symbol.setdefault(row["symbol"], []).append(row)
    for symbol, rows in history_by_symbol.items():
        rows_sorted = sorted(rows, key=lambda x: x.get("date") or "")
        latest = rows_sorted[-1]
        max_notional = max(r["notionalUsd"] for r in rows_sorted)
        avg_notional = mean([r["notionalUsd"] for r in rows_sorted]) if rows_sorted else 0
        by_symbol[symbol] = {
            "symbol": symbol,
            "latestDate": latest["date"],
            "fails": latest["fails"],
            "price": latest["price"],
            "notionalUsd": latest["notionalUsd"],
            "maxNotionalUsd": round(max_notional, 2),
            "avgNotionalUsd": round(avg_notional, 2),
            "notionalVsAvg": round(latest["notionalUsd"] / avg_notional, 2) if avg_notional else None,
        }
    latest_items = [by_symbol[r["symbol"]] for r in latest_rows if r["symbol"] in by_symbol]
    latest_total = sum(r["notionalUsd"] for r in latest_rows)
    historical_totals = {}
    for r in all_rows:
        historical_totals[r["date"]] = historical_totals.get(r["date"], 0) + r["notionalUsd"]
    avg_total = mean(historical_totals.values()) if historical_totals else 0
    high_count = sum(1 for item in latest_items if item["notionalUsd"] >= 10_000_000 or (item.get("notionalVsAvg") or 0) >= 3)
    if latest_total >= 250_000_000 or high_count >= 5:
        status = "distribution"
    elif latest_total <= 50_000_000 and high_count == 0:
        status = "accumulation"
    else:
        status = "neutral"
    top = sorted(latest_items, key=lambda x: x["notionalUsd"], reverse=True)[:12]
    return {
        "name": "SEC FTD 结算压力快照",
        "source": SEC_FTD_PAGE_URL,
        "sourceType": "official_sec_ftd_zip",
        "updatedAt": latest_date,
        "value": f"样本FTD ${latest_total/1_000_000:.1f}M · 活跃 {len(latest_rows)}只 · 异常 {high_count}只",
        "status": status,
        "latestDate": latest_date,
        "latestNotionalUsd": round(latest_total, 2),
        "averageNotionalUsd": round(avg_total, 2),
        "activeTickerCount": len(latest_rows),
        "abnormalTickerCount": high_count,
        "files": [p.get("url") for p in parsed],
        "topTickers": top,
        "threshold": "样本最新 FTD 名义金额≥$250M 或异常股票≥5只：结算压力升温/派发压力线索；≤$50M 且无异常股票：结算压力低/承接改善；其他中性。异常股票指单票FTD名义金额≥$10M或高于近期均值3倍。",
        "limitations": "FTD 是 NSCC CNS 系统的交割失败余额，不是每日新增失败数；可能来自多种长/短交易原因，不等于裸卖空或机构交易行为证据。SEC 通常半月披露，存在延迟；价格字段为前一日收盘价且SEC不保证与其他源完全一致。",
    }


def parse_barchart_quote(symbol: str) -> dict:
    encoded = urllib.parse.quote(symbol, safe="")
    url = BARCHART_QUOTE_URL_TEMPLATE.format(symbol=encoded)
    text = fetch_text(url, timeout=30, headers={"User-Agent": BROWSER_UA, "Accept": "text/html,*/*"})
    match = re.search(r"data-ng-init='init\((\{.*?\})\)'", text)
    if not match:
        raise ValueError(f"Barchart quote payload not found for {symbol}")
    payload = json.loads(html.unescape(match.group(1)))
    return {
        "symbol": payload.get("symbol") or symbol,
        "name": payload.get("symbolName") or symbol,
        "lastPrice": parse_float_safe(payload.get("lastPrice")),
        "priceChange": parse_float_safe(payload.get("priceChange")),
        "percentChange": parse_float_safe(payload.get("percentChange")),
        "tradeTime": payload.get("tradeTime"),
        "sessionDateDisplayLong": payload.get("sessionDateDisplayLong"),
        "source": url,
    }


def collect_nyse_breadth_indicators(fetch_errors: list[dict]) -> dict:
    quotes = {}
    for key, symbol in BARCHART_NYSE_BREADTH_SYMBOLS.items():
        try:
            quotes[key] = parse_barchart_quote(symbol)
            time.sleep(0.15)
        except Exception as e:
            fetch_errors.append({"source": "barchart_nyse_breadth", "symbol": symbol, "error": str(e)[:180]})

    out = {}
    adv = quotes.get("advancers", {}).get("lastPrice")
    dec = quotes.get("decliners", {}).get("lastPrice")
    ratio = quotes.get("adRatio", {}).get("lastPrice")
    if adv is not None and dec is not None and dec > 0:
        ratio = ratio if ratio is not None else adv / dec
        if dec >= adv * 1.25 or ratio <= 0.8:
            status = "distribution"
        elif adv >= dec * 1.5 or ratio >= 1.5:
            status = "accumulation"
        else:
            status = "neutral"
        out["nyse_ad_line"] = {
            "name": "NYSE A/D 日宽度",
            "source": BARCHART_QUOTE_URL_TEMPLATE.format(symbol=urllib.parse.quote("$ADVN", safe="")),
            "sourceType": "barchart_scraped_quote",
            "updatedAt": utc_now(),
            "value": f"上涨 {adv:.0f} · 下跌 {dec:.0f} · A/D {ratio:.2f}",
            "status": status,
            "advancers": int(round(adv)),
            "decliners": int(round(dec)),
            "advanceDeclineRatio": round(ratio, 3),
            "quotes": {k: quotes.get(k) for k in ["advancers", "decliners", "adRatio"] if quotes.get(k)},
            "threshold": "A/D ≤0.8 或下跌家数≥上涨家数1.25倍：宽度恶化/派发压力；A/D ≥1.5：宽度扩散/承接改善；中间中性。",
            "limitations": "Barchart 页面抓取非官方 JSON，可能改版或限流；该项是当日 NYSE 上涨/下跌家数与比率，不是 StockCharts $NYAD 累计 A/D Line。宽度恶化只能说明市场结构走弱，不能单独证明机构交易行为。",
        }

    highs = quotes.get("newHighs52w", {}).get("lastPrice")
    lows = quotes.get("newLows52w", {}).get("lastPrice")
    if highs is not None and lows is not None:
        hl_ratio = highs / lows if lows > 0 else None
        if lows > highs or lows >= 100:
            status = "distribution"
        elif highs >= 100 and (hl_ratio is None or hl_ratio >= 3):
            status = "accumulation"
        else:
            status = "neutral"
        ratio_text = f" · H/L {hl_ratio:.2f}" if hl_ratio is not None else " · H/L ∞"
        out["nyse_nh_nl"] = {
            "name": "NYSE 52周新高 / 新低",
            "source": BARCHART_QUOTE_URL_TEMPLATE.format(symbol=urllib.parse.quote("$MAHN", safe="")),
            "sourceType": "barchart_scraped_quote",
            "updatedAt": utc_now(),
            "value": f"新高 {highs:.0f} · 新低 {lows:.0f}{ratio_text}",
            "status": status,
            "newHighs52w": int(round(highs)),
            "newLows52w": int(round(lows)),
            "highLowRatio": round(hl_ratio, 3) if hl_ratio is not None else None,
            "quotes": {k: quotes.get(k) for k in ["newHighs52w", "newLows52w"] if quotes.get(k)},
            "threshold": "新低家数>新高家数或新低≥100：宽度恶化/派发压力；新高≥100且新高/新低≥3：宽度扩散/承接改善；中间中性。",
            "limitations": "Barchart 页面抓取非官方 JSON，可能改版或限流；该项只反映 NYSE 52周新高/新低家数快照，不直接反映机构交易。需与指数位置、A/D、RSP/SPY、Form 4/13F 等共振判断。",
        }
    return out


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
        macro_indicators.update(collect_nyse_breadth_indicators(fetch_errors))
    except Exception as e:
        fetch_errors.append({"source": "barchart_nyse_breadth", "error": str(e)[:180]})
    try:
        short_sale = parse_finra_short_sale_volume(sp500, qqq)
        if short_sale:
            macro_indicators["finra_short_sale_volume"] = short_sale
        else:
            fetch_errors.append({"source": "finra_short_sale_volume", "error": "parser returned no data"})
    except Exception as e:
        fetch_errors.append({"source": "finra_short_sale_volume", "error": str(e)})

    try:
        form4 = collect_sec_form4_insider_activity(fetch_errors)
        if form4:
            macro_indicators["sec_form4_insider_activity"] = form4
        else:
            fetch_errors.append({"source": "sec_form4_insider_activity", "error": "parser returned no data"})
    except Exception as e:
        fetch_errors.append({"source": "sec_form4_insider_activity", "error": str(e)})

    try:
        sec13f = collect_sec_13f_institutional_sample(fetch_errors)
        if sec13f:
            macro_indicators["sec_13f_institutional_sample"] = sec13f
        else:
            fetch_errors.append({"source": "sec_13f_institutional_sample", "error": "parser returned no data"})
    except Exception as e:
        fetch_errors.append({"source": "sec_13f_institutional_sample", "error": str(e)})

    try:
        ftd = collect_sec_ftd_settlement_pressure(fetch_errors)
        if ftd:
            macro_indicators["sec_ftd_settlement_pressure"] = ftd
        else:
            fetch_errors.append({"source": "sec_ftd_settlement_pressure", "error": "parser returned no data"})
    except Exception as e:
        fetch_errors.append({"source": "sec_ftd_settlement_pressure", "error": str(e)})

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
            "secCompanyTickers": SEC_COMPANY_TICKERS_URL,
            "secSubmissions": "https://data.sec.gov/submissions/CIK##########.json",
            "sec13fDataSets": "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets",
            "secFtdDataSets": SEC_FTD_PAGE_URL,
            "barchartNyseBreadth": "https://www.barchart.com/stocks/quotes/$ADVN and $MAHN/$MALN",
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
