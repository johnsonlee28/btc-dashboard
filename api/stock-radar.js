/**
 * Vercel Edge Function: /api/stock-radar
 * 美股派发/承接雷达 V2
 *
 * 原则：
 * - 可解释性 > 漂亮。每个指标必须带 sourceName / sourceUrl / frequency / updatedAt / dataStatus
 *   / logic / threshold / caseStudy / limitations 完整元数据。
 * - 免费公开接口（Yahoo chart）失败时，指标 dataStatus = "data_unavailable"，
 *   不让整个 API 失败；不伪造值。
 * - AAII / BofA FMS / FINRA Margin Debt 目前无稳定 Edge 可抓取接口，
 *   走 Manual；CBOE Put/Call 从官方 Daily Market Statistics HTML 的 Next payload 自动解析。
 * - 仅作研究参考，不构成投资建议。
 */

export const config = { runtime: 'edge' };

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  // 股票数据本身更新频率低，给个稍长缓存
  'Cache-Control': 's-maxage=300, stale-while-revalidate=900',
};

// Yahoo Finance chart endpoints (免费、无鉴权、偶尔 429)
const YF_CHART = (symbol, range = '3mo', interval = '1d') =>
  `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${range}&interval=${interval}`;

// V2 第二刀：真实宽度先从“AI 主题核心样本”做起。
// 选择标准：data/ai-stock-pool.json 中按近似市值排序的前 24 只，覆盖 Mag7、芯片、云、软件、AI 电力/基础设施。
// 不直接 Edge 拉 95 只，避免 Yahoo 限流和 Vercel 超时；后续可迁移到 GitHub Actions 日更快照。
const AI_BREADTH_TICKERS = [
  'MSFT', 'AAPL', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'AVGO',
  'TSM', 'ORCL', 'PLTR', 'ASML', 'CRM', 'AMD', 'BABA', 'GE',
  'IBM', 'QCOM', 'NOW', 'ADBE', 'APP', 'ISRG', 'PDD', 'NEE',
];
const MAG7_TICKERS = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA'];

function num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function nowISO() {
  return new Date().toISOString();
}

async function safeFetchJson(key, url, timeout = 5500) {
  try {
    const res = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'stock-distribution-radar/1.0 (+https://stock.zhixingshe.cc)',
      },
      signal: AbortSignal.timeout(timeout),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { key, ok: true, data: await res.json() };
  } catch (err) {
    return { key, ok: false, error: err?.message || String(err) };
  }
}

async function safeFetchText(key, url, timeout = 5500) {
  try {
    const res = await fetch(url, {
      headers: {
        'Accept': 'text/csv,text/plain,*/*',
        'User-Agent': 'stock-distribution-radar/2.0 (+https://stock.zhixingshe.cc)',
      },
      signal: AbortSignal.timeout(timeout),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { key, ok: true, text: await res.text() };
  } catch (err) {
    return { key, ok: false, error: err?.message || String(err) };
  }
}

function extractCloses(yfJson) {
  try {
    const result = yfJson?.chart?.result?.[0];
    const ts = result?.timestamp || [];
    const quote = result?.indicators?.quote?.[0] || {};
    const opens = quote.open || [];
    const highs = quote.high || [];
    const lows = quote.low || [];
    const closes = quote.close || [];
    const volumes = quote.volume || [];
    const out = [];
    for (let i = 0; i < ts.length; i++) {
      const c = num(closes[i]);
      const v = num(volumes[i]);
      if (c != null) out.push({
        t: ts[i],
        open: num(opens[i]),
        high: num(highs[i]),
        low: num(lows[i]),
        close: c,
        volume: v,
      });
    }
    return out;
  } catch {
    return [];
  }
}

function latestMeta(yfJson) {
  try {
    const meta = yfJson?.chart?.result?.[0]?.meta || {};
    return {
      regularMarketPrice: num(meta.regularMarketPrice),
      previousClose: num(meta.chartPreviousClose) ?? num(meta.previousClose),
      regularMarketTime: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toISOString() : null,
      symbol: meta.symbol,
      currency: meta.currency,
    };
  } catch {
    return {};
  }
}

/**
 * Distribution Days 自算：
 * - 近 25 个交易日内，当日 SPY 收跌且成交量 > 前一交易日成交量 => distribution day
 * - >= 5 偏派发；>= 4 警戒；<=2 健康
 */
function computeDistributionDays(bars, lookback = 25) {
  if (!Array.isArray(bars) || bars.length < lookback + 2) return null;
  const recent = bars.slice(-lookback - 1); // 多一根用于前一日对比
  let count = 0;
  const hits = [];
  for (let i = 1; i < recent.length; i++) {
    const prev = recent[i - 1];
    const cur = recent[i];
    if (cur.close < prev.close && cur.volume > prev.volume) {
      count++;
      hits.push({ t: cur.t, closeChangePct: ((cur.close / prev.close) - 1) * 100, volume: cur.volume });
    }
  }
  return { count, lookback, hits: hits.slice(-8) };
}

/**
 * VIX Term Structure proxy:
 * - 用 ^VIX (近月) 和 ^VIX3M (3 个月) 对比
 * - 正常 contango: VIX < VIX3M
 * - 倒挂 backwardation: VIX > VIX3M → 近期恐慌升高
 */
function computeVixTerm(vixBars, vix3mBars) {
  if (!vixBars?.length || !vix3mBars?.length) return null;
  const vix = vixBars[vixBars.length - 1].close;
  const vix3m = vix3mBars[vix3mBars.length - 1].close;
  if (!Number.isFinite(vix) || !Number.isFinite(vix3m) || vix3m === 0) return null;
  const ratio = vix / vix3m;
  return {
    vix,
    vix3m,
    ratio,
    inverted: ratio > 1.0,
  };
}

function avg(values) {
  const arr = values.filter(v => Number.isFinite(v));
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function movingAverage(bars, n, offset = 0) {
  const end = bars.length - offset;
  if (end < n) return null;
  return avg(bars.slice(end - n, end).map(b => b.close));
}

function computeDownUpVolumeRatio(bars, lookback = 50) {
  if (!Array.isArray(bars) || bars.length < lookback + 1) return null;
  const recent = bars.slice(-lookback - 1);
  let downVol = 0;
  let upVol = 0;
  for (let i = 1; i < recent.length; i++) {
    const prev = recent[i - 1];
    const cur = recent[i];
    if (!Number.isFinite(cur.volume)) continue;
    if (cur.close < prev.close) downVol += cur.volume;
    else if (cur.close > prev.close) upVol += cur.volume;
  }
  if (!upVol) return null;
  return { ratio: downVol / upVol, downVol, upVol, lookback };
}


function computeRelativeStrength(numeratorBars, denominatorBars, lookback = 60) {
  if (!Array.isArray(numeratorBars) || !Array.isArray(denominatorBars)) return null;
  const len = Math.min(numeratorBars.length, denominatorBars.length);
  if (len < lookback + 1) return null;
  const nRecent = numeratorBars.slice(-lookback - 1);
  const dRecent = denominatorBars.slice(-lookback - 1);
  const ratios = [];
  for (let i = 0; i < nRecent.length; i++) {
    const n = nRecent[i]?.close;
    const d = dRecent[i]?.close;
    if (Number.isFinite(n) && Number.isFinite(d) && d !== 0) ratios.push(n / d);
  }
  if (ratios.length < lookback * 0.8) return null;
  const first = ratios[0];
  const last = ratios[ratios.length - 1];
  const changePct = (last / first - 1) * 100;
  const slope = changePct / lookback;
  return {
    ratio: last,
    changePct,
    slope,
    trend: changePct <= -3 ? 'weakening' : changePct >= 3 ? 'improving' : 'flat',
  };
}

function rsStatus(rs, invert = false) {
  if (!rs) return 'pending';
  if (!invert) {
    if (rs.changePct <= -3) return 'distribution';
    if (rs.changePct >= 3) return 'accumulation';
    return 'neutral';
  }
  if (rs.changePct >= 3) return 'distribution';
  if (rs.changePct <= -3) return 'accumulation';
  return 'neutral';
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseCboeDailyMarketStatsHtml(text) {
  if (!text || typeof text !== 'string') return null;

  // Cboe 新页面是 Next.js/RSC HTML，数据嵌在 self.__next_f payload 中，形态类似：
  // \"ratios\":[{\"name\":\"TOTAL PUT/CALL RATIO\",\"value\":\"0.67\"}, ...]
  // 先把转义字符串还原成普通 JSON 片段，再用窄正则提取目标字段；不 eval，不执行页面脚本。
  const normalized = text
    .replace(/\\u0026/g, '&')
    .replace(/\\u003c/g, '<')
    .replace(/\\u003e/g, '>')
    .replace(/\\"/g, '"');

  const pickRatio = (label) => {
    const re = new RegExp(`"name"\\s*:\\s*"${escapeRegex(label)}"\\s*,\\s*"value"\\s*:\\s*"([0-9.]+)"`, 'i');
    const match = normalized.match(re);
    return match ? num(match[1]) : null;
  };

  const total = pickRatio('TOTAL PUT/CALL RATIO');
  const index = pickRatio('INDEX PUT/CALL RATIO');
  const etp = pickRatio('EXCHANGE TRADED PRODUCTS PUT/CALL RATIO');
  const equity = pickRatio('EQUITY PUT/CALL RATIO');
  const vix = pickRatio('CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO');
  const spx = pickRatio('SPX + SPXW PUT/CALL RATIO');
  const selectedDate = normalized.match(/"selectedDate"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"/i)?.[1]
    || normalized.match(/"prevTradingDay"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"/i)?.[1]
    || null;

  if (total == null && equity == null) return null;
  return {
    asOf: selectedDate,
    total,
    index,
    etp,
    equity,
    vix,
    spx,
    source: 'cboe_next_payload',
  };
}

function computeSectorRotation(sectorBars, spyBars) {
  const sectors = [
    ['XLK', '科技'], ['XLF', '金融'], ['XLE', '能源'], ['XLV', '医疗'], ['XLY', '可选消费'],
    ['XLP', '必需消费'], ['XLI', '工业'], ['XLB', '材料'], ['XLU', '公用事业'], ['XLRE', '地产'], ['XLC', '通信'],
  ];
  const ranking = sectors.map(([symbol, name]) => {
    const rs = computeRelativeStrength(sectorBars[symbol.toLowerCase()], spyBars, 60);
    return rs ? { symbol, name, changePct: Number(rs.changePct.toFixed(2)), trend: rs.trend } : null;
  }).filter(Boolean).sort((a, b) => b.changePct - a.changePct);
  if (ranking.length < 6) return null;
  const defensive = ['XLU', 'XLP', 'XLV'];
  const growth = ['XLK', 'XLY', 'XLC'];
  const defensiveAvg = avg(ranking.filter(r => defensive.includes(r.symbol)).map(r => r.changePct));
  const growthAvg = avg(ranking.filter(r => growth.includes(r.symbol)).map(r => r.changePct));
  const spread = defensiveAvg != null && growthAvg != null ? defensiveAvg - growthAvg : null;
  return { ranking, defensiveAvg, growthAvg, spread };
}

function analyzeBreadthSample(symbolBars, tickers) {
  const rows = [];
  for (const symbol of tickers) {
    const bars = symbolBars[symbol] || [];
    if (!Array.isArray(bars) || bars.length < 210) continue;
    const last = bars[bars.length - 1];
    const ma20 = movingAverage(bars, 20);
    const ma50 = movingAverage(bars, 50);
    const ma200 = movingAverage(bars, 200);
    const highs = bars.map(b => Number.isFinite(b.high) ? b.high : b.close).filter(Number.isFinite);
    const lows = bars.map(b => Number.isFinite(b.low) ? b.low : b.close).filter(Number.isFinite);
    const high52w = highs.length ? Math.max(...highs) : null;
    const low52w = lows.length ? Math.min(...lows) : null;
    if (!Number.isFinite(last?.close) || !Number.isFinite(ma50) || !Number.isFinite(ma200)) continue;
    rows.push({
      symbol,
      close: last.close,
      above20: Number.isFinite(ma20) ? last.close >= ma20 : null,
      above50: last.close >= ma50,
      above200: last.close >= ma200,
      nearHigh: high52w ? last.close >= high52w * 0.95 : null,
      nearLow: low52w ? last.close <= low52w * 1.10 : null,
      updatedAt: new Date(last.t * 1000).toISOString(),
    });
  }
  if (!rows.length) return null;
  const pct = (key) => Number(((rows.filter(r => r[key]).length / rows.length) * 100).toFixed(1));
  const above50 = pct('above50');
  const above200 = pct('above200');
  const nearHigh = pct('nearHigh');
  const nearLow = pct('nearLow');
  const leaders = rows.filter(r => r.above50 && r.nearHigh).map(r => r.symbol).slice(0, 8);
  const laggards = rows.filter(r => !r.above50 || r.nearLow).map(r => r.symbol).slice(0, 8);
  const updatedAt = rows.map(r => r.updatedAt).sort().slice(-1)[0] || null;
  return {
    sampleSize: rows.length,
    requested: tickers.length,
    coveragePct: Number((rows.length / tickers.length * 100).toFixed(1)),
    above20Pct: pct('above20'),
    above50Pct: above50,
    above200Pct: above200,
    nearHighPct: nearHigh,
    nearLowPct: nearLow,
    leaders,
    laggards,
    updatedAt,
    rows,
  };
}

function breadthStatus(breadth) {
  if (!breadth || breadth.coveragePct < 70) return 'pending';
  if (breadth.above50Pct < 45 || breadth.above200Pct < 50 || breadth.nearLowPct >= 25) return 'distribution';
  if (breadth.above50Pct >= 70 && breadth.above200Pct >= 70 && breadth.nearHighPct >= 35) return 'accumulation';
  return 'neutral';
}

function breadthValue(breadth) {
  if (!breadth) return null;
  return `MA50上方 ${breadth.above50Pct}% · MA200上方 ${breadth.above200Pct}% · 近52周高 ${breadth.nearHighPct}%`;
}

function breadthExtras(breadth) {
  if (!breadth) return null;
  return {
    sampleSize: breadth.sampleSize,
    requested: breadth.requested,
    coveragePct: breadth.coveragePct,
    above20Pct: breadth.above20Pct,
    above50Pct: breadth.above50Pct,
    above200Pct: breadth.above200Pct,
    nearHighPct: breadth.nearHighPct,
    nearLowPct: breadth.nearLowPct,
    leaders: breadth.leaders,
    laggards: breadth.laggards,
  };
}

function cachedBreadthExtras(snapshot, universe) {
  if (!universe) return null;
  return {
    snapshotGeneratedAt: snapshot?.generatedAt || null,
    sourceUpdatedAt: universe.sourceUpdatedAt || null,
    requested: universe.requested,
    sampleSize: universe.sampleSize,
    coveragePct: universe.coveragePct,
    above20Pct: universe.above20Pct,
    above50Pct: universe.above50Pct,
    above200Pct: universe.above200Pct,
    nearHighPct: universe.nearHighPct,
    newHighPct: universe.newHighPct,
    nearLowPct: universe.nearLowPct,
    newLowPct: universe.newLowPct,
    advancePct: universe.advancePct,
    declinePct: universe.declinePct,
    counts: universe.counts,
    leaders: universe.leaders,
    laggards: universe.laggards,
  };
}

function cachedBreadthMetric(snapshot, key, id, name, purpose) {
  const universe = snapshot?.universes?.[key];
  const usable = universe && universe.coveragePct >= 70;
  return metric({
    id,
    name,
    purpose,
    value: usable ? universe.value : null,
    status: usable ? universe.status : 'pending',
    threshold: '覆盖率 ≥70% 才参与判断；MA50上方 <45% 或 MA200上方 <50% 或新低占比 ≥10% 偏派发；MA50/MA200均 ≥70% 且新高占比 ≥8% 偏承接扩散',
    sourceName: 'GitHub Actions 日更宽度快照',
    sourceUrl: universe?.source || '/data/snapshots/market-breadth-latest.json',
    frequency: '日更（GitHub Actions，美股收盘后；API 读取静态 JSON）',
    updatedAt: universe?.updatedAt || snapshot?.generatedAt || null,
    dataStatus: usable ? 'Cached' : 'data_unavailable',
    logic: '用成分股日线自算站上 MA20/MA50/MA200、52周新高/新低、上涨/下跌家数。宽度恶化说明上涨集中或底层股票撤退；宽度扩散说明承接改善，但不能单独证明机构真实交易。',
    caseStudy: '2000、2007、2021 多次顶部前，指数仍强但成分股宽度先走弱；反弹初期则常见 MA50/MA200 上方比例逐步扩散。',
    limitations: snapshot?.limitations || '免费数据源可能限流；Nasdaq-100 用 QQQ 持仓作为实战代理；宽度指标是市场结构证据，不是 13F/Form 4/期权流等机构证据。',
    extras: cachedBreadthExtras(snapshot, universe),
  });
}

function statusFromScore(score) {
  if (score >= 70) return 'distribution';
  if (score >= 50) return 'watch';
  if (score < 30) return 'healthy';
  return 'neutral';
}

function statusLabel(status) {
  if (status === 'distribution') return '明显量价派发';
  if (status === 'watch') return '量价派发嫌疑';
  if (status === 'healthy') return '低派发风险';
  return '中性观察';
}

function analyzeStockBars(symbol, bars, meta = {}) {
  if (!Array.isArray(bars) || bars.length < 60) return null;
  const last = bars[bars.length - 1];
  const prev = bars[bars.length - 2];
  const dd = computeDistributionDays(bars, 25);
  const du = computeDownUpVolumeRatio(bars, 50);
  const ma20 = movingAverage(bars, 20);
  const ma50 = movingAverage(bars, 50);
  const ma20Prev5 = movingAverage(bars, 20, 5);
  const highs = bars.map(b => Number.isFinite(b.high) ? b.high : b.close).filter(Number.isFinite);
  const high52w = highs.length ? Math.max(...highs) : null;
  const drawdownFrom52wHigh = high52w ? (high52w - last.close) / high52w : null;
  const changePct = prev?.close ? (last.close / prev.close - 1) * 100 : null;

  let ddScore = 0;
  if (dd?.count >= 6) ddScore = 35;
  else if (dd?.count >= 4) ddScore = 25;
  else if (dd?.count >= 2) ddScore = 12;

  let duScore = 0;
  if (du?.ratio >= 1.3) duScore = 30;
  else if (du?.ratio >= 1.0) duScore = 18;
  else if (du?.ratio >= 0.8) duScore = 8;

  let maScore = 0;
  let maStructure = 'unknown';
  if (Number.isFinite(ma20) && Number.isFinite(ma50)) {
    if (last.close < ma50 && ma20 < ma50) { maScore = 20; maStructure = '破 MA50 且 MA20 < MA50'; }
    else if (last.close < ma20) { maScore = 12; maStructure = '价格低于 MA20，短期走弱'; }
    else if (last.close > ma20 && ma20 > ma50) { maScore = 0; maStructure = '价格 > MA20 > MA50，趋势健康'; }
    else { maScore = 7; maStructure = '均线结构中性'; }
    if (Number.isFinite(ma20Prev5) && ma20 < ma20Prev5) maScore = Math.min(20, maScore + 4);
  }

  let dd52Score = 0;
  if (drawdownFrom52wHigh != null) {
    if (drawdownFrom52wHigh >= 0.15) dd52Score = 15;
    else if (drawdownFrom52wHigh >= 0.08) dd52Score = 11;
    else if (drawdownFrom52wHigh >= 0.03) dd52Score = 5;
  }

  const score = Math.max(0, Math.min(100, Math.round(ddScore + duScore + maScore + dd52Score)));
  const status = statusFromScore(score);

  return {
    symbol: String(symbol || meta.symbol || '').toUpperCase(),
    generatedAt: nowISO(),
    updatedAt: meta.regularMarketTime || new Date(last.t * 1000).toISOString(),
    sourceName: 'Yahoo Finance chart',
    sourceUrl: `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/history`,
    frequency: '日更（美股交易日）',
    dataStatus: 'Live',
    status,
    verdict: statusLabel(status),
    score,
    price: Number(last.close.toFixed(2)),
    changePct: changePct != null ? Number(changePct.toFixed(2)) : null,
    metrics: [
      {
        id: 'distribution_days_25d',
        name: '近25日放量下跌日',
        value: dd ? `${dd.count}/25` : '无数据',
        status: dd?.count >= 6 ? 'distribution' : dd?.count >= 4 ? 'watch' : dd?.count <= 1 ? 'healthy' : 'neutral',
        logic: '单日收跌且成交量高于前一日，记为放量下跌日；连续聚集说明市场可能出现大资金分发，但不能单独证明机构派发。',
        threshold: '≥6 强派发；4-5 派发嫌疑；0-1 低派发风险',
      },
      {
        id: 'down_up_volume_ratio_50d',
        name: '50日下跌量能/上涨量能',
        value: du ? Number(du.ratio.toFixed(2)) : '无数据',
        status: du?.ratio >= 1.3 ? 'distribution' : du?.ratio >= 1.0 ? 'watch' : du?.ratio < 0.8 ? 'healthy' : 'neutral',
        logic: '下跌日总成交量明显大于上涨日，说明卖盘主导；反之说明承接更强。它是量价证据，不是机构持仓证据。',
        threshold: '≥1.30 派发；1.00-1.30 偏弱；<0.80 低派发风险',
      },
      {
        id: 'ma_structure',
        name: 'MA20 / MA50 结构',
        value: maStructure,
        status: maScore >= 16 ? 'distribution' : maScore >= 10 ? 'watch' : maScore <= 3 ? 'healthy' : 'neutral',
        logic: '价格跌破短中期均线、MA20 下行或跌破 MA50，通常意味着承接变弱；均线健康只能说明量价趋势尚可，不能定义机构增持。',
        threshold: '价格 < MA50 且 MA20 < MA50 为趋势破位',
      },
      {
        id: 'drawdown_from_52w_high',
        name: '距52周高点回撤',
        value: drawdownFrom52wHigh != null ? `${(drawdownFrom52wHigh * 100).toFixed(1)}%` : '无数据',
        status: drawdownFrom52wHigh >= 0.15 ? 'distribution' : drawdownFrom52wHigh >= 0.08 ? 'watch' : drawdownFrom52wHigh < 0.03 ? 'healthy' : 'neutral',
        logic: '强势龙头通常贴近新高；从高位回撤扩大且伴随放量下跌，更像派发完成后的结果。贴近新高只代表强势，不代表机构增持。',
        threshold: '≥15% 深度回撤；8-15% 警示；<3% 强势/低派发风险',
      },
    ],
    raw: {
      distributionDays: dd,
      downUpVolumeRatio: du,
      ma20: ma20 != null ? Number(ma20.toFixed(2)) : null,
      ma50: ma50 != null ? Number(ma50.toFixed(2)) : null,
      high52w: high52w != null ? Number(high52w.toFixed(2)) : null,
      drawdownFrom52wHigh,
    },
    notes: [
      '这是免费行情可做的量价派发判断，不含 Form 4、13F、暗池、期权大单。',
      '低分只代表“量价派发风险低/趋势承接尚可”，不能定义为机构增持。',
      '若要判断机构增持或机构派发，必须继续接入 13F、Form 4、OpenInsider cluster buy/sell、期权/暗池等证据。',
    ],
  };
}

/** 指标卡元数据工厂 */
function metric({
  id, name, purpose, value, status, threshold,
  sourceName, sourceUrl, frequency, updatedAt, dataStatus,
  logic, caseStudy, limitations, extras,
}) {
  return {
    id,
    name,
    purpose,
    value: value ?? null,
    status: status || 'pending', // distribution | neutral | accumulation | pending
    threshold: threshold || '',
    sourceName,
    sourceUrl,
    frequency,
    updatedAt: updatedAt || null,
    dataStatus: dataStatus || 'Pending', // Live | Cached | Manual | Pending | data_unavailable
    logic: logic || '',
    caseStudy: caseStudy || '',
    limitations: limitations || '',
    extras: extras || null,
  };
}

/** 计算综合状态 */
function applyManualIndicatorOverrides(metrics, manualConfig) {
  const indicators = manualConfig?.indicators || {};
  const validStatuses = new Set(['distribution', 'neutral', 'accumulation', 'pending']);
  return metrics.map(metricItem => {
    const override = indicators[metricItem.id];
    if (!override?.enabled) return metricItem;

    const status = validStatuses.has(override.status) ? override.status : metricItem.status;
    const dataStatus = ['Manual', 'Cached', 'Live', 'Pending', 'data_unavailable'].includes(override.dataStatus)
      ? override.dataStatus
      : 'Manual';
    const extras = {
      ...(metricItem.extras || {}),
      manualOverride: {
        enabled: true,
        schemaVersion: manualConfig?.schemaVersion || 1,
        updatedAt: override.updatedAt || manualConfig?.updatedAt || null,
        note: override.manualNote || null,
      },
    };

    return metric({
      ...metricItem,
      value: override.value ?? metricItem.value,
      status,
      updatedAt: override.updatedAt || manualConfig?.updatedAt || metricItem.updatedAt,
      dataStatus,
      sourceUrl: override.sourceUrl || metricItem.sourceUrl,
      logic: override.logic || metricItem.logic,
      limitations: override.limitations || `${metricItem.limitations || ''} 手工开启值来自 data/manual-stock-indicators.json；请核对原始来源与日期。`.trim(),
      extras,
    });
  });
}

function aggregate(metrics) {
  // 对 Live / Cached / Manual 且有明确 status 的指标做投票；Pending 不参与评分。
  const considered = metrics.filter(m =>
    ['Live', 'Cached', 'Manual'].includes(m.dataStatus) && ['distribution', 'neutral', 'accumulation'].includes(m.status)
  );
  const distHits = metrics.filter(m => m.status === 'distribution').length;
  const accHits = metrics.filter(m => m.status === 'accumulation').length;

  // 简单打分：每个 distribution +12, neutral 0, accumulation -12，基线 50
  let score = 50;
  for (const m of considered) {
    if (m.status === 'distribution') score += 12;
    else if (m.status === 'accumulation') score -= 12;
  }
  score = Math.max(0, Math.min(100, score));

  let overall;
  if (score >= 65) overall = 'distribution';
  else if (score <= 35) overall = 'accumulation';
  else overall = 'neutral';

  const total = metrics.length;
  const live = metrics.filter(m => m.dataStatus === 'Live').length;
  const cached = metrics.filter(m => m.dataStatus === 'Cached').length;
  const manual = metrics.filter(m => m.dataStatus === 'Manual').length;
  const pending = metrics.filter(m => ['Pending', 'data_unavailable'].includes(m.dataStatus)).length;

  return {
    overall,
    score,
    distributionHits: distHits,
    accumulationHits: accHits,
    dataQuality: { total, live, cached, manual, pending },
  };
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: JSON_HEADERS });
  if (req.method !== 'GET') {
    return new Response(JSON.stringify({ error: 'Method Not Allowed' }), { status: 405, headers: JSON_HEADERS });
  }

  const url = new URL(req.url);
  const includePrices = url.searchParams.get('prices') !== '0';
  const symbolParam = (url.searchParams.get('symbol') || '').trim().toUpperCase();

  if (symbolParam) {
    if (!/^[A-Z0-9.-]{1,12}$/.test(symbolParam)) {
      return new Response(JSON.stringify({ error: 'Invalid symbol' }), { status: 400, headers: JSON_HEADERS });
    }
    const r = await safeFetchJson(symbolParam, YF_CHART(symbolParam, '1y', '1d'), 6500);
    if (!r.ok) {
      return new Response(JSON.stringify({ error: 'symbol_fetch_failed', symbol: symbolParam, detail: r.error }), { status: 502, headers: JSON_HEADERS });
    }
    const bars = extractCloses(r.data);
    const meta = latestMeta(r.data);
    const analysis = analyzeStockBars(symbolParam, bars, meta);
    if (!analysis) {
      return new Response(JSON.stringify({ error: 'insufficient_data', symbol: symbolParam }), { status: 422, headers: JSON_HEADERS });
    }
    return new Response(JSON.stringify(analysis, null, 2), { status: 200, headers: JSON_HEADERS });
  }

  // 大盘指标需要的 Yahoo 数据。V2 第一阶段坚持免费 + 自动化：指数、ETF、板块 ETF 全走 Yahoo chart。
  const endpoints = {
    spy: YF_CHART('SPY', '6mo', '1d'),
    qqq: YF_CHART('QQQ', '6mo', '1d'),
    iwm: YF_CHART('IWM', '6mo', '1d'),
    rsp: YF_CHART('RSP', '6mo', '1d'),
    hyg: YF_CHART('HYG', '6mo', '1d'),
    vix: YF_CHART('^VIX', '1mo', '1d'),     // Yahoo chart 会在 YF_CHART 内统一 encode
    vix3m: YF_CHART('^VIX3M', '1mo', '1d'),
    skew: YF_CHART('^SKEW', '1mo', '1d'),
    gspc: YF_CHART('^GSPC', '1mo', '1d'),
    xlk: YF_CHART('XLK', '6mo', '1d'),
    xlf: YF_CHART('XLF', '6mo', '1d'),
    xle: YF_CHART('XLE', '6mo', '1d'),
    xlv: YF_CHART('XLV', '6mo', '1d'),
    xly: YF_CHART('XLY', '6mo', '1d'),
    xlp: YF_CHART('XLP', '6mo', '1d'),
    xli: YF_CHART('XLI', '6mo', '1d'),
    xlb: YF_CHART('XLB', '6mo', '1d'),
    xlu: YF_CHART('XLU', '6mo', '1d'),
    xlre: YF_CHART('XLRE', '6mo', '1d'),
    xlc: YF_CHART('XLC', '6mo', '1d'),
  };
  for (const symbol of AI_BREADTH_TICKERS) {
    endpoints[`ai_${symbol}`] = YF_CHART(symbol, '1y', '1d');
  }

  const notes = [];
  const manualConfigUrl = new URL('/data/manual-stock-indicators.json', url.origin).toString();
  const breadthSnapshotUrl = new URL('/data/snapshots/market-breadth-latest.json', url.origin).toString();
  const cboeDailyStatsUrl = 'https://www.cboe.com/markets/us/options/market-statistics/daily/';
  const [jsonResults, manualConfigResult, breadthSnapshotResult, cboePcResult] = await Promise.all([
    Promise.all(Object.entries(endpoints).map(([k, u]) => safeFetchJson(k, u, 5000))),
    safeFetchJson('manual_indicators', manualConfigUrl, 2500),
    safeFetchJson('market_breadth_snapshot', breadthSnapshotUrl, 2500),
    safeFetchText('cboe_pc', cboeDailyStatsUrl, 5000),
  ]);
  const data = {};
  for (const r of jsonResults) {
    if (r.ok) data[r.key] = r.data;
    else notes.push(`${r.key} fetch failed: ${r.error}`);
  }
  if (!cboePcResult.ok) notes.push(`cboe_pc unavailable: ${cboePcResult.error}`);
  const manualConfig = manualConfigResult.ok ? manualConfigResult.data : null;
  if (!manualConfigResult.ok) notes.push(`manual_indicators unavailable: ${manualConfigResult.error}`);
  const breadthSnapshot = breadthSnapshotResult.ok ? breadthSnapshotResult.data : null;
  if (!breadthSnapshotResult.ok) notes.push(`market_breadth_snapshot unavailable: ${breadthSnapshotResult.error}`);

  const spyBars = extractCloses(data.spy || {});
  const qqqBars = extractCloses(data.qqq || {});
  const iwmBars = extractCloses(data.iwm || {});
  const rspBars = extractCloses(data.rsp || {});
  const hygBars = extractCloses(data.hyg || {});
  const vixBars = extractCloses(data.vix || {});
  const vix3mBars = extractCloses(data.vix3m || {});
  const skewBars = extractCloses(data.skew || {});
  const sectorBars = Object.fromEntries(['xlk','xlf','xle','xlv','xly','xlp','xli','xlb','xlu','xlre','xlc'].map(k => [k, extractCloses(data[k] || {})]));
  const aiSymbolBars = Object.fromEntries(AI_BREADTH_TICKERS.map(symbol => [symbol, extractCloses(data[`ai_${symbol}`] || {})]));
  const cboePc = cboePcResult.ok ? parseCboeDailyMarketStatsHtml(cboePcResult.text) : null;
  if (cboePcResult.ok && !cboePc) notes.push('cboe_pc parse failed: CBOE page fetched but ratio payload was not found');

  const spyMeta = latestMeta(data.spy || {});
  const qqqMeta = latestMeta(data.qqq || {});
  const vixMeta = latestMeta(data.vix || {});
  const skewMeta = latestMeta(data.skew || {});

  const metrics = [];

  // 1) BofA FMS Cash Level（月更，无免费 JSON；手工更新）
  metrics.push(metric({
    id: 'bofa_fms_cash',
    name: 'BofA FMS Cash Level',
    purpose: '全球基金经理月度问卷的现金仓位，反向情绪指标',
    value: null,
    status: 'pending',
    threshold: '< 4% 触发 sell signal；> 5% 偏防御/承接改善',
    sourceName: 'BofA Global Fund Manager Survey',
    sourceUrl: 'https://business.bofa.com/en-us/content/global-research.html',
    frequency: '月更（每月中旬发布）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '机构现金仓位极低 → 子弹打完，易成为顶部反向信号；现金仓位抬升 → 防御/等待更好承接位置。',
    caseStudy: '2018/01, 2021/11 现金仓位跌破 4% 后均伴随中期顶部。',
    limitations: '月度样本，时效差；BofA 报告非公开 JSON 接口，需人工/第三方转载同步。',
  }));

  // 2) FINRA Margin Debt
  metrics.push(metric({
    id: 'finra_margin_debt',
    name: 'FINRA Margin Debt',
    purpose: '融资余额（杠杆敞口）月度变化',
    value: null,
    status: 'pending',
    threshold: '同比 > +25% 且创新高 偏派发；同比转负 偏去杠杆/承接改善',
    sourceName: 'FINRA Margin Statistics',
    sourceUrl: 'https://www.finra.org/investors/learn-to-invest/advanced-investing/margin-statistics',
    frequency: '月更（次月中下旬发布上月数据）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '融资买入余额 = 杠杆总规模；高位且同比快速扩张 → 散户/对冲基金杠杆拥挤，派发风险升高。',
    caseStudy: '2000/03, 2007/07, 2021/10 margin debt 同比峰值均早于或同步于大顶。',
    limitations: '月度数据，滞后 4-6 周；FINRA 只发 PDF/表格，需要抓取或人工录入。',
  }));

  // 3) AAII Bull-Bear Spread
  metrics.push(metric({
    id: 'aaii_bull_bear',
    name: 'AAII Bull-Bear Spread',
    purpose: '美国散户投资者协会周度情绪调查的看多-看空差',
    value: null,
    status: 'pending',
    threshold: '> +30% 散户极度乐观 偏派发；< -20% 极度悲观 偏恐慌释放/承接改善',
    sourceName: 'AAII Investor Sentiment Survey',
    sourceUrl: 'https://www.aaii.com/sentimentsurvey',
    frequency: '周更（每周四公布）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '反向指标：散户看多过度 → 情绪拥挤，顶部风险；极度看空 → 常伴随阶段性底部。',
    caseStudy: '2024/01, 2025/07 bull-bear spread 连续多周 > 30% 后出现震荡/回调。',
    limitations: '样本量小、自愿填写，噪声大；需要在 AAII 官网手工同步或抓取。',
  }));

  // 4) CBOE Put/Call Ratio（V2：从官方 Daily Market Statistics HTML / Next payload 自动解析，失败则 Pending）
  {
    const equity = cboePc?.equity;
    const total = cboePc?.total;
    const value = equity != null ? `Equity ${equity.toFixed(2)}` + (total != null ? ` · Total ${total.toFixed(2)}` : '') : null;
    const status = equity == null ? 'pending' : equity < 0.55 ? 'distribution' : equity > 0.85 ? 'accumulation' : 'neutral';
    metrics.push(metric({
      id: 'cboe_equity_pc',
      name: 'CBOE Equity Put/Call Ratio',
      purpose: '个股期权 Put/Call 成交量比（不含指数），观察散户投机与保护性需求',
      value,
      status,
      threshold: '< 0.55 Call 投机偏热/派发风险；> 0.85 保护性需求偏强/恐慌释放；0.55-0.85 中性',
      sourceName: 'CBOE Daily Market Statistics',
      sourceUrl: cboeDailyStatsUrl,
      frequency: '日更（CBOE 收盘后发布；自动解析官网 HTML 中的 Next payload）',
      updatedAt: cboePc?.asOf || null,
      dataStatus: equity != null ? 'Live' : 'Pending',
      logic: 'Equity Put/Call 越低，说明个股 Call 投机越拥挤，常见于情绪过热阶段；越高说明保护性需求上升，常见于风险释放阶段。它是期权情绪证据，不等于机构真实派发。',
      caseStudy: '2021 年末 Equity P/C 低位徘徊，成长股投机拥挤；2022 年多个阶段 Equity P/C 升高后出现恐慌释放。',
      limitations: '依赖 CBOE 官网 HTML 中的 Next/RSC 数据片段，页面结构若改版会自动降级为 Pending；旧 CSV 在服务器/Edge 环境仍可能 403，不作为主来源。',
      extras: cboePc || null,
    }));
  }

  // 5) SKEW Index（Live via Yahoo ^SKEW）
  {
    const skew = skewBars.length ? skewBars[skewBars.length - 1].close : null;
    let status = 'neutral';
    if (skew == null) status = 'pending';
    else if (skew >= 150) status = 'distribution';
    else if (skew <= 125) status = 'accumulation';
    metrics.push(metric({
      id: 'cboe_skew',
      name: 'CBOE SKEW Index',
      purpose: '尾部风险指数，衡量 OTM put 相对 ATM 的溢价',
      value: skew != null ? Number(skew.toFixed(2)) : null,
      status,
      threshold: '> 150 机构买尾部保险（派发/对冲信号）；< 125 相对放松（风险偏好改善）',
      sourceName: 'CBOE SKEW (via Yahoo ^SKEW)',
      sourceUrl: 'https://finance.yahoo.com/quote/%5ESKEW/',
      frequency: '日更（交易日收盘后）',
      updatedAt: skewMeta.regularMarketTime || nowISO(),
      dataStatus: skew != null ? 'Live' : 'data_unavailable',
      logic: 'SKEW 越高 → 机构越愿意为黑天鹅付保费，通常与派发/分散出货同步；SKEW 偏低 → 市场不担心尾部风险。',
      caseStudy: '2018/01, 2021/11 SKEW 持续 > 150 之后出现显著回调。',
      limitations: 'SKEW 对方向性预测能力弱，仅反映尾部溢价；仅作派发/承接环境拼图之一。',
    }));
  }

  // 6) VIX Term Structure (^VIX vs ^VIX3M)
  {
    const term = computeVixTerm(vixBars, vix3mBars);
    let status = 'pending';
    let value = null;
    let updatedAt = vixMeta.regularMarketTime || null;
    let dataStatus = 'data_unavailable';
    if (term) {
      dataStatus = 'Live';
      value = `VIX ${term.vix.toFixed(2)} / VIX3M ${term.vix3m.toFixed(2)} · ratio ${term.ratio.toFixed(3)}`;
      if (term.ratio >= 1.0) status = 'distribution';
      else if (term.ratio <= 0.85) status = 'accumulation';
      else status = 'neutral';
    }
    metrics.push(metric({
      id: 'vix_term_structure',
      name: 'VIX Term Structure',
      purpose: 'VIX 近月 vs 3 个月期限结构，倒挂代表近期恐慌显著上升',
      value,
      status,
      threshold: 'VIX / VIX3M ≥ 1.00 倒挂 偏派发；≤ 0.85 深度 contango 偏承接环境改善',
      sourceName: 'Yahoo Finance ^VIX & ^VIX3M',
      sourceUrl: 'https://www.cboe.com/tradable_products/vix/',
      frequency: '日更',
      updatedAt,
      dataStatus,
      logic: '正常状态应为 contango（远月 > 近月）；倒挂说明市场对近期风险担忧陡升，常伴随调整或派发。',
      caseStudy: '2020/02, 2022/01, 2025/08 均在倒挂出现后一周内发生显著回调。',
      limitations: 'VIX3M 非官方 CBOE 直供，Yahoo 间或缺数；倒挂信号高风险时已经发生，择时价值有限。',
      extras: term || null,
    }));
  }

  // 7) NYSE A/D Line（无稳定免费接口，Pending）
  metrics.push(metric({
    id: 'nyse_ad_line',
    name: 'NYSE A/D Line',
    purpose: '纽交所涨跌家数累计线，衡量市场宽度',
    value: null,
    status: 'pending',
    threshold: '指数创新高但 A/D Line 未创新高 偏派发；同步创新高 偏宽度健康/承接扩散',
    sourceName: 'StockCharts / WSJ Market Data',
    sourceUrl: 'https://stockcharts.com/h-sc/ui?s=$NYAD',
    frequency: '日更',
    updatedAt: null,
    dataStatus: 'Pending',
    logic: '背离原理：少数大票抬指数，多数股票走弱 → A/D 未创新高 → 宽度恶化，派发风险升高。',
    caseStudy: '2021/11, 2015/05 均先出现 A/D 与指数背离后再现中期顶部。',
    limitations: 'Yahoo 不直接提供 A/D 累计序列；MVP 第二期通过 StockCharts 抓取或自算 NYSE 成份股日涨跌数。',
  }));

  // 8) NYSE New High/New Low
  metrics.push(metric({
    id: 'nyse_nh_nl',
    name: 'NYSE New High / New Low',
    purpose: '纽交所 52 周新高 vs 新低家数',
    value: null,
    status: 'pending',
    threshold: '指数新高但新高家数 < 100 且新低家数上升 偏派发；新低萎缩、新高扩张 偏宽度健康/承接扩散',
    sourceName: 'WSJ Market Data / StockCharts',
    sourceUrl: 'https://www.wsj.com/market-data/stocks/highsandlows',
    frequency: '日更',
    updatedAt: null,
    dataStatus: 'Pending',
    logic: '宽度指标：指数新高但创新高个股变少，代表涨幅集中在少数权重股；新低家数同时抬头是典型派发前兆。',
    caseStudy: '2000/03、2007/07 指数新高时新高家数反而持续走低。',
    limitations: '免费接口不稳定，MVP 第二期接 WSJ / StockCharts 爬虫或 FINRA 统计。',
  }));

  // 9) V2 第三刀：日更静态快照，全量 S&P500 / Nasdaq-100 宽度（API 优先读缓存 JSON，不在 Edge 实时拉 600 只）。
  metrics.push(cachedBreadthMetric(
    breadthSnapshot,
    'sp500',
    'sp500_breadth_snapshot',
    'S&P500 成分股真实宽度快照',
    '用 S&P500 成分股日线自算 MA50/MA200、52周新高/新低、上涨/下跌家数，替代只看 SPY/RSP 的代理判断'
  ));
  metrics.push(cachedBreadthMetric(
    breadthSnapshot,
    'nasdaq100',
    'nasdaq100_breadth_snapshot',
    'Nasdaq-100 / QQQ 持仓真实宽度快照',
    '用 QQQ 持仓作为 Nasdaq-100 实战代理，自算科技权重内部宽度，观察龙头行情是否扩散或恶化'
  ));

  // 10) V2 市场宽度代理：小盘/等权/信用相对 SPY。免费自动化，用来观察“指数强但底层股票先弱”。
  {
    const iwmRs = computeRelativeStrength(iwmBars, spyBars, 60);
    metrics.push(metric({
      id: 'smallcap_rs_iwm_spy',
      name: '小盘相对强弱 IWM/SPY',
      purpose: '观察次要股票/小盘股是否先于指数走弱，是“少数权重股撑指数”的重要代理',
      value: iwmRs ? `${iwmRs.changePct.toFixed(2)}% / 60D` : null,
      status: rsStatus(iwmRs),
      threshold: '60日相对 SPY ≤ -3% 偏派发；≥ +3% 承接改善；中间为中性',
      sourceName: 'Yahoo Finance IWM/SPY 自算',
      sourceUrl: 'https://finance.yahoo.com/quote/IWM/history',
      frequency: '日更（美股交易日）',
      updatedAt: spyMeta.regularMarketTime || nowISO(),
      dataStatus: iwmRs ? 'Live' : 'data_unavailable',
      logic: '派发期常见现象不是指数立刻下跌，而是小盘股、次级股票先跌，指数由少数大权重继续托住。IWM/SPY 下行说明风险偏好正在从底层股票撤退。',
      caseStudy: '2021 年下半年，很多成长/小盘股先走熊，指数仍由大票维持，随后宽度恶化扩散。',
      limitations: 'IWM 是 ETF 代理，不等于全市场 A/D Line；只代表小盘相对强弱，不能单独证明机构派发。',
      extras: iwmRs || null,
    }));
  }

  {
    const rspRs = computeRelativeStrength(rspBars, spyBars, 60);
    metrics.push(metric({
      id: 'equal_weight_rs_rsp_spy',
      name: '等权相对强弱 RSP/SPY',
      purpose: '观察 S&P500 普通成分股是否跑输市值权重指数，识别“龙头撑指数”',
      value: rspRs ? `${rspRs.changePct.toFixed(2)}% / 60D` : null,
      status: rsStatus(rspRs),
      threshold: '60日相对 SPY ≤ -2% 宽度走弱；≥ +2% 承接扩散；中间为中性',
      sourceName: 'Yahoo Finance RSP/SPY 自算',
      sourceUrl: 'https://finance.yahoo.com/quote/RSP/history',
      frequency: '日更（美股交易日）',
      updatedAt: spyMeta.regularMarketTime || nowISO(),
      dataStatus: rspRs ? 'Live' : 'data_unavailable',
      logic: 'RSP 等权代表“普通 S&P500 成分股”的平均表现。若 SPY 强而 RSP/SPY 走弱，说明上涨集中在少数权重股，宽度恶化。',
      caseStudy: '大型科技权重集中上涨时，RSP/SPY 经常提前暴露市场内部走弱。',
      limitations: 'RSP 仍是 S&P500 内部代理，不覆盖小盘/中盘/海外市场；需要与 IWM/SPY、A/D、NH/NL 共振看。',
      extras: rspRs || null,
    }));
  }

  {
    const hygRs = computeRelativeStrength(hygBars, spyBars, 60);
    metrics.push(metric({
      id: 'credit_risk_hyg_spy',
      name: '信用风险偏好 HYG/SPY',
      purpose: '观察高收益债是否相对股票走弱；信用市场走弱常早于权益风险释放',
      value: hygRs ? `${hygRs.changePct.toFixed(2)}% / 60D` : null,
      status: rsStatus(hygRs),
      threshold: '60日相对 SPY ≤ -3% 信用风险走弱；≥ +3% 风险偏好改善',
      sourceName: 'Yahoo Finance HYG/SPY 自算',
      sourceUrl: 'https://finance.yahoo.com/quote/HYG/history',
      frequency: '日更（美股交易日）',
      updatedAt: spyMeta.regularMarketTime || nowISO(),
      dataStatus: hygRs ? 'Live' : 'data_unavailable',
      logic: '高收益债对流动性和信用风险更敏感。HYG 相对 SPY 持续走弱，说明风险资产内部可能开始撤退。',
      caseStudy: '信用利差走阔、高收益债走弱，常与权益市场调整或风险偏好下降同步出现。',
      limitations: 'HYG 受利率与信用双重影响，不是纯机构派发指标；需和宽度、VIX、派发日一起看。',
      extras: hygRs || null,
    }));
  }

  // 10) V2 板块轮动：进攻/防御切换。
  {
    const sector = computeSectorRotation(sectorBars, spyBars);
    const leaders = sector?.ranking?.slice(0, 3).map(r => `${r.symbol} ${r.changePct.toFixed(1)}%`).join(' / ');
    const laggards = sector?.ranking?.slice(-3).map(r => `${r.symbol} ${r.changePct.toFixed(1)}%`).join(' / ');
    const status = !sector ? 'pending' : sector.spread != null && sector.spread >= 3 ? 'distribution' : sector.spread != null && sector.spread <= -3 ? 'accumulation' : 'neutral';
    metrics.push(metric({
      id: 'sector_rotation_rs',
      name: '板块相对强弱：防御 vs 进攻',
      purpose: '观察资金是否从科技/消费/通信等进攻板块切向公用事业/必需消费/医疗等防御板块',
      value: sector ? `领涨 ${leaders} · 落后 ${laggards}` : null,
      status,
      threshold: '防御板块 60日相对表现 - 进攻板块 ≥ +3% 偏派发；≤ -3% 风险偏好改善',
      sourceName: 'Yahoo Finance 11 SPDR Sector ETFs 自算',
      sourceUrl: 'https://www.sectorspdrs.com/',
      frequency: '日更（美股交易日）',
      updatedAt: spyMeta.regularMarketTime || nowISO(),
      dataStatus: sector ? 'Live' : 'data_unavailable',
      logic: '派发期常出现防御板块开始跑赢、进攻板块开始掉队。它说明资金风险偏好下降，但不是单票机构交易证据。',
      caseStudy: '市场见顶或调整前，XLU/XLP/XLV 相对走强、XLK/XLY 等高 beta 板块相对走弱较常见。',
      limitations: '板块 ETF 受成分股权重影响，防御领先也可能来自利率变化；需与宽度和派发日共同判断。',
      extras: sector || null,
    }));
  }

  // 11) Distribution Days（Live 自算：SPY）
  {
    const ddSpy = computeDistributionDays(spyBars, 25);
    const ddQqq = computeDistributionDays(qqqBars, 25);
    let status = 'pending';
    let value = null;
    let dataStatus = 'data_unavailable';
    let updatedAt = spyMeta.regularMarketTime || null;
    if (ddSpy) {
      dataStatus = 'Live';
      value = `SPY ${ddSpy.count}/25` + (ddQqq ? ` · QQQ ${ddQqq.count}/25` : '');
      const maxCount = Math.max(ddSpy.count, ddQqq?.count || 0);
      if (maxCount >= 5) status = 'distribution';
      else if (maxCount <= 2) status = 'accumulation';
      else status = 'neutral';
    }
    metrics.push(metric({
      id: 'distribution_days',
      name: 'Distribution Days (SPY/QQQ)',
      purpose: 'IBD 经典方法：放量下跌日统计，衡量机构分发力度',
      value,
      status,
      threshold: '近 25 交易日 ≥ 5 派发日 → 偏派发；≤ 2 → 派发压力低/承接尚可',
      sourceName: 'Yahoo Finance SPY/QQQ 自算',
      sourceUrl: 'https://finance.yahoo.com/quote/SPY/history',
      frequency: '日更（美股收盘后）',
      updatedAt,
      dataStatus,
      logic: '当日收跌且成交量 > 前一交易日 → 记为 distribution day。机构大规模分发通常形成连续派发日聚集。',
      caseStudy: '2022/01 SPY 近 25 日出现 6 个派发日，随后开启熊市。',
      limitations: 'ETF 成交量受当日做市与再平衡影响，易有噪声；阈值为经验值非硬约束。',
      extras: { spy: ddSpy, qqq: ddQqq },
    }));
  }

  // 12) V2 第二刀：AI 主题核心样本真实宽度（MA50 / MA200 / 52周高低）。
  {
    const aiBreadth = analyzeBreadthSample(aiSymbolBars, AI_BREADTH_TICKERS);
    metrics.push(metric({
      id: 'ai_core_breadth_ma',
      name: 'AI核心样本宽度：MA50 / MA200',
      purpose: '观察 AI 主题内部是否只有少数龙头撑住，还是大部分核心票仍在趋势线上方',
      value: breadthValue(aiBreadth),
      status: breadthStatus(aiBreadth),
      threshold: '样本覆盖 ≥70% 才参与判断；MA50上方 <45% 或 MA200上方 <50% 偏派发；二者 ≥70% 且近52周高 ≥35% 承接扩散',
      sourceName: 'Yahoo Finance 24只AI核心样本自算',
      sourceUrl: 'https://finance.yahoo.com/lookup',
      frequency: '日更（美股交易日；Edge 实时抓取，失败则降级）',
      updatedAt: aiBreadth?.updatedAt || null,
      dataStatus: aiBreadth && aiBreadth.coveragePct >= 70 ? 'Live' : 'data_unavailable',
      logic: '真正的主题强势不能只看 NVDA/少数龙头。若 AI 核心样本多数跌破 MA50/MA200，说明主题内部宽度恶化；若多数站上均线并贴近52周高，说明承接扩散。',
      caseStudy: '2021 年成长股见顶前，指数和少数龙头仍强，但主题内部大量个股先跌破 MA50/MA200。',
      limitations: '这是 24 只 AI 核心样本，不是全市场或完整 S&P500/Nasdaq100 宽度；Yahoo 单源可能限流，后续应迁移到 GitHub Actions 日更快照。',
      extras: breadthExtras(aiBreadth),
    }));
  }

  {
    const mag7Breadth = analyzeBreadthSample(aiSymbolBars, MAG7_TICKERS);
    metrics.push(metric({
      id: 'mag7_breadth_ma',
      name: 'Mag7 宽度：龙头是否共振',
      purpose: '观察七大权重股是否同步站在趋势线上方，避免指数被一两只股票硬撑',
      value: breadthValue(mag7Breadth),
      status: breadthStatus(mag7Breadth),
      threshold: 'MA50上方 <4/7 或 MA200上方 <4/7 偏风险；多数站上 MA50/MA200 且贴近高点，说明龙头承接尚可',
      sourceName: 'Yahoo Finance Mag7 自算',
      sourceUrl: 'https://finance.yahoo.com/lookup',
      frequency: '日更（美股交易日）',
      updatedAt: mag7Breadth?.updatedAt || null,
      dataStatus: mag7Breadth && mag7Breadth.coveragePct >= 70 ? 'Live' : 'data_unavailable',
      logic: '如果 SPY/QQQ 仍强，但 Mag7 内部开始多数跌破均线，说明权重支撑也在松动；若 Mag7 多数健康，只能说明龙头承接尚可，不代表机构正在增持。',
      caseStudy: '权重股集中行情里，Mag7 内部分化通常先于指数波动扩大。',
      limitations: 'Mag7 是极窄样本，容易被单票财报影响；必须结合 RSP/SPY、IWM/SPY 与 AI 样本宽度一起看。',
      extras: breadthExtras(mag7Breadth),
    }));
  }

  // 手工开启：data/manual-stock-indicators.json 中 enabled=true 的指标覆盖同 id 卡片。
  const finalMetrics = applyManualIndicatorOverrides(metrics, manualConfig);

  // 综合评分
  const agg = aggregate(finalMetrics);

  // 股池元数据（前端直接读 /data/ai-stock-pool.json；这里也一并返回 summary）
  const stockPool = {
    path: '/data/ai-stock-pool.json',
    note: 'MVP 第一期筛选范围：AI 主题股池 + 股池内已标记 in_sp500 / in_nasdaq100 子集。全量 S&P500/Nasdaq-100 宽度已通过 /data/snapshots/market-breadth-latest.json 日更快照接入。',
    categories: [
      'AI-Chip-Compute',
      'AI-Infra-Datacenter',
      'AI-Power-Nuclear',
      'AI-Grid-Equipment',
      'AI-Software-Application',
      'AI-Robotics-Autonomous',
      'AI-China-ADR',
    ],
  };

  // 可选：前 N 个 ticker 的价格（MVP 默认关闭，避免 Edge 并发拉太慢；前端可通过 ?prices=1 打开）
  let pricedSample = null;
  if (includePrices && url.searchParams.get('prices') === '1') {
    // 只取少量热门股票示范；若 Yahoo 限流，失败忽略
    const sampleTickers = ['NVDA', 'AMD', 'AVGO', 'MSFT', 'TSLA', 'CEG', 'VST', 'OKLO', 'SMR', 'NNE'];
    const samples = await Promise.all(sampleTickers.map(async t => {
      const r = await safeFetchJson(t, YF_CHART(t, '5d', '1d'), 3500);
      if (!r.ok) return { ticker: t, ok: false, error: r.error };
      const bars = extractCloses(r.data);
      const m = latestMeta(r.data);
      const last = bars[bars.length - 1]?.close ?? m.regularMarketPrice;
      const prev = bars[bars.length - 2]?.close ?? m.previousClose;
      const changePct = last && prev ? (last / prev - 1) * 100 : null;
      return { ticker: t, ok: true, last, prev, changePct, updatedAt: m.regularMarketTime };
    }));
    pricedSample = samples;
  }

  const body = {
    generatedAt: nowISO(),
    status: agg.overall,
    score: agg.score,
    distributionHits: agg.distributionHits,
    accumulationHits: agg.accumulationHits,
    dataQuality: agg.dataQuality,
    metrics: finalMetrics,
    stockPool,
    pricedSample,
    notes: [
      '仅作研究参考，不构成投资建议。',
      'Live 指标通过 Yahoo Finance 免费公开行情获取；失败时单项降级为 data_unavailable/Pending，不影响整体返回。',
      'V2 第一阶段已接入小盘 IWM/SPY、等权 RSP/SPY、信用 HYG/SPY、板块轮动；V2 第二刀新增 AI 核心样本与 Mag7 真实宽度；V2 第三刀新增 GitHub Actions 日更静态快照，覆盖全量 S&P500 与 QQQ/Nasdaq-100 持仓宽度。CBOE Put/Call 现在从 CBOE Daily Market Statistics 官网 HTML 自动解析，页面结构变化时单项降级。',
      'BofA FMS / FINRA Margin Debt / AAII 仍可通过 /data/manual-stock-indicators.json 手工开启；NYSE A/D Line、New High/New Low 仍保持 Pending，未找到稳定免费自动源前不伪造数值。',
      'Distribution Days 自算方法：当日收跌且成交量 > 前一交易日 → 派发日，近 25 交易日统计。',
      ...notes,
    ],
  };

  return new Response(JSON.stringify(body, null, 2), { status: 200, headers: JSON_HEADERS });
}
