/**
 * Vercel Edge Function: /api/distribution-radar
 * BTC 派发雷达：用公开市场微结构数据观察“上涨但承接变差 / 合约推动”风险。
 * 仅作市场结构观察，不构成投资建议。
 */

export const config = { runtime: 'edge' };

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Cache-Control': 's-maxage=45, stale-while-revalidate=120',
};

const ENDPOINTS = {
  spotTicker: 'https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT',
  futuresTicker: 'https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT',
  premium: 'https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT',
  openInterest: 'https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT',
  spotKlines: 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=48',
  futuresKlines: 'https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=48',
  takerLongShort: 'https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=1h&limit=24',
  coinbaseTicker: 'https://api.exchange.coinbase.com/products/BTC-USD/ticker',
  coinbaseAdvancedTicker: 'https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/ticker',
  krakenTicker: 'https://api.kraken.com/0/public/Ticker?pair=XBTUSD',
  bitstampTicker: 'https://www.bitstamp.net/api/v2/ticker/btcusd/',
  geminiTicker: 'https://api.gemini.com/v1/pubticker/btcusd',
};

function num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(v, min = 0, max = 100) {
  return Math.max(min, Math.min(max, v));
}

function pct(v) {
  return Number.isFinite(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '--';
}

function usd(v, digits = 0) {
  if (!Number.isFinite(v)) return '--';
  return `$${v.toLocaleString('en-US', { maximumFractionDigits: digits })}`;
}

function parseFiatSpotTicker(data) {
  const sources = [
    {
      key: 'coinbaseTicker',
      source: 'Coinbase',
      price: num(data.coinbaseTicker?.price),
    },
    {
      key: 'coinbaseAdvancedTicker',
      source: 'Coinbase',
      price: num(data.coinbaseAdvancedTicker?.price) || num(data.coinbaseAdvancedTicker?.trades?.[0]?.price),
    },
    {
      key: 'krakenTicker',
      source: 'Kraken',
      price: (() => {
        const result = data.krakenTicker?.result || {};
        const firstPair = Object.values(result)[0];
        return num(firstPair?.c?.[0]);
      })(),
    },
    {
      key: 'bitstampTicker',
      source: 'Bitstamp',
      price: num(data.bitstampTicker?.last),
    },
    {
      key: 'geminiTicker',
      source: 'Gemini',
      price: num(data.geminiTicker?.last),
    },
  ];

  return sources.find(s => Number.isFinite(s.price) && s.price > 0) || { key: null, source: null, price: null };
}

async function safeFetchJson(key, url, timeout = 6500) {
  try {
    const res = await fetch(url, {
      headers: { 'Accept': 'application/json', 'User-Agent': 'btc-distribution-radar/1.0' },
      signal: AbortSignal.timeout(timeout),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { key, ok: true, data: await res.json() };
  } catch (err) {
    return { key, ok: false, error: err?.message || String(err) };
  }
}

function parseKline(k) {
  const open = num(k[1], 0);
  const high = num(k[2], 0);
  const low = num(k[3], 0);
  const close = num(k[4], 0);
  const volume = num(k[5], 0);
  const quoteVolume = num(k[7], 0);
  const takerBuyBase = num(k[9], null);
  const takerBuyQuote = num(k[10], null);
  return { open, high, low, close, volume, quoteVolume, takerBuyBase, takerBuyQuote };
}

function splitTrend(klines) {
  if (!Array.isArray(klines) || klines.length < 8) return null;
  const candles = klines.map(parseKline).filter(c => c.close > 0);
  if (candles.length < 8) return null;
  const half = Math.floor(candles.length / 2);
  const first = candles.slice(0, half);
  const second = candles.slice(half);
  const start = first[0].open;
  const end = second[second.length - 1].close;
  const priceChangePct = start > 0 ? (end / start - 1) * 100 : null;
  const firstVol = first.reduce((s, c) => s + c.quoteVolume, 0) / first.length;
  const secondVol = second.reduce((s, c) => s + c.quoteVolume, 0) / second.length;
  const volumeChangePct = firstVol > 0 ? (secondVol / firstVol - 1) * 100 : null;
  return { candles, priceChangePct, volumeChangePct, firstVol, secondVol, latest: candles[candles.length - 1] };
}

function wickStats(klines) {
  if (!Array.isArray(klines) || klines.length < 4) return null;
  const candles = klines.map(parseKline).filter(c => c.high > c.low);
  const recent = candles.slice(-12);
  if (!recent.length) return null;
  const ratios = recent.map(c => {
    const range = c.high - c.low;
    const upper = c.high - Math.max(c.open, c.close);
    return range > 0 ? upper / range : 0;
  });
  const avgUpperWickPct = ratios.reduce((a, b) => a + b, 0) / ratios.length * 100;
  const latest = candles[candles.length - 1];
  const latestPullbackPct = latest.high > 0 ? (latest.high - latest.close) / latest.high * 100 : 0;
  return { avgUpperWickPct, latestPullbackPct };
}

function addMetric(metrics, metric) {
  metrics.push({
    ...metric,
    score: clamp(Math.round(metric.score || 0), 0, metric.maxScore || 100),
    status: metric.status || 'neutral',
  });
}

function levelFromScore(score) {
  if (score <= 34) return { code: 'healthy', label: '健康上涨', tone: 'good' };
  if (score <= 64) return { code: 'watch', label: '派发苗头', tone: 'warn' };
  return { code: 'distribution', label: '强派发嫌疑', tone: 'danger' };
}

export default async function handler(req) {
  if (req.method !== 'GET' && req.method !== 'OPTIONS') {
    return new Response(JSON.stringify({ error: 'Method Not Allowed' }), { status: 405, headers: JSON_HEADERS });
  }
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: JSON_HEADERS });

  const results = await Promise.all(Object.entries(ENDPOINTS).map(([key, url]) => safeFetchJson(key, url)));
  const data = {};
  const notes = [];
  for (const r of results) {
    if (r.ok) data[r.key] = r.data;
    else notes.push(`${r.key} failed: ${r.error}`);
  }

  const spotTicker = data.spotTicker || {};
  const futuresTicker = data.futuresTicker || {};
  const premium = data.premium || {};
  const openInterest = data.openInterest || {};
  const spotTrend = splitTrend(data.spotKlines);
  const futuresTrend = splitTrend(data.futuresKlines);
  const wick = wickStats(data.futuresKlines || data.spotKlines);

  const spotPrice = num(spotTicker.lastPrice) || num(premium.indexPrice) || (spotTrend?.latest?.close ?? null);
  const markPrice = num(premium.markPrice) || spotPrice;
  const spotQuoteVolume = num(spotTicker.quoteVolume);
  const futuresQuoteVolume = num(futuresTicker.quoteVolume);
  const price24hPct = num(spotTicker.priceChangePercent);
  const price48hPct = spotTrend?.priceChangePct;
  const rawFundingRate = num(premium.lastFundingRate);
  const fundingRatePct = Number.isFinite(rawFundingRate) ? rawFundingRate * 100 : null;
  const oiBtc = num(openInterest.openInterest);
  const oiNotional = oiBtc && markPrice ? oiBtc * markPrice : null;
  const futuresSpotRatio = spotQuoteVolume && futuresQuoteVolume ? futuresQuoteVolume / spotQuoteVolume : null;
  const oiToFuturesVolume = oiNotional && futuresQuoteVolume ? oiNotional / futuresQuoteVolume : null;
  const fiatSpot = parseFiatSpotTicker(data);
  const fiatSpotPrice = fiatSpot.price;
  const fiatSpotSource = fiatSpot.source;
  const fiatSpotPremiumPct = fiatSpotPrice && spotPrice ? (fiatSpotPrice / spotPrice - 1) * 100 : null;
  // Backward-compatible aliases for older frontends/consumers.
  const coinbasePrice = fiatSpotPrice;
  const coinbasePremiumPct = fiatSpotPremiumPct;

  const takerRows = Array.isArray(data.takerLongShort) ? data.takerLongShort : [];
  const recentTaker = takerRows.map(r => num(r.buySellRatio)).filter(Number.isFinite);
  const takerBuySellRatio = recentTaker.length ? recentTaker.reduce((a, b) => a + b, 0) / recentTaker.length : null;

  const metrics = [];

  // 1) 价格趋势与成交量背离（权重 18）
  let divScore = 7;
  if (spotTrend && Number.isFinite(spotTrend.priceChangePct) && Number.isFinite(spotTrend.volumeChangePct)) {
    if (spotTrend.priceChangePct > 2 && spotTrend.volumeChangePct < -25) divScore = 18;
    else if (spotTrend.priceChangePct > 1 && spotTrend.volumeChangePct < -10) divScore = 13;
    else if (spotTrend.priceChangePct > 0 && spotTrend.volumeChangePct < 0) divScore = 9;
    else if (spotTrend.priceChangePct > 0 && spotTrend.volumeChangePct > 15) divScore = 3;
    else divScore = 6;
    addMetric(metrics, {
      id: 'spot_volume_divergence', name: '价格趋势与现货成交量背离', maxScore: 18, score: divScore,
      status: divScore >= 13 ? 'risk' : divScore <= 5 ? 'healthy' : 'neutral',
      value: `${pct(spotTrend.priceChangePct)} / 量 ${pct(spotTrend.volumeChangePct)}`,
      detail: '对比最近 48 根 1h 现货K线前后两段：价格上涨但成交额下降，代表承接质量变弱。',
    });
  } else {
    notes.push('spot kline trend unavailable; divergence metric set neutral');
    addMetric(metrics, { id: 'spot_volume_divergence', name: '价格趋势与现货成交量背离', maxScore: 18, score: 7, status: 'neutral', value: '待接入', detail: '现货K线获取失败，按中性处理。' });
  }

  // 2) 合约/现货成交比（权重 16）
  let ratioScore = 7;
  if (Number.isFinite(futuresSpotRatio)) {
    ratioScore = futuresSpotRatio > 5 ? 16 : futuresSpotRatio > 3 ? 12 : futuresSpotRatio > 2 ? 8 : 4;
    addMetric(metrics, {
      id: 'futures_spot_volume_ratio', name: '合约 / 现货成交比', maxScore: 16, score: ratioScore,
      status: ratioScore >= 12 ? 'risk' : ratioScore <= 5 ? 'healthy' : 'neutral',
      value: `${futuresSpotRatio.toFixed(2)}x`,
      detail: `Binance 24h 成交额：合约 ${usd(futuresQuoteVolume)}，现货 ${usd(spotQuoteVolume)}。比值越高，越像合约推着价格走。`,
    });
  } else {
    addMetric(metrics, { id: 'futures_spot_volume_ratio', name: '合约 / 现货成交比', maxScore: 16, score: 7, status: 'neutral', value: '待接入', detail: '现货或合约 24h ticker 不完整，按中性处理。' });
  }

  // 3) OI 杠杆压力（权重 16）
  let oiScore = 7;
  if (Number.isFinite(oiToFuturesVolume)) {
    oiScore = oiToFuturesVolume > 0.45 ? 16 : oiToFuturesVolume > 0.28 ? 12 : oiToFuturesVolume > 0.16 ? 8 : 4;
    if ((price24hPct ?? 0) > 2 && Number.isFinite(fundingRatePct) && fundingRatePct > 0.02) oiScore = Math.min(16, oiScore + 2);
    addMetric(metrics, {
      id: 'open_interest_pressure', name: 'OI 规模 / 杠杆拥挤度', maxScore: 16, score: oiScore,
      status: oiScore >= 12 ? 'risk' : oiScore <= 5 ? 'healthy' : 'neutral',
      value: `${usd(oiNotional)} / 24h量 ${(oiToFuturesVolume * 100).toFixed(1)}%`,
      detail: '当前 BTCUSDT 永续 OI 折美元后对比 24h 合约成交额；价格上涨时 OI 拥挤说明杠杆资金占比偏高。',
    });
  } else {
    addMetric(metrics, { id: 'open_interest_pressure', name: 'OI 规模 / 杠杆拥挤度', maxScore: 16, score: 7, status: 'neutral', value: '待接入', detail: 'Open Interest 或合约成交额获取失败，按中性处理。' });
  }

  // 4) Funding 热度（权重 14）
  if (Number.isFinite(fundingRatePct)) {
    const fundingScore = fundingRatePct > 0.06 ? 14 : fundingRatePct > 0.03 ? 10 : fundingRatePct > 0.015 ? 6 : fundingRatePct < -0.01 ? 2 : 4;
    addMetric(metrics, {
      id: 'funding_heat', name: 'Funding 多头付费热度', maxScore: 14, score: fundingScore,
      status: fundingScore >= 10 ? 'risk' : fundingScore <= 4 ? 'healthy' : 'neutral',
      value: `${fundingRatePct.toFixed(4)}% / 8h`,
      detail: 'Funding 越高，说明追多资金越拥挤；上涨主要由高 funding 推动时，冲高回落风险上升。',
    });
  } else {
    notes.push('funding rate unavailable; metric set neutral');
    addMetric(metrics, {
      id: 'funding_heat', name: 'Funding 多头付费热度', maxScore: 14, score: 6,
      status: 'neutral', value: '待接入 / 中性',
      detail: 'Funding 接口获取失败，不能判断多头拥挤度，暂按中性处理。',
    });
  }

  // 5) 上影线 / 冲高回落（权重 14）
  let wickScore = 6;
  if (wick) {
    wickScore = wick.avgUpperWickPct > 38 || wick.latestPullbackPct > 1.2 ? 14 : wick.avgUpperWickPct > 28 || wick.latestPullbackPct > 0.7 ? 10 : wick.avgUpperWickPct > 20 ? 7 : 3;
    addMetric(metrics, {
      id: 'upper_wick_pullback', name: '上影线 / 冲高回落', maxScore: 14, score: wickScore,
      status: wickScore >= 10 ? 'risk' : wickScore <= 4 ? 'healthy' : 'neutral',
      value: `均上影 ${wick.avgUpperWickPct.toFixed(1)}% / 最新回落 ${wick.latestPullbackPct.toFixed(2)}%`,
      detail: '统计最近 12 根 1h K线的上影占比与最新K线距高点回落幅度。上影多代表高位抛压增强。',
    });
  } else {
    addMetric(metrics, { id: 'upper_wick_pullback', name: '上影线 / 冲高回落', maxScore: 14, score: 6, status: 'neutral', value: '待接入', detail: 'K线获取失败，按中性处理。' });
  }

  // 6) 美盘/法币现货溢价（权重 12）
  let cbScore = 5;
  if (Number.isFinite(fiatSpotPremiumPct)) {
    cbScore = fiatSpotPremiumPct < -0.08 ? 12 : fiatSpotPremiumPct < 0 ? 9 : fiatSpotPremiumPct < 0.04 ? 6 : 2;
    if ((price24hPct ?? price48hPct ?? 0) > 1 && fiatSpotPremiumPct < 0.02) cbScore = Math.min(12, cbScore + 2);
    addMetric(metrics, {
      id: 'coinbase_premium', name: '美盘/法币现货溢价', maxScore: 12, score: cbScore,
      status: cbScore >= 9 ? 'risk' : cbScore <= 3 ? 'healthy' : 'neutral',
      value: `${fiatSpotPremiumPct >= 0 ? '+' : ''}${fiatSpotPremiumPct.toFixed(3)}% (${fiatSpotSource})`,
      detail: `${fiatSpotSource} BTC/USD ${usd(fiatSpotPrice, 2)} vs Binance BTCUSDT ${usd(spotPrice, 2)}。使用 Coinbase/Kraken/Bitstamp/Gemini fallback；溢价弱代表美盘/法币现货买盘不积极。`,
    });
  } else {
    notes.push('fiat spot premium unavailable; all Coinbase/Kraken/Bitstamp/Gemini fallback sources failed; metric set neutral');
    addMetric(metrics, { id: 'coinbase_premium', name: '美盘/法币现货溢价', maxScore: 12, score: 5, status: 'neutral', value: '待接入', detail: 'Coinbase/Kraken/Bitstamp/Gemini ticker 均不可用，按中性处理。' });
  }

  // 7) 主动买盘质量（权重 10）
  let takerScore = 5;
  if (Number.isFinite(takerBuySellRatio)) {
    takerScore = takerBuySellRatio < 0.9 ? 10 : takerBuySellRatio < 0.98 ? 7 : takerBuySellRatio < 1.08 ? 4 : 2;
    addMetric(metrics, {
      id: 'active_buy_quality', name: '主动买盘质量', maxScore: 10, score: takerScore,
      status: takerScore >= 7 ? 'risk' : takerScore <= 3 ? 'healthy' : 'neutral',
      value: `买卖比 ${takerBuySellRatio.toFixed(2)}`,
      detail: 'Binance futures taker long/short buy-sell ratio 最近 24 小时均值；上涨时主动买盘偏弱，是承接不足信号。',
    });
  } else {
    notes.push('taker buy/sell data unavailable; metric set neutral');
    addMetric(metrics, { id: 'active_buy_quality', name: '主动买盘质量', maxScore: 10, score: 5, status: 'neutral', value: '待接入 / 中性', detail: '公开接口可能限流或不可用，暂按中性处理。' });
  }

  const score = clamp(metrics.reduce((sum, m) => sum + (m.score || 0), 0), 0, 100);
  const level = levelFromScore(score);
  const riskNames = metrics.filter(m => m.status === 'risk').map(m => m.name).slice(0, 3);
  const summary = riskNames.length
    ? `当前风险点集中在：${riskNames.join('、')}。该页面只观察“上涨质量”，不判断方向。`
    : '当前未见明显派发结构，现货承接与衍生品热度暂未出现极端背离。';

  const okSources = Object.keys(data);
  return new Response(JSON.stringify({
    generatedAt: new Date().toISOString(),
    score,
    level,
    summary,
    metrics,
    raw: {
      price: spotPrice,
      priceChange24hPct: price24hPct,
      priceChange48hPct: price48hPct,
      spotQuoteVolume,
      futuresQuoteVolume,
      futuresSpotRatio,
      openInterestBtc: oiBtc,
      openInterestNotionalUsd: oiNotional,
      fundingRatePct,
      fiatSpotPrice,
      fiatSpotSource,
      fiatSpotPremiumPct,
      coinbasePrice,
      coinbasePremiumPct,
      takerBuySellRatio,
      sourcesOk: okSources,
    },
    notes: [
      '仅作为市场结构观察，不构成投资建议。',
      '单一数据源失败不会中断整体评分，失败项按中性或待接入处理。',
      ...notes,
    ],
  }, null, 2), { status: 200, headers: JSON_HEADERS });
}
