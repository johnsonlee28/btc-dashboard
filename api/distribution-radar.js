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
  okxFuturesTicker: 'https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP',
  bybitFuturesTicker: 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT',
  deribitFuturesTicker: 'https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL',
  okxSpotTicker: 'https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT',
  bybitSpotTicker: 'https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT',
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

function positiveNum(v) {
  const n = num(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function okxVolumeToUsd(ticker, fallbackPrice) {
  const last = positiveNum(ticker?.last) || positiveNum(fallbackPrice);
  const volCcy24h = positiveNum(ticker?.volCcy24h);
  if (volCcy24h) {
    // OKX volCcy24h unit is not uniformly documented across spot/swap.
    // Large values are treated as quote notional; BTC-sized values are converted by last.
    if (volCcy24h > 10_000_000) return { quoteVolume: volCcy24h, note: 'OKX volCcy24h parsed as quote/USDT notional.' };
    if (last) return { quoteVolume: volCcy24h * last, note: 'OKX volCcy24h parsed as BTC/base volume × last; approximate USD notional.' };
  }
  const vol24h = positiveNum(ticker?.vol24h);
  if (vol24h && last) return { quoteVolume: vol24h * last, note: 'OKX vol24h fallback × last; contract/base unit may be approximate.' };
  return { quoteVolume: null, note: 'OKX volume fields unavailable or unit could not be converted.' };
}

function venue(venueName, quoteVolume, ok = true, note = undefined) {
  const qv = positiveNum(quoteVolume);
  return { venue: venueName, quoteVolume: qv, ok: Boolean(ok && qv), ...(note ? { note } : {}) };
}

function venueFailed(venueName, note) {
  return { venue: venueName, quoteVolume: null, ok: false, note };
}

function bybitTicker(data, expectedSymbol = 'BTCUSDT') {
  const list = data?.result?.list;
  if (!Array.isArray(list)) return null;
  return list.find(item => item?.symbol === expectedSymbol) || list[0] || null;
}

function buildVolumeAggregation(data, spotPrice, futuresQuoteVolume, spotQuoteVolume) {
  const futuresVenues = [];
  const spotVenues = [];

  futuresVenues.push(venue('Binance BTCUSDT perpetual', futuresQuoteVolume));

  const okxFutures = data.okxFuturesTicker?.data?.[0];
  if (okxFutures) {
    const parsed = okxVolumeToUsd(okxFutures, spotPrice);
    futuresVenues.push(venue('OKX BTC-USDT-SWAP', parsed.quoteVolume, true, parsed.note));
  } else {
    futuresVenues.push(venueFailed('OKX BTC-USDT-SWAP', 'ticker unavailable'));
  }

  const bybitFutures = bybitTicker(data.bybitFuturesTicker);
  futuresVenues.push(bybitFutures
    ? venue('Bybit linear BTCUSDT', positiveNum(bybitFutures.turnover24h), true, 'Bybit turnover24h parsed as quote/USDT notional.')
    : venueFailed('Bybit linear BTCUSDT', 'ticker unavailable'));

  const deribit = data.deribitFuturesTicker?.result;
  const deribitPrice = positiveNum(deribit?.last_price) || positiveNum(spotPrice);
  const deribitVolumeUsd = positiveNum(deribit?.stats?.volume_usd)
    || (positiveNum(deribit?.stats?.volume) && deribitPrice ? positiveNum(deribit.stats.volume) * deribitPrice : null);
  futuresVenues.push(deribit
    ? venue('Deribit BTC-PERPETUAL', deribitVolumeUsd, true, deribit?.stats?.volume_usd ? 'Deribit stats.volume_usd parsed as USD notional.' : 'Deribit volume fallback × price; approximate USD notional.')
    : venueFailed('Deribit BTC-PERPETUAL', 'ticker unavailable'));

  spotVenues.push(venue('Binance BTCUSDT spot', spotQuoteVolume));

  const okxSpot = data.okxSpotTicker?.data?.[0];
  if (okxSpot) {
    const parsed = okxVolumeToUsd(okxSpot, spotPrice);
    spotVenues.push(venue('OKX BTC-USDT spot', parsed.quoteVolume, true, parsed.note));
  } else {
    spotVenues.push(venueFailed('OKX BTC-USDT spot', 'ticker unavailable'));
  }

  const bybitSpot = bybitTicker(data.bybitSpotTicker);
  spotVenues.push(bybitSpot
    ? venue('Bybit spot BTCUSDT', positiveNum(bybitSpot.turnover24h), true, 'Bybit turnover24h parsed as quote/USDT notional.')
    : venueFailed('Bybit spot BTCUSDT', 'ticker unavailable'));

  const globalFuturesQuoteVolume = futuresVenues.reduce((sum, v) => sum + (v.ok && Number.isFinite(v.quoteVolume) ? v.quoteVolume : 0), 0) || null;
  const globalSpotQuoteVolume = spotVenues.reduce((sum, v) => sum + (v.ok && Number.isFinite(v.quoteVolume) ? v.quoteVolume : 0), 0) || null;
  const globalFuturesSpotRatio = globalFuturesQuoteVolume && globalSpotQuoteVolume ? globalFuturesQuoteVolume / globalSpotQuoteVolume : null;

  return { futuresVenues, spotVenues, globalFuturesQuoteVolume, globalSpotQuoteVolume, globalFuturesSpotRatio };
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
  const {
    futuresVenues,
    spotVenues,
    globalFuturesQuoteVolume,
    globalSpotQuoteVolume,
    globalFuturesSpotRatio,
  } = buildVolumeAggregation(data, spotPrice, futuresQuoteVolume, spotQuoteVolume);
  const ratioForMetric = Number.isFinite(globalFuturesSpotRatio) ? globalFuturesSpotRatio : futuresSpotRatio;
  const usingGlobalRatio = Number.isFinite(globalFuturesSpotRatio);
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

  // 2) 全市场合约/现货成交比（权重 16）
  let ratioScore = 7;
  if (Number.isFinite(ratioForMetric)) {
    ratioScore = ratioForMetric > 5 ? 16 : ratioForMetric > 3 ? 12 : ratioForMetric > 2 ? 8 : 4;
    const futuresVenueNames = futuresVenues.filter(v => v.ok).map(v => v.venue).join('、') || '无';
    const spotVenueNames = spotVenues.filter(v => v.ok).map(v => v.venue).join('、') || '无';
    addMetric(metrics, {
      id: 'futures_spot_volume_ratio', name: '全市场合约 / 现货成交比', maxScore: 16, score: ratioScore,
      status: ratioScore >= 12 ? 'risk' : ratioScore <= 5 ? 'healthy' : 'neutral',
      value: `${ratioForMetric.toFixed(2)}x${usingGlobalRatio ? '' : ' (Binance fallback)'}`,
      detail: usingGlobalRatio
        ? `聚合 24h 成交额：合约 ${usd(globalFuturesQuoteVolume)}（${futuresVenueNames}），现货 ${usd(globalSpotQuoteVolume)}（${spotVenueNames}）。Deribit 只作为合约侧参考、不是现货；法币现货价格用于溢价，成交量聚合暂以 Binance/OKX/Bybit 为主。比值越高，越像合约推着价格走。`
        : `全市场聚合失败，fallback 到 Binance 单点 24h 成交额：合约 ${usd(futuresQuoteVolume)}，现货 ${usd(spotQuoteVolume)}。Deribit 只作为合约侧参考、不是现货。`,
    });
  } else {
    addMetric(metrics, { id: 'futures_spot_volume_ratio', name: '全市场合约 / 现货成交比', maxScore: 16, score: 7, status: 'neutral', value: '待接入', detail: '全市场现货或合约 24h ticker 不完整，按中性处理；Deribit 只作为合约侧参考、不是现货。' });
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
      globalFuturesQuoteVolume,
      globalSpotQuoteVolume,
      globalFuturesSpotRatio,
      futuresVenues,
      spotVenues,
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
      '全市场合约成交量聚合 Binance / OKX / Bybit / Deribit；现货成交量聚合 Binance / OKX / Bybit。Deribit 只作为合约侧参考、不是现货。',
      'Coinbase/Kraken/Bitstamp/Gemini 法币现货价格用于溢价，成交量聚合暂以 Binance/OKX/Bybit 为主。',
      '单一数据源失败不会中断整体评分，失败项按中性或待接入处理。',
      ...notes,
    ],
  }, null, 2), { status: 200, headers: JSON_HEADERS });
}
