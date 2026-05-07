/**
 * Vercel Edge Function: /api/stock-radar
 * 美股派发/建仓雷达 MVP 第一期
 *
 * 原则：
 * - 可解释性 > 漂亮。每个指标必须带 sourceName / sourceUrl / frequency / updatedAt / dataStatus
 *   / logic / threshold / caseStudy / limitations 完整元数据。
 * - 免费公开接口（Yahoo chart）失败时，指标 dataStatus = "data_unavailable"，
 *   不让整个 API 失败；不伪造值。
 * - CBOE Equity Put/Call / AAII / BofA FMS / FINRA Margin Debt 目前无稳定免费 JSON 接口，
 *   一律标记 Manual 或 Pending，只给方法论，不给捏造数值。
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

function extractCloses(yfJson) {
  try {
    const result = yfJson?.chart?.result?.[0];
    const ts = result?.timestamp || [];
    const quote = result?.indicators?.quote?.[0] || {};
    const closes = quote.close || [];
    const volumes = quote.volume || [];
    const out = [];
    for (let i = 0; i < ts.length; i++) {
      const c = num(closes[i]);
      const v = num(volumes[i]);
      if (c != null) out.push({ t: ts[i], close: c, volume: v });
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
function aggregate(metrics) {
  // 仅对 Live 且有明确 status 的指标做投票
  const considered = metrics.filter(m =>
    m.dataStatus === 'Live' && ['distribution', 'neutral', 'accumulation'].includes(m.status)
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

  // 大盘指标需要的 Yahoo 数据
  const endpoints = {
    spy: YF_CHART('SPY', '3mo', '1d'),
    qqq: YF_CHART('QQQ', '3mo', '1d'),
    vix: YF_CHART('^VIX', '1mo', '1d'),     // Yahoo chart 会在 YF_CHART 内统一 encode
    vix3m: YF_CHART('^VIX3M', '1mo', '1d'),
    skew: YF_CHART('^SKEW', '1mo', '1d'),
    gspc: YF_CHART('^GSPC', '1mo', '1d'),
  };

  const notes = [];
  const results = await Promise.all(Object.entries(endpoints).map(([k, u]) => safeFetchJson(k, u)));
  const data = {};
  for (const r of results) {
    if (r.ok) data[r.key] = r.data;
    else notes.push(`${r.key} fetch failed: ${r.error}`);
  }

  const spyBars = extractCloses(data.spy || {});
  const qqqBars = extractCloses(data.qqq || {});
  const vixBars = extractCloses(data.vix || {});
  const vix3mBars = extractCloses(data.vix3m || {});
  const skewBars = extractCloses(data.skew || {});

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
    threshold: '< 4% 触发 sell signal；> 5% 偏防御/建仓',
    sourceName: 'BofA Global Fund Manager Survey',
    sourceUrl: 'https://business.bofa.com/en-us/content/global-research.html',
    frequency: '月更（每月中旬发布）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '机构现金仓位极低 → 子弹打完，易成为顶部反向信号；现金仓位抬升 → 防御/等待建仓。',
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
    threshold: '同比 > +25% 且创新高 偏派发；同比转负 偏建仓',
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
    threshold: '> +30% 散户极度乐观 偏派发；< -20% 极度悲观 偏建仓',
    sourceName: 'AAII Investor Sentiment Survey',
    sourceUrl: 'https://www.aaii.com/sentimentsurvey',
    frequency: '周更（每周四公布）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '反向指标：散户看多过度 → 情绪拥挤，顶部风险；极度看空 → 常伴随阶段性底部。',
    caseStudy: '2024/01, 2025/07 bull-bear spread 连续多周 > 30% 后出现震荡/回调。',
    limitations: '样本量小、自愿填写，噪声大；需要在 AAII 官网手工同步或抓取。',
  }));

  // 4) CBOE Equity Put/Call Ratio
  metrics.push(metric({
    id: 'cboe_equity_pc',
    name: 'CBOE Equity Put/Call Ratio',
    purpose: '个股期权 Put/Call 成交量比（不含指数），反向情绪指标',
    value: null,
    status: 'pending',
    threshold: '< 0.50 极度贪婪 偏派发；> 0.80 偏恐慌/建仓',
    sourceName: 'CBOE Market Statistics',
    sourceUrl: 'https://www.cboe.com/us/options/market_statistics/daily/',
    frequency: '日更（收盘后发布）',
    updatedAt: null,
    dataStatus: 'Manual',
    logic: '个股 Put/Call 越低，说明散户买 Call 投机情绪越重；通常与市场顶部重合。',
    caseStudy: '2021/11 equity P/C 持续 < 0.40，伴随成长股顶部；2022/10 飙升至 > 0.9 后出现中期底。',
    limitations: 'CBOE 官网以 CSV/HTML 发布，没有稳定免费 JSON 接口；MVP 人工同步。',
  }));

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
      threshold: '> 150 机构买尾部保险（派发/对冲信号）；< 125 相对放松（建仓/风险偏好）',
      sourceName: 'CBOE SKEW (via Yahoo ^SKEW)',
      sourceUrl: 'https://finance.yahoo.com/quote/%5ESKEW/',
      frequency: '日更（交易日收盘后）',
      updatedAt: skewMeta.regularMarketTime || nowISO(),
      dataStatus: skew != null ? 'Live' : 'data_unavailable',
      logic: 'SKEW 越高 → 机构越愿意为黑天鹅付保费，通常与派发/分散出货同步；SKEW 偏低 → 市场不担心尾部风险。',
      caseStudy: '2018/01, 2021/11 SKEW 持续 > 150 之后出现显著回调。',
      limitations: 'SKEW 对方向性预测能力弱，仅反映尾部溢价；仅作派发/建仓拼图之一。',
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
      threshold: 'VIX / VIX3M ≥ 1.00 倒挂 偏派发；≤ 0.85 深度 contango 偏建仓',
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
    threshold: '指数创新高但 A/D Line 未创新高 偏派发；同步创新高 偏建仓',
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
    threshold: '指数新高但新高家数 < 100 且新低家数上升 偏派发；新低萎缩、新高扩张 偏建仓',
    sourceName: 'WSJ Market Data / StockCharts',
    sourceUrl: 'https://www.wsj.com/market-data/stocks/highsandlows',
    frequency: '日更',
    updatedAt: null,
    dataStatus: 'Pending',
    logic: '宽度指标：指数新高但创新高个股变少，代表涨幅集中在少数权重股；新低家数同时抬头是典型派发前兆。',
    caseStudy: '2000/03、2007/07 指数新高时新高家数反而持续走低。',
    limitations: '免费接口不稳定，MVP 第二期接 WSJ / StockCharts 爬虫或 FINRA 统计。',
  }));

  // 9) Distribution Days（Live 自算：SPY）
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
      threshold: '近 25 交易日 ≥ 5 派发日 → 偏派发；≤ 2 → 偏建仓',
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

  // 综合评分
  const agg = aggregate(metrics);

  // 股池元数据（前端直接读 /data/ai-stock-pool.json；这里也一并返回 summary）
  const stockPool = {
    path: '/data/ai-stock-pool.json',
    note: 'MVP 第一期筛选范围：AI 主题股池 + 股池内已标记 in_sp500 / in_nasdaq100 子集。全量 S&P500/Nasdaq100 成分股将在第二期接入。',
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
    metrics,
    stockPool,
    pricedSample,
    notes: [
      '仅作研究参考，不构成投资建议。',
      'Live 指标通过 Yahoo Finance 免费公开接口获取；失败时单项降级为 data_unavailable，不影响整体返回。',
      'BofA FMS / FINRA Margin Debt / AAII / CBOE Equity Put-Call 目前无稳定免费 JSON 接口，MVP 一期标记 Manual / Pending，不伪造实时值。',
      'NYSE A/D Line 与 New High/New Low 宽度指标将在 MVP 第二期接入。',
      'Distribution Days 自算方法：当日收跌且成交量 > 前一交易日 → 派发日，近 25 交易日统计。',
      ...notes,
    ],
  };

  return new Response(JSON.stringify(body, null, 2), { status: 200, headers: JSON_HEADERS });
}
