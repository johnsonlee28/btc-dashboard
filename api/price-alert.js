/**
 * Vercel Serverless Function: /api/price-alert
 *
 * 两种调用方式：
 * 1. Vercel Cron（每5分钟）: GET /api/price-alert  — 自动拉价格，对所有alerts检查并推送
 * 2. 前端手动触发:            POST /api/price-alert — body: { alerts, serverChanKey }
 *
 * Alert 格式:
 * { id, symbol, condition, targetPrice, label, triggered, serverChanKey }
 * condition: 'below' | 'above'
 * symbol: 'BTC' | 'ETH' | 任意 CoinGecko id
 */

const COINGECKO_IDS = {
  BTC: 'bitcoin',
  ETH: 'ethereum',
  BNB: 'binancecoin',
  SOL: 'solana',
  XRP: 'ripple',
  ADA: 'cardano',
  DOGE: 'dogecoin',
  AVAX: 'avalanche-2',
  DOT: 'polkadot',
  MATIC: 'matic-network',
  LINK: 'chainlink',
  UNI: 'uniswap',
};

async function getPrice(symbol) {
  const cgId = COINGECKO_IDS[symbol.toUpperCase()] || symbol.toLowerCase();

  // 先试 Binance（BTC/ETH/BNB/SOL等主流币快）
  try {
    const pair = symbol.toUpperCase() + 'USDT';
    const res = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${pair}`, {
      signal: AbortSignal.timeout(4000),
    });
    if (res.ok) {
      const d = await res.json();
      const p = parseFloat(d.price);
      if (!isNaN(p) && p > 0) return { price: p, source: 'Binance' };
    }
  } catch (_) {}

  // 备用 CoinGecko
  const res = await fetch(
    `https://api.coingecko.com/api/v3/simple/price?ids=${cgId}&vs_currencies=usd`,
    { signal: AbortSignal.timeout(8000) }
  );
  if (!res.ok) throw new Error(`CoinGecko HTTP ${res.status}`);
  const d = await res.json();
  const p = d?.[cgId]?.usd;
  if (!p) throw new Error(`No price for ${symbol}`);
  return { price: p, source: 'CoinGecko' };
}

async function sendServerChan(key, title, desp) {
  const url = `https://sctapi.ftqq.com/${key}.send`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ title, desp }),
    signal: AbortSignal.timeout(8000),
  });
  const d = await res.json();
  return d;
}

export default async function handler(req) {
  const isCron = req.headers.get('x-vercel-cron') === '1' ||
                 req.method === 'GET';

  let alerts = [];
  let serverChanKey = '';

  if (isCron) {
    // Cron 模式：从环境变量读 key 和 alerts
    serverChanKey = process.env.SERVER_CHAN_KEY || '';
    const raw = process.env.PRICE_ALERTS || '[]';
    try { alerts = JSON.parse(raw); } catch (_) { alerts = []; }
  } else {
    // 前端 POST 模式
    try {
      const body = await req.json();
      alerts = body.alerts || [];
      serverChanKey = body.serverChanKey || process.env.SERVER_CHAN_KEY || '';
    } catch (_) {
      return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  if (!alerts.length) {
    return new Response(JSON.stringify({ ok: true, checked: 0, triggered: [] }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // 按 symbol 分组，批量拉价格
  const symbols = [...new Set(alerts.map(a => a.symbol.toUpperCase()))];
  const prices = {};
  await Promise.all(symbols.map(async sym => {
    try {
      const { price } = await getPrice(sym);
      prices[sym] = price;
    } catch (e) {
      prices[sym] = null;
      console.error(`Price fetch failed for ${sym}:`, e.message);
    }
  }));

  const triggered = [];

  for (const alert of alerts) {
    if (alert.triggered) continue; // 已触发，跳过
    const sym = alert.symbol.toUpperCase();
    const current = prices[sym];
    if (current === null) continue;

    const hit =
      (alert.condition === 'below' && current <= alert.targetPrice) ||
      (alert.condition === 'above' && current >= alert.targetPrice);

    if (hit) {
      triggered.push({ ...alert, currentPrice: current });

      // 推送微信
      const key = alert.serverChanKey || serverChanKey;
      if (key) {
        const dir = alert.condition === 'below' ? '📉 跌破' : '📈 突破';
        const title = `🔔 ${sym} 价格提醒 | ${dir} $${alert.targetPrice.toLocaleString()}`;
        const desp = [
          `**标的：** ${sym}`,
          `**目标价：** $${alert.targetPrice.toLocaleString()}`,
          `**当前价：** $${current.toLocaleString()}`,
          `**条件：** ${dir} 目标价`,
          alert.label ? `**备注：** ${alert.label}` : '',
          '',
          `[点击查看仪表盘](https://btc.flowhunt.net)`,
        ].filter(Boolean).join('\n\n');

        try {
          await sendServerChan(key, title, desp);
        } catch (e) {
          console.error('Server酱推送失败:', e.message);
        }
      }
    }
  }

  return new Response(
    JSON.stringify({ ok: true, checked: alerts.length, triggered, prices }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    }
  );
}

export const config = {
  runtime: 'edge',
};
