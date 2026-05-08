export const config = { runtime: 'edge' };

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type',
  'Cache-Control': 'no-store',
};

function memberTokens() {
  return (process.env.MEMBER_LICENSE_CODES || process.env.MEMBER_TOKENS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

function normalizeCode(code) {
  return String(code || '').trim();
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: JSON_HEADERS });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method Not Allowed' }), { status: 405, headers: JSON_HEADERS });
  }

  let body = {};
  try {
    body = await req.json();
  } catch {
    body = {};
  }

  const code = normalizeCode(body.code);
  if (!code) {
    return new Response(JSON.stringify({ error: 'missing_code', message: '请输入会员兑换码' }), { status: 400, headers: JSON_HEADERS });
  }

  if (!memberTokens().includes(code)) {
    return new Response(JSON.stringify({ error: 'invalid_code', message: '兑换码无效或已停用' }), { status: 403, headers: JSON_HEADERS });
  }

  return new Response(JSON.stringify({
    ok: true,
    token: code,
    plan: 'member',
    message: '会员已解锁。现在可以查看个股派发/承接线索。',
  }), { status: 200, headers: JSON_HEADERS });
}
