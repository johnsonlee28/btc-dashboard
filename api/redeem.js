import { createMemberSession } from './_membership.js';

export const config = { runtime: 'edge' };

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-Device-Id',
  'Cache-Control': 'no-store',
};

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
  const deviceId = String(body.deviceId || req.headers.get('x-device-id') || '').trim();
  if (!code) {
    return new Response(JSON.stringify({ error: 'missing_code', message: '请输入会员兑换码' }), { status: 400, headers: JSON_HEADERS });
  }

  const session = await createMemberSession(req, code, deviceId);
  if (!session.ok) {
    return new Response(JSON.stringify({
      error: session.error,
      message: session.message,
      retryAfter: session.retryAfter,
    }), { status: session.status || 403, headers: JSON_HEADERS });
  }

  return new Response(JSON.stringify({
    ok: true,
    token: session.token,
    expiresIn: session.expiresIn,
    expiresAt: session.expiresAt,
    sessionMode: session.sessionMode,
    plan: 'member',
    message: '会员已解锁。当前会员码同一时间仅允许一个 IP 使用，30 分钟无操作后可重新接管。',
  }), { status: 200, headers: JSON_HEADERS });
}
