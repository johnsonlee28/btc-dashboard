const DEFAULT_TTL_SECONDS = 1800;
const MAX_SESSION_LIFETIME_SECONDS = 24 * 60 * 60;
const DEFAULT_TRIAL_DAYS = 30;
const SESSION_PREFIX = 'stock-member-session:';
const TRIAL_PREFIX = 'stock-member-trial:';
const GENERIC_CODE_UNAVAILABLE = '兑换码无效、已停用，或正在其他 IP 使用。';

function textEncoder() {
  return new TextEncoder();
}

function bytesToHex(buffer) {
  return [...new Uint8Array(buffer)].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function sha256Hex(value) {
  const digest = await crypto.subtle.digest('SHA-256', textEncoder().encode(String(value || '')));
  return bytesToHex(digest);
}

function ttlSeconds() {
  const n = Number(process.env.MEMBER_SESSION_TTL_SECONDS || DEFAULT_TTL_SECONDS);
  return Number.isFinite(n) && n >= 60 ? Math.floor(n) : DEFAULT_TTL_SECONDS;
}

function trialDurationSeconds() {
  const days = Number(process.env.MEMBER_TRIAL_DAYS || DEFAULT_TRIAL_DAYS);
  const safeDays = Number.isFinite(days) && days >= 1 && days <= 365 ? Math.floor(days) : DEFAULT_TRIAL_DAYS;
  return safeDays * 24 * 60 * 60;
}

export function memberCodes() {
  return (process.env.MEMBER_LICENSE_CODES || process.env.MEMBER_TOKENS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

export function trialCodes() {
  return (process.env.MEMBER_TRIAL_CODES || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

export function getBearerToken(req) {
  const auth = req.headers.get('authorization') || '';
  if (!auth.startsWith('Bearer ')) return '';
  return auth.slice(7).trim();
}

export function getDeviceId(req) {
  return String(req.headers.get('x-device-id') || '').trim().slice(0, 80);
}

export function getClientIp(req) {
  const forwarded = req.headers.get('x-forwarded-for') || '';
  const firstForwarded = forwarded.split(',')[0]?.trim();
  return (
    req.headers.get('cf-connecting-ip') ||
    req.headers.get('x-real-ip') ||
    firstForwarded ||
    'unknown'
  ).trim().slice(0, 120);
}

function getUserAgent(req) {
  return String(req.headers.get('user-agent') || 'unknown').slice(0, 240);
}

function kvConfig() {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
  return { url, token, enabled: Boolean(url && token) };
}

async function redisCommand(command) {
  const { url, token, enabled } = kvConfig();
  if (!enabled) throw new Error('kv_missing');
  const res = await fetch(`${url}/pipeline`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify([command]),
    signal: AbortSignal.timeout(4500),
  });
  if (!res.ok) throw new Error(`kv_http_${res.status}`);
  const data = await res.json();
  const first = Array.isArray(data) ? data[0] : data;
  if (first?.error) throw new Error(`kv_error_${first.error}`);
  return first?.result;
}

async function kvGet(key) {
  const raw = await redisCommand(['GET', key]);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function kvSetEx(key, seconds, value) {
  await redisCommand(['SET', key, JSON.stringify(value), 'EX', String(seconds)]);
}

async function kvSetNx(key, value) {
  const result = await redisCommand(['SET', key, JSON.stringify(value), 'NX']);
  return result === 'OK';
}

function sessionKey(codeHash) {
  return `${SESSION_PREFIX}${codeHash}`;
}

function trialKey(codeHash) {
  return `${TRIAL_PREFIX}${codeHash}`;
}

async function codeHashes(codes) {
  return Promise.all(codes.map(code => sha256Hex(code)));
}

async function classifyCodeByHash(codeHash) {
  const [regularHashes, trialHashes] = await Promise.all([
    codeHashes(memberCodes()),
    codeHashes(trialCodes()),
  ]);
  const isRegular = regularHashes.includes(codeHash);
  const isTrial = trialHashes.includes(codeHash);
  if (isRegular && isTrial) return 'conflict';
  if (isRegular) return 'regular';
  if (isTrial) return 'trial';
  return null;
}

async function ensureTrialRecord(codeHash, now, ipHash) {
  const recordKey = trialKey(codeHash);
  const existing = await kvGet(recordKey);
  if (existing?.expiresAt) {
    if (existing.expiresAt <= now) return { ok: false, expired: true };
    return { ok: true, expiresAt: existing.expiresAt, activatedAt: existing.activatedAt };
  }

  const duration = trialDurationSeconds();
  const record = {
    kind: 'trial',
    activatedAt: now,
    expiresAt: now + duration,
    firstIpHash: ipHash,
  };
  const created = await kvSetNx(recordKey, record);
  if (created) return { ok: true, expiresAt: record.expiresAt, activatedAt: record.activatedAt };

  const current = await kvGet(recordKey);
  if (!current?.expiresAt || current.expiresAt <= now) return { ok: false, expired: true };
  return { ok: true, expiresAt: current.expiresAt, activatedAt: current.activatedAt };
}

async function verifyTrialRecord(codeHash, now) {
  const record = await kvGet(trialKey(codeHash));
  if (!record?.expiresAt || record.expiresAt <= now) return { ok: false };
  return { ok: true, expiresAt: record.expiresAt, activatedAt: record.activatedAt };
}

function parseMemberToken(token) {
  const parts = String(token || '').split('.');
  if (parts.length !== 3 || parts[0] !== 'v2') return null;
  const codeHash = parts[1];
  const sid = parts[2];
  if (!/^[a-f0-9]{64}$/.test(codeHash) || !/^[A-Za-z0-9-]{20,80}$/.test(sid)) return null;
  return { codeHash, sid };
}

export async function createMemberSession(req, code, deviceId) {
  if (!kvConfig().enabled) {
    return { ok: false, status: 503, error: 'session_store_missing', message: '会员会话存储未配置，暂时无法启用单 IP 在线。' };
  }

  const normalizedCode = String(code || '').trim();
  const ttl = ttlSeconds();
  const now = Math.floor(Date.now() / 1000);
  const codeHash = await sha256Hex(normalizedCode);
  const ipHash = await sha256Hex(getClientIp(req));
  const uaHash = await sha256Hex(getUserAgent(req));
  const safeDeviceId = String(deviceId || '').trim().slice(0, 80) || crypto.randomUUID();
  const key = sessionKey(codeHash);
  const existing = await kvGet(key);
  const codeKind = await classifyCodeByHash(codeHash);

  if (!codeKind || codeKind === 'conflict') {
    return { ok: false, status: 403, error: 'code_unavailable', message: GENERIC_CODE_UNAVAILABLE };
  }

  let trialExpiresAt = null;
  let trialActivatedAt = null;
  if (codeKind === 'trial') {
    const trial = await ensureTrialRecord(codeHash, now, ipHash);
    if (!trial.ok) {
      return { ok: false, status: 403, error: 'code_unavailable', message: GENERIC_CODE_UNAVAILABLE };
    }
    trialExpiresAt = trial.expiresAt;
    trialActivatedAt = trial.activatedAt;
  }

  if (existing?.sid && existing?.expiresAt > now) {
    const sameIp = existing.ipHash === ipHash;
    if (!sameIp) {
      return {
        ok: false,
        status: 403,
        error: 'code_unavailable',
        message: GENERIC_CODE_UNAVAILABLE,
      };
    }
  }

  const sid = crypto.randomUUID();
  const sessionAbsoluteExpiresAt = Math.min(now + MAX_SESSION_LIFETIME_SECONDS, trialExpiresAt || now + MAX_SESSION_LIFETIME_SECONDS);
  const session = {
    sid,
    codeHash,
    ipHash,
    uaHash,
    deviceId: safeDeviceId,
    createdAt: now,
    lastSeen: now,
    expiresAt: Math.min(now + ttl, sessionAbsoluteExpiresAt),
    absoluteExpiresAt: sessionAbsoluteExpiresAt,
    kind: codeKind,
    trialActivatedAt: trialActivatedAt || undefined,
    trialExpiresAt: trialExpiresAt || undefined,
  };
  await kvSetEx(key, ttl, session);
  return {
    ok: true,
    token: `v2.${codeHash}.${sid}`,
    expiresIn: ttl,
    expiresAt: session.expiresAt,
    sessionMode: 'single_active_ip',
    plan: codeKind === 'trial' ? 'trial' : 'member',
    trialActivatedAt: trialActivatedAt || undefined,
    trialExpiresAt: trialExpiresAt || undefined,
  };
}

export async function verifyMemberRequest(req) {
  if (!kvConfig().enabled) {
    return { ok: false, status: 503, error: 'session_store_missing', message: '会员会话存储未配置，暂时无法查询个股。' };
  }

  const parsed = parseMemberToken(getBearerToken(req));
  if (!parsed) {
    return { ok: false, status: 402, error: 'member_required', message: '个股派发/承接线索为会员功能。大盘证据链继续免费开放。', upgradeUrl: '/pricing' };
  }

  const now = Math.floor(Date.now() / 1000);
  const codeKind = await classifyCodeByHash(parsed.codeHash);
  if (!codeKind || codeKind === 'conflict') {
    return { ok: false, status: 403, error: 'session_expired', message: '会员会话已过期，请重新兑换。', upgradeUrl: '/pricing' };
  }

  if (codeKind === 'trial') {
    const trial = await verifyTrialRecord(parsed.codeHash, now);
    if (!trial.ok) {
      return { ok: false, status: 403, error: 'session_expired', message: '试用已到期，请重新兑换正式会员。', upgradeUrl: '/pricing' };
    }
  }

  const session = await kvGet(sessionKey(parsed.codeHash));
  if (!session || session.sid !== parsed.sid || session.expiresAt <= now || session.absoluteExpiresAt <= now) {
    return { ok: false, status: 403, error: 'session_expired', message: '会员会话已过期，请重新兑换。', upgradeUrl: '/pricing' };
  }

  const ipHash = await sha256Hex(getClientIp(req));
  const sameIp = session.ipHash === ipHash;
  if (!sameIp) {
    return { ok: false, status: 409, error: 'session_ip_mismatch', message: '该会员码正在其他 IP 使用。若是本人切换网络，请重新兑换接管。', upgradeUrl: '/pricing' };
  }

  const ttl = ttlSeconds();
  const nextExpiry = Math.min(now + ttl, session.absoluteExpiresAt || now + ttl);
  const refreshed = { ...session, lastSeen: now, expiresAt: nextExpiry };
  await kvSetEx(sessionKey(parsed.codeHash), ttl, refreshed);
  return { ok: true, session: refreshed };
}
