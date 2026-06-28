const { Readable } = require('node:stream');

const DEFAULT_BASE_URL = 'https://api.deepseek.com';
const DEFAULT_MODEL = 'deepseek-chat';

function cleanEnv(value) {
  return String(value || '').replace(/^\uFEFF/, '').trim();
}

function setCors(req, res) {
  const allowedOrigins = (process.env.ALLOWED_ORIGINS || '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
  const origin = req.headers.origin;

  if (!allowedOrigins.length) {
    res.setHeader('Access-Control-Allow-Origin', '*');
  } else if (origin && allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }

  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function readBody(req) {
  if (req.body) {
    return Promise.resolve(
      typeof req.body === 'string' ? req.body : JSON.stringify(req.body)
    );
  }

  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function normalizePayload(rawBody) {
  const maxChars = Number(process.env.MAX_INPUT_CHARS || 12000);
  rawBody = String(rawBody || '').replace(/^\uFEFF/, '');
  if (rawBody.length > maxChars) {
    const error = new Error(`Request body too large. Max ${maxChars} characters.`);
    error.statusCode = 413;
    throw error;
  }

  let payload;
  try {
    payload = JSON.parse(rawBody || '{}');
  } catch (_) {
    const error = new Error('Invalid JSON body');
    error.statusCode = 400;
    throw error;
  }

  if (!Array.isArray(payload.messages) || !payload.messages.length) {
    const error = new Error('messages is required');
    error.statusCode = 400;
    throw error;
  }

  payload.model = cleanEnv(process.env.DS_MODEL) || payload.model || DEFAULT_MODEL;
  return JSON.stringify(payload);
}

module.exports = async function handler(req, res) {
  setCors(req, res);

  if (req.method === 'OPTIONS') {
    res.status(204).end();
    return;
  }

  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST, OPTIONS');
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  const apiKey = cleanEnv(process.env.DS_KEY);
  if (!apiKey) {
    res.status(500).json({ error: 'Missing DS_KEY environment variable' });
    return;
  }

  const controller = new AbortController();
  const timeoutMs = Number(process.env.CHAT_TIMEOUT_MS || 45000);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const rawBody = await readBody(req);
    const body = normalizePayload(rawBody);
    const baseUrl = (cleanEnv(process.env.DS_BASE_URL) || DEFAULT_BASE_URL).replace(/\/$/, '');
    const upstream = await fetch(`${baseUrl}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body,
      signal: controller.signal,
    });

    res.status(upstream.status);
    const contentType = upstream.headers.get('content-type');
    if (contentType) res.setHeader('Content-Type', contentType);
    res.setHeader('Cache-Control', 'no-cache, no-transform');

    if (!upstream.body) {
      res.end();
      return;
    }

    Readable.fromWeb(upstream.body).pipe(res);
  } catch (error) {
    const isAbort = error.name === 'AbortError';
    const statusCode = error.statusCode || (isAbort ? 504 : 502);
    res.status(statusCode).json({
      error: isAbort ? 'DeepSeek request timed out' : 'DeepSeek request failed',
      message: error.message,
    });
  } finally {
    clearTimeout(timeout);
  }
};
