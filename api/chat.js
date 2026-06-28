const { Readable } = require('node:stream');

const DEFAULT_BASE_URL = 'https://api.deepseek.com';
const DEFAULT_MODEL = 'deepseek-chat';
const DEFAULT_MAX_INPUT_CHARS = 12000;
const DEFAULT_MAX_OUTPUT_TOKENS = 2200;
const DEFAULT_TIMEOUT_MS = 45000;
const ALLOWED_ROLES = new Set(['system', 'user', 'assistant']);

function cleanEnv(value) {
  return String(value || '').replace(/^\uFEFF/, '').trim();
}

function parseAllowedOrigins() {
  return cleanEnv(process.env.ALLOWED_ORIGINS)
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
}

function setCors(req, res) {
  const allowedOrigins = parseAllowedOrigins();
  const origin = req.headers.origin;
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (!allowedOrigins.length) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    return true;
  }

  if (!origin) return true;
  if (allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    return true;
  }

  return false;
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

function fail(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  throw error;
}

function normalizePayload(rawBody) {
  const maxChars = Number(cleanEnv(process.env.MAX_INPUT_CHARS) || DEFAULT_MAX_INPUT_CHARS);
  rawBody = String(rawBody || '').replace(/^\uFEFF/, '');
  if (rawBody.length > maxChars) {
    fail(413, `Request body too large. Max ${maxChars} characters.`);
  }

  let input;
  try {
    input = JSON.parse(rawBody || '{}');
  } catch (_) {
    fail(400, 'Invalid JSON body');
  }

  if (!Array.isArray(input.messages) || !input.messages.length) {
    fail(400, 'messages is required');
  }
  if (input.messages.length > 8) {
    fail(400, 'Too many messages');
  }

  let contentChars = 0;
  const messages = input.messages.map((message) => {
    const role = String(message?.role || '').trim();
    const content = String(message?.content || '').trim();
    if (!ALLOWED_ROLES.has(role)) fail(400, 'Invalid message role');
    if (!content) fail(400, 'Message content is required');
    if (content.length > 6000) fail(400, 'Single message is too long');
    contentChars += content.length;
    return { role, content };
  });

  if (contentChars > maxChars) {
    fail(413, `Message content too large. Max ${maxChars} characters.`);
  }

  const maxOutputTokens = Number(cleanEnv(process.env.MAX_OUTPUT_TOKENS) || DEFAULT_MAX_OUTPUT_TOKENS);
  const requestedMaxTokens = Number(input.max_tokens || 1800);
  const maxTokens = Math.min(Math.max(Number.isFinite(requestedMaxTokens) ? requestedMaxTokens : 1800, 1), maxOutputTokens);
  const requestedTemperature = Number(input.temperature ?? 0.65);
  const temperature = Math.min(Math.max(Number.isFinite(requestedTemperature) ? requestedTemperature : 0.65, 0), 1);

  return JSON.stringify({
    model: cleanEnv(process.env.DS_MODEL) || DEFAULT_MODEL,
    messages,
    stream: Boolean(input.stream),
    max_tokens: maxTokens,
    temperature,
  });
}

function pipeWebStream(webStream, res) {
  return new Promise((resolve, reject) => {
    const nodeStream = Readable.fromWeb(webStream);
    nodeStream.on('error', reject);
    res.on('error', reject);
    res.on('finish', resolve);
    nodeStream.pipe(res);
  });
}

module.exports = async function handler(req, res) {
  const corsAllowed = setCors(req, res);

  if (req.method === 'OPTIONS') {
    res.status(corsAllowed ? 204 : 403).end();
    return;
  }

  if (!corsAllowed) {
    res.status(403).json({ error: 'Origin not allowed' });
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
  const timeoutMs = Number(cleanEnv(process.env.CHAT_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  req.on('aborted', () => controller.abort());

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

    await pipeWebStream(upstream.body, res);
  } catch (error) {
    const isAbort = error.name === 'AbortError';
    const statusCode = error.statusCode || (isAbort ? 504 : 502);
    if (!res.headersSent) {
      res.status(statusCode).json({
        error: isAbort ? 'DeepSeek request timed out' : 'DeepSeek request failed',
        message: error.message,
      });
    } else {
      res.end();
    }
  } finally {
    clearTimeout(timeout);
  }
};
