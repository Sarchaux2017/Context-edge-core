/**
 * Vaultcore :: Plexroute Pipeline
 * ---------------------------------------------------------------------------
 * Transparent inline reverse-proxy for OpenAI (/v1/chat/completions) and
 * Anthropic (/v1/messages) traffic. Eligible requests are answered locally by
 * the Purecore Engine worker pool; everything else is forwarded upstream with
 * the original bytes and client credentials untouched.
 *
 * Privacy note: message content is never logged or persisted by this process.
 *
 * @typedef {'openai' | 'anthropic'} Dialect
 * @typedef {{ total: number, byType: Record<string, number> }} PhiSummary
 * @typedef {{
 *   id: number,
 *   routedLocally: boolean,
 *   intent: string,
 *   reason: string,
 *   output: string,
 *   latencyMs: number,
 *   estimatedTokensSaved: number,
 *   phiEntities: PhiSummary
 * }} EngineResponse
 * @typedef {{ eligible: true, text: string } | { eligible: false, reason: string }} Inspection
 * @typedef {{ model: string, stream: boolean, includeUsage: boolean, maxTokens: number | null }} LocalRequestInfo
 * @typedef {{ local: false, reason: string } | { local: true, reason: string, result: EngineResponse, request: LocalRequestInfo }} Decision
 * @typedef {{
 *   id: number,
 *   text: string,
 *   resolve: (result: EngineResponse) => void,
 *   settled: boolean,
 *   timer: NodeJS.Timeout | null,
 *   slot: WorkerSlot | null
 * }} PoolTask
 * @typedef {{
 *   index: number,
 *   worker: Worker | null,
 *   ready: boolean,
 *   current: PoolTask | null,
 *   failures: number,
 *   respawnTimer: NodeJS.Timeout | null
 * }} WorkerSlot
 */

import 'dotenv/config';
import { availableParallelism, cpus } from 'node:os';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import { randomBytes } from 'node:crypto';
import { pipeline } from 'node:stream';
import express from 'express';
import fetch from 'node-fetch';

/* -------------------------------------------------------------------------- */
/* Configuration                                                              */
/* -------------------------------------------------------------------------- */

/**
 * @param {string} name
 * @param {number} fallback
 * @param {number} min
 * @param {number} max
 * @returns {number}
 */
function intEnv(name, fallback, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

/**
 * @param {string} name
 * @param {boolean} fallback
 * @returns {boolean}
 */
function boolEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  return !['0', 'false', 'no', 'off'].includes(raw.trim().toLowerCase());
}

/** @param {string} url */
function trimTrailingSlash(url) {
  return url.replace(/\/+$/, '');
}

const cpuLimit = typeof availableParallelism === 'function' ? availableParallelism() : cpus().length;

const CONFIG = Object.freeze({
  port: intEnv('PORT', 8080, 1, 65535),
  host: process.env.HOST || '0.0.0.0',
  workerPoolSize: intEnv('WORKER_POOL_SIZE', Math.max(1, Math.min(4, cpuLimit)), 1, 64),
  workerTimeoutMs: intEnv('WORKER_TIMEOUT_MS', 250, 1, 60_000),
  workerQueueLimit: intEnv('WORKER_QUEUE_LIMIT', 1024, 1, 100_000),
  localRouting: boolEnv('LOCAL_ROUTING_ENABLED', true),
  localMaxChars: intEnv('LOCAL_MAX_CHARS', 200_000, 1, 10_000_000),
  bodyLimitBytes: intEnv('BODY_LIMIT_BYTES', 10 * 1024 * 1024, 1024, 256 * 1024 * 1024),
  upstreamTimeoutMs: intEnv('UPSTREAM_TIMEOUT_MS', 120_000, 1000, 3_600_000),
  shutdownGraceMs: intEnv('SHUTDOWN_GRACE_MS', 10_000, 100, 120_000),
  openaiUpstream: trimTrailingSlash(process.env.OPENAI_UPSTREAM_URL || 'https://api.openai.com'),
  anthropicUpstream: trimTrailingSlash(process.env.ANTHROPIC_UPSTREAM_URL || 'https://api.anthropic.com'),
});

const FINGERPRINT = 'vaultcore-local';
const WORKER_URL = new URL('./localEngine.js', import.meta.url);

/* -------------------------------------------------------------------------- */
/* Logging + stats                                                            */
/* -------------------------------------------------------------------------- */

/**
 * Structured logger. Callers must never pass message content.
 * @param {'info' | 'warn' | 'error'} level
 * @param {string} msg
 * @param {Record<string, unknown>} [extra]
 */
function log(level, msg, extra = {}) {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, msg, ...extra });
  if (level === 'error') console.error(line);
  else console.log(line);
}

const startedAt = Date.now();

const stats = {
  total: 0,
  local: 0,
  forwarded: 0,
  upstreamErrors: 0,
  tokensSaved: 0,
  phiEntitiesDetected: 0,
  byDialect: { openai: 0, anthropic: 0 },
  /** @type {Record<string, number>} */
  reasons: Object.create(null),
};

/** @param {string} reason */
function countReason(reason) {
  stats.reasons[reason] = (stats.reasons[reason] ?? 0) + 1;
}

/* -------------------------------------------------------------------------- */
/* WorkerPool                                                                 */
/* -------------------------------------------------------------------------- */

/**
 * @param {string} reason
 * @returns {EngineResponse}
 */
function poolFallback(reason) {
  return {
    id: 0,
    routedLocally: false,
    intent: 'none',
    reason,
    output: '',
    latencyMs: 0,
    estimatedTokensSaved: 0,
    phiEntities: { total: 0, byType: {} },
  };
}

/**
 * @param {unknown} msg
 * @returns {msg is EngineResponse}
 */
function isEngineResponse(msg) {
  if (typeof msg !== 'object' || msg === null) return false;
  const m = /** @type {Record<string, unknown>} */ (msg);
  return (
    typeof m.id === 'number' &&
    typeof m.routedLocally === 'boolean' &&
    typeof m.intent === 'string' &&
    typeof m.reason === 'string' &&
    typeof m.output === 'string' &&
    typeof m.latencyMs === 'number' &&
    typeof m.estimatedTokensSaved === 'number'
  );
}

class WorkerPool {
  /**
   * @param {{ size: number, timeoutMs: number, workerUrl: URL, maxQueue: number }} options
   */
  constructor({ size, timeoutMs, workerUrl, maxQueue }) {
    this.size = size;
    this.timeoutMs = timeoutMs;
    this.workerUrl = workerUrl;
    this.maxQueue = maxQueue;
    /** @type {WorkerSlot[]} */
    this.slots = [];
    /** @type {PoolTask[]} */
    this.queue = [];
    this.nextTaskId = 1;
    this.closed = false;
    this.metrics = { completed: 0, timeouts: 0, crashes: 0, recycled: 0, queueRejections: 0 };

    for (let i = 0; i < size; i += 1) {
      /** @type {WorkerSlot} */
      const slot = { index: i, worker: null, ready: false, current: null, failures: 0, respawnTimer: null };
      this.slots.push(slot);
      this.spawn(slot);
    }
  }

  /**
   * Runs one text through the Purecore Engine. Never rejects: failures,
   * timeouts and crashes resolve to a routedLocally:false verdict so the
   * caller can forward upstream.
   * @param {string} text
   * @returns {Promise<EngineResponse>}
   */
  run(text) {
    return new Promise((resolve) => {
      if (this.closed) {
        resolve(poolFallback('pool-closed'));
        return;
      }
      if (this.queue.length >= this.maxQueue) {
        this.metrics.queueRejections += 1;
        resolve(poolFallback('queue-full'));
        return;
      }
      /** @type {PoolTask} */
      const task = { id: this.nextTaskId, text, resolve, settled: false, timer: null, slot: null };
      this.nextTaskId = this.nextTaskId >= Number.MAX_SAFE_INTEGER ? 1 : this.nextTaskId + 1;
      task.timer = setTimeout(() => this.onTimeout(task), this.timeoutMs);
      this.queue.push(task);
      this.dispatch();
    });
  }

  /** @returns {{ size: number, ready: number, busy: number, queued: number, completed: number, timeouts: number, crashes: number, recycled: number, queueRejections: number }} */
  snapshot() {
    return {
      size: this.size,
      ready: this.slots.filter((s) => s.ready).length,
      busy: this.slots.filter((s) => s.current !== null).length,
      queued: this.queue.length,
      ...this.metrics,
    };
  }

  /** @returns {Promise<void>} */
  async close() {
    if (this.closed) return;
    this.closed = true;

    for (const task of this.queue.splice(0)) this.settle(task, poolFallback('pool-closed'));

    const terminations = [];
    for (const slot of this.slots) {
      if (slot.respawnTimer) clearTimeout(slot.respawnTimer);
      slot.respawnTimer = null;
      if (slot.current) {
        this.settle(slot.current, poolFallback('pool-closed'));
        slot.current = null;
      }
      const worker = slot.worker;
      slot.worker = null;
      slot.ready = false;
      if (worker) terminations.push(worker.terminate().catch(() => 0));
    }
    await Promise.allSettled(terminations);
  }

  /** @param {WorkerSlot} slot */
  spawn(slot) {
    if (this.closed) return;
    slot.respawnTimer = null;

    /** @type {Worker} */
    let worker;
    try {
      worker = new Worker(this.workerUrl);
    } catch (err) {
      this.metrics.crashes += 1;
      log('error', 'worker-spawn-failed', { slot: slot.index, error: err instanceof Error ? err.message : String(err) });
      this.scheduleRespawn(slot, true);
      return;
    }

    slot.worker = worker;
    slot.ready = true;

    worker.on('message', (msg) => this.onMessage(slot, worker, msg));
    worker.on('error', (err) => {
      log('error', 'worker-error', { slot: slot.index, error: err instanceof Error ? err.message : String(err) });
      this.onFailure(slot, worker);
    });
    worker.on('messageerror', () => this.onFailure(slot, worker));
    worker.on('exit', () => this.onFailure(slot, worker));

    this.dispatch();
  }

  /**
   * @param {WorkerSlot} slot
   * @param {boolean} countFailure
   */
  scheduleRespawn(slot, countFailure) {
    if (this.closed) return;
    if (countFailure) slot.failures += 1;
    const delay = slot.failures <= 1 ? 0 : Math.min(5000, 50 * 2 ** (slot.failures - 1));
    if (slot.respawnTimer) clearTimeout(slot.respawnTimer);
    slot.respawnTimer = setTimeout(() => this.spawn(slot), delay);
  }

  dispatch() {
    for (const slot of this.slots) {
      if (!slot.ready || slot.current !== null || slot.worker === null) continue;

      let task = this.queue.shift();
      while (task && task.settled) task = this.queue.shift();
      if (!task) return;

      slot.current = task;
      task.slot = slot;
      try {
        slot.worker.postMessage({ id: task.id, text: task.text });
      } catch {
        slot.current = null;
        task.slot = null;
        this.queue.unshift(task);
        this.onFailure(slot, slot.worker);
      }
    }
  }

  /**
   * @param {WorkerSlot} slot
   * @param {Worker} worker
   * @param {unknown} msg
   */
  onMessage(slot, worker, msg) {
    if (slot.worker !== worker) return;
    const task = slot.current;
    if (!task || !isEngineResponse(msg) || msg.id !== task.id) return;

    slot.current = null;
    slot.failures = 0;
    this.metrics.completed += 1;
    this.settle(task, {
      ...msg,
      phiEntities: msg.phiEntities ?? { total: 0, byType: {} },
    });
    this.dispatch();
  }

  /**
   * Crash / unexpected-exit handler: fails the in-flight task and recycles the
   * slot. Stale events from already-replaced workers are ignored.
   * @param {WorkerSlot} slot
   * @param {Worker | null} worker
   */
  onFailure(slot, worker) {
    if (worker === null || slot.worker !== worker) return;
    this.metrics.crashes += 1;

    const task = slot.current;
    slot.current = null;
    slot.worker = null;
    slot.ready = false;
    void worker.terminate().catch(() => 0);

    if (task) this.settle(task, poolFallback('worker-crash'));
    this.scheduleRespawn(slot, true);
  }

  /** @param {PoolTask} task */
  onTimeout(task) {
    if (task.settled) return;
    this.metrics.timeouts += 1;

    const slot = task.slot;
    if (slot && slot.current === task) {
      // The worker may be wedged; discard it and bring up a fresh one.
      const worker = slot.worker;
      slot.current = null;
      slot.worker = null;
      slot.ready = false;
      if (worker) void worker.terminate().catch(() => 0);
      this.metrics.recycled += 1;
      this.scheduleRespawn(slot, false);
    } else {
      const idx = this.queue.indexOf(task);
      if (idx >= 0) this.queue.splice(idx, 1);
    }

    this.settle(task, poolFallback('worker-timeout'));
  }

  /**
   * @param {PoolTask} task
   * @param {EngineResponse} result
   */
  settle(task, result) {
    if (task.settled) return;
    task.settled = true;
    if (task.timer) clearTimeout(task.timer);
    task.timer = null;
    task.resolve(result);
  }
}

const pool = new WorkerPool({
  size: CONFIG.workerPoolSize,
  timeoutMs: CONFIG.workerTimeoutMs,
  workerUrl: WORKER_URL,
  maxQueue: CONFIG.workerQueueLimit,
});

/* -------------------------------------------------------------------------- */
/* Request body inspector                                                     */
/* -------------------------------------------------------------------------- */

const COMMON_DISQUALIFIERS = ['tools', 'tool_choice', 'response_format', 'logprobs'];

/** @type {Record<Dialect, string[]>} */
const DIALECT_DISQUALIFIERS = {
  openai: ['top_logprobs', 'functions', 'function_call', 'audio', 'prediction', 'web_search_options'],
  anthropic: ['mcp_servers', 'thinking', 'container'],
};

const TOOL_PART_TYPES = new Set([
  'tool_use',
  'tool_result',
  'server_tool_use',
  'mcp_tool_use',
  'mcp_tool_result',
  'web_search_tool_result',
]);

/** @param {unknown} v */
function isPlainObject(v) {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/**
 * @param {string} key
 * @param {unknown} value
 * @returns {boolean}
 */
function isDisqualifyingValue(key, value) {
  if (value === undefined || value === null || value === false || value === 0) return false;
  if (Array.isArray(value) && value.length === 0) return false;
  if (key === 'response_format' && isPlainObject(value) && value.type === 'text') return false;
  if (key === 'thinking' && isPlainObject(value) && value.type === 'disabled') return false;
  return true;
}

/**
 * @param {unknown} content
 * @returns {string | null}
 */
function extractText(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return null;
  const parts = [];
  for (const part of content) {
    if (isPlainObject(part) && part.type === 'text' && typeof part.text === 'string') parts.push(part.text);
    else return null;
  }
  return parts.join('\n');
}

/**
 * @param {unknown} message
 * @returns {boolean}
 */
function hasToolArtifacts(message) {
  if (!isPlainObject(message)) return false;
  if (message.role === 'tool' || message.role === 'function') return true;
  if (message.tool_calls || message.function_call) return true;
  if (Array.isArray(message.content)) {
    return message.content.some((p) => isPlainObject(p) && typeof p.type === 'string' && TOOL_PART_TYPES.has(p.type));
  }
  return false;
}

/**
 * Decides whether a parsed request body may be considered for local handling
 * and extracts the last user message string.
 * @param {Dialect} dialect
 * @param {unknown} body
 * @returns {Inspection}
 */
function inspectRequest(dialect, body) {
  if (!isPlainObject(body)) return { eligible: false, reason: 'invalid-body' };

  for (const key of [...COMMON_DISQUALIFIERS, ...DIALECT_DISQUALIFIERS[dialect]]) {
    if (isDisqualifyingValue(key, body[key])) return { eligible: false, reason: `disqualified:${key}` };
  }
  if (typeof body.n === 'number' && body.n > 1) return { eligible: false, reason: 'disqualified:n' };

  const messages = body.messages;
  if (!Array.isArray(messages) || messages.length === 0) return { eligible: false, reason: 'no-messages' };
  if (messages.some(hasToolArtifacts)) return { eligible: false, reason: 'tool-history' };

  const last = messages[messages.length - 1];
  if (!isPlainObject(last) || last.role !== 'user') return { eligible: false, reason: 'last-message-not-user' };

  const text = extractText(last.content);
  if (text === null) return { eligible: false, reason: 'non-text-content' };
  if (text.trim().length === 0) return { eligible: false, reason: 'empty-user-text' };

  return { eligible: true, text };
}

/* -------------------------------------------------------------------------- */
/* Local (mock AI) response builders                                          */
/* -------------------------------------------------------------------------- */

/** @param {string} prefix */
function newId(prefix) {
  return `${prefix}${randomBytes(12).toString('hex')}`;
}

/**
 * Surrogate-pair-safe chunking for SSE deltas.
 * @param {string} text
 * @param {number} [size]
 * @returns {string[]}
 */
function chunkText(text, size = 200) {
  const out = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + size, text.length);
    if (end < text.length) {
      const code = text.charCodeAt(end - 1);
      if (code >= 0xd800 && code <= 0xdbff) end += 1;
    }
    out.push(text.slice(i, end));
    i = end;
  }
  return out;
}

const OPENAI_ZERO_USAGE = Object.freeze({ prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 });
const ANTHROPIC_ZERO_USAGE = Object.freeze({ input_tokens: 0, output_tokens: 0 });

/**
 * @param {string} model
 * @param {string} output
 */
function openAiBody(model, output) {
  return {
    id: newId('chatcmpl-vc-'),
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    system_fingerprint: FINGERPRINT,
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content: output, refusal: null },
        logprobs: null,
        finish_reason: 'stop',
      },
    ],
    usage: { ...OPENAI_ZERO_USAGE },
  };
}

/**
 * @param {string} model
 * @param {string} output
 * @param {boolean} includeUsage
 * @returns {string[]}
 */
function openAiFrames(model, output, includeUsage) {
  const base = {
    id: newId('chatcmpl-vc-'),
    object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000),
    model,
    system_fingerprint: FINGERPRINT,
  };
  /**
   * @param {unknown[]} choices
   * @param {Record<string, unknown>} [extra]
   */
  const frame = (choices, extra = {}) => `data: ${JSON.stringify({ ...base, choices, ...extra })}\n\n`;
  const choice = (/** @type {Record<string, unknown>} */ delta, /** @type {string | null} */ finish) => ({
    index: 0,
    delta,
    logprobs: null,
    finish_reason: finish,
  });

  const frames = [frame([choice({ role: 'assistant', content: '' }, null)])];
  for (const piece of chunkText(output)) frames.push(frame([choice({ content: piece }, null)]));
  frames.push(frame([choice({}, 'stop')]));
  if (includeUsage) frames.push(frame([], { usage: { ...OPENAI_ZERO_USAGE } }));
  frames.push('data: [DONE]\n\n');
  return frames;
}

/**
 * @param {string} model
 * @param {string} output
 */
function anthropicBody(model, output) {
  return {
    id: newId('msg_vc_'),
    type: 'message',
    role: 'assistant',
    model,
    system_fingerprint: FINGERPRINT,
    content: [{ type: 'text', text: output }],
    stop_reason: 'end_turn',
    stop_sequence: null,
    usage: { ...ANTHROPIC_ZERO_USAGE },
  };
}

/**
 * @param {string} model
 * @param {string} output
 * @returns {string[]}
 */
function anthropicFrames(model, output) {
  /**
   * @param {string} event
   * @param {Record<string, unknown>} data
   */
  const sse = (event, data) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

  const frames = [
    sse('message_start', {
      type: 'message_start',
      message: {
        id: newId('msg_vc_'),
        type: 'message',
        role: 'assistant',
        model,
        system_fingerprint: FINGERPRINT,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { ...ANTHROPIC_ZERO_USAGE },
      },
    }),
    sse('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }),
    sse('ping', { type: 'ping' }),
  ];
  for (const piece of chunkText(output)) {
    frames.push(
      sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: piece } })
    );
  }
  frames.push(
    sse('content_block_stop', { type: 'content_block_stop', index: 0 }),
    sse('message_delta', {
      type: 'message_delta',
      delta: { stop_reason: 'end_turn', stop_sequence: null },
      usage: { output_tokens: 0 },
    }),
    sse('message_stop', { type: 'message_stop' })
  );
  return frames;
}

/* -------------------------------------------------------------------------- */
/* Telemetry + error helpers                                                  */
/* -------------------------------------------------------------------------- */

/**
 * @param {import('express').Response} res
 * @param {{ local: boolean, latencyMs: number, tokensSaved: number, reason: string }} t
 */
function setTelemetry(res, { local, latencyMs, tokensSaved, reason }) {
  res.setHeader('X-Vaultcore-Local', local ? 'true' : 'false');
  res.setHeader('X-Vaultcore-Latency-MS', latencyMs.toFixed(3));
  res.setHeader('X-Purecore-Tokens-Saved', String(Math.max(0, Math.trunc(tokensSaved))));
  res.setHeader('X-Plexroute-Route-Reason', reason.replace(/[^\x20-\x7e]/g, '_').slice(0, 120));
}

/**
 * @param {number} status
 * @param {string} code
 * @param {string} message
 * @returns {Error & { status: number, code: string }}
 */
function httpError(status, code, message) {
  return Object.assign(new Error(message), { status, code });
}

/**
 * @param {string} path
 * @param {string} type
 * @param {string} message
 * @param {string} code
 */
function errorBody(path, type, message, code) {
  if (path === '/v1/messages') return { type: 'error', error: { type, message } };
  return { error: { message, type, param: null, code } };
}

/**
 * Reads the raw request body (bytes untouched) with a hard size cap.
 * @param {import('express').Request} req
 * @param {number} limit
 * @returns {Promise<Buffer>}
 */
async function readBody(req, limit) {
  const declared = Number(req.headers['content-length']);
  if (Number.isFinite(declared) && declared > limit) {
    throw httpError(413, 'payload_too_large', `Request body exceeds ${limit} bytes.`);
  }
  /** @type {Buffer[]} */
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buf.length;
    if (size > limit) throw httpError(413, 'payload_too_large', `Request body exceeds ${limit} bytes.`);
    chunks.push(buf);
  }
  return Buffer.concat(chunks, size);
}

/* -------------------------------------------------------------------------- */
/* Routing decision                                                           */
/* -------------------------------------------------------------------------- */

/**
 * @param {string} reason
 * @returns {Decision}
 */
function passThrough(reason) {
  return { local: false, reason };
}

/**
 * @param {Dialect} dialect
 * @param {import('express').Request} req
 * @param {Buffer} raw
 * @returns {Promise<Decision>}
 */
async function decide(dialect, req, raw) {
  if (!CONFIG.localRouting) return passThrough('local-routing-disabled');

  const encoding = String(req.headers['content-encoding'] || 'identity').toLowerCase();
  if (encoding !== 'identity') return passThrough('encoded-body');
  if (raw.length === 0) return passThrough('empty-body');

  /** @type {unknown} */
  let body;
  try {
    body = JSON.parse(raw.toString('utf8'));
  } catch {
    return passThrough('invalid-json');
  }

  const inspection = inspectRequest(dialect, body);
  if (!inspection.eligible) return passThrough(inspection.reason);
  if (inspection.text.length > CONFIG.localMaxChars) return passThrough('payload-too-large-for-local');

  const result = await pool.run(inspection.text);
  if (!result.routedLocally) return passThrough(result.reason);

  const b = /** @type {Record<string, unknown>} */ (body);
  const limitRaw = typeof b.max_tokens === 'number' ? b.max_tokens : b.max_completion_tokens;
  const maxTokens = typeof limitRaw === 'number' && Number.isFinite(limitRaw) ? limitRaw : null;
  if (maxTokens !== null && Math.ceil(result.output.length / 4) > maxTokens) {
    return passThrough('output-exceeds-max-tokens');
  }

  const streamOptions = isPlainObject(b.stream_options) ? b.stream_options : null;
  return {
    local: true,
    reason: result.reason,
    result,
    request: {
      model: typeof b.model === 'string' && b.model ? b.model : dialect === 'openai' ? 'gpt-vaultcore-local' : 'claude-vaultcore-local',
      stream: b.stream === true,
      includeUsage: streamOptions?.include_usage === true,
      maxTokens,
    },
  };
}

/* -------------------------------------------------------------------------- */
/* Local responder                                                            */
/* -------------------------------------------------------------------------- */

/**
 * @param {Dialect} dialect
 * @param {import('express').Response} res
 * @param {Extract<Decision, { local: true }>} decision
 * @param {number} t0
 */
function sendLocal(dialect, res, decision, t0) {
  const { result, request } = decision;

  stats.local += 1;
  stats.tokensSaved += result.estimatedTokensSaved;
  stats.phiEntitiesDetected += result.phiEntities.total;
  countReason(decision.reason);

  setTelemetry(res, {
    local: true,
    latencyMs: performance.now() - t0,
    tokensSaved: result.estimatedTokensSaved,
    reason: decision.reason,
  });

  if (request.stream) {
    res.status(200);
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const frames =
      dialect === 'openai'
        ? openAiFrames(request.model, result.output, request.includeUsage)
        : anthropicFrames(request.model, result.output);
    res.end(frames.join(''));
    return;
  }

  res
    .status(200)
    .json(dialect === 'openai' ? openAiBody(request.model, result.output) : anthropicBody(request.model, result.output));
}

/* -------------------------------------------------------------------------- */
/* Upstream forwarder                                                         */
/* -------------------------------------------------------------------------- */

const REQUEST_HEADER_STRIP = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'host',
  'content-length',
  'expect',
]);

const RESPONSE_HEADER_STRIP = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'set-cookie',
]);

/**
 * Passes through every client header (authorization, x-api-key,
 * anthropic-version, openai-organization, ...) except hop-by-hop ones.
 * @param {import('node:http').IncomingHttpHeaders} incoming
 * @returns {Record<string, string>}
 */
function buildUpstreamHeaders(incoming) {
  /** @type {Record<string, string>} */
  const out = {};
  for (const [key, value] of Object.entries(incoming)) {
    if (value === undefined || REQUEST_HEADER_STRIP.has(key)) continue;
    out[key] = Array.isArray(value) ? value.join(', ') : value;
  }
  return out;
}

/**
 * @param {Dialect} dialect
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {Buffer} raw
 * @param {string} reason
 * @param {number} t0
 * @returns {Promise<void>}
 */
async function forward(dialect, req, res, raw, reason, t0) {
  const base = dialect === 'openai' ? CONFIG.openaiUpstream : CONFIG.anthropicUpstream;
  const target = `${base}${req.originalUrl}`;

  // Latency here is the time Vaultcore itself spent before handing off upstream.
  const overheadMs = performance.now() - t0;

  const controller = new AbortController();
  const headerTimer = setTimeout(() => controller.abort(), CONFIG.upstreamTimeoutMs);
  const onClose = () => {
    if (!res.writableFinished) controller.abort();
  };
  res.on('close', onClose);

  /** @type {import('node-fetch').Response} */
  let upstream;
  try {
    upstream = await fetch(target, {
      method: 'POST',
      headers: buildUpstreamHeaders(req.headers),
      body: raw.length > 0 ? raw : undefined,
      signal: controller.signal,
      compress: false,
      redirect: 'manual',
    });
  } catch (err) {
    clearTimeout(headerTimer);
    res.off('close', onClose);
    stats.upstreamErrors += 1;

    if (res.destroyed || res.headersSent) {
      res.destroy();
      return;
    }
    const aborted = err instanceof Error && err.name === 'AbortError';
    const failReason = aborted ? 'upstream-timeout' : 'upstream-unreachable';
    countReason(failReason);
    log('warn', failReason, { dialect, error: err instanceof Error ? err.message : String(err) });
    setTelemetry(res, { local: false, latencyMs: overheadMs, tokensSaved: 0, reason: failReason });
    res
      .status(aborted ? 504 : 502)
      .json(
        errorBody(
          req.path,
          'vaultcore_upstream_error',
          aborted ? 'Upstream provider timed out.' : 'Upstream provider is unreachable.',
          failReason.replace('-', '_')
        )
      );
    return;
  }

  clearTimeout(headerTimer);
  stats.forwarded += 1;
  countReason(reason);

  res.status(upstream.status);
  for (const [key, value] of upstream.headers) {
    if (!RESPONSE_HEADER_STRIP.has(key.toLowerCase())) res.setHeader(key, value);
  }
  setTelemetry(res, { local: false, latencyMs: overheadMs, tokensSaved: 0, reason });
  res.flushHeaders();

  if (!upstream.body) {
    res.off('close', onClose);
    res.end();
    return;
  }

  pipeline(upstream.body, res, (err) => {
    res.off('close', onClose);
    if (err) {
      controller.abort();
      if (err.code !== 'ERR_STREAM_PREMATURE_CLOSE') log('warn', 'upstream-stream-error', { dialect, error: err.message });
    }
  });
}

/* -------------------------------------------------------------------------- */
/* Express application                                                        */
/* -------------------------------------------------------------------------- */

let shuttingDown = false;

const app = express();
app.disable('x-powered-by');

/**
 * @param {Dialect} dialect
 * @returns {import('express').RequestHandler}
 */
function createHandler(dialect) {
  return async (req, res, next) => {
    const t0 = performance.now();
    stats.total += 1;
    stats.byDialect[dialect] += 1;
    try {
      const raw = await readBody(req, CONFIG.bodyLimitBytes);
      const decision = await decide(dialect, req, raw);
      if (decision.local) {
        sendLocal(dialect, res, decision, t0);
        return;
      }
      await forward(dialect, req, res, raw, decision.reason, t0);
    } catch (err) {
      next(err);
    }
  };
}

app.post('/v1/chat/completions', createHandler('openai'));
app.post('/v1/messages', createHandler('anthropic'));

app.get('/healthz', (_req, res) => {
  const snap = pool.snapshot();
  const status = shuttingDown ? 'shutting-down' : snap.ready === 0 ? 'degraded' : 'ok';
  res.status(shuttingDown ? 503 : 200).json({
    status,
    uptimeSec: Math.round((Date.now() - startedAt) / 1000),
    localRouting: CONFIG.localRouting,
    workers: { configured: snap.size, ready: snap.ready },
  });
});

app.get('/stats', (_req, res) => {
  const mem = process.memoryUsage();
  const mb = (/** @type {number} */ bytes) => Math.round((bytes / 1024 / 1024) * 100) / 100;
  res.json({
    uptimeSec: Math.round((Date.now() - startedAt) / 1000),
    memory: {
      rssMB: mb(mem.rss),
      heapUsedMB: mb(mem.heapUsed),
      heapTotalMB: mb(mem.heapTotal),
      externalMB: mb(mem.external),
      arrayBuffersMB: mb(mem.arrayBuffers),
    },
    requests: {
      total: stats.total,
      local: stats.local,
      forwarded: stats.forwarded,
      upstreamErrors: stats.upstreamErrors,
      localRatio: stats.total === 0 ? 0 : Math.round((stats.local / stats.total) * 10_000) / 10_000,
      byDialect: stats.byDialect,
    },
    tokensSaved: stats.tokensSaved,
    phiEntitiesDetected: stats.phiEntitiesDetected,
    routeReasons: { ...stats.reasons },
    workerPool: pool.snapshot(),
    config: {
      workerPoolSize: CONFIG.workerPoolSize,
      workerTimeoutMs: CONFIG.workerTimeoutMs,
      localRouting: CONFIG.localRouting,
    },
  });
});

app.use((req, res) => {
  res
    .status(404)
    .json(errorBody(req.path, 'not_found_error', `No route for ${req.method} ${req.path}.`, 'not_found'));
});

app.use(
  /** @type {import('express').ErrorRequestHandler} */
  (err, req, res, _next) => {
    if (res.headersSent) {
      res.destroy();
      return;
    }
    const status = Number.isInteger(err?.status) && err.status >= 400 && err.status < 600 ? err.status : 500;
    if (status === 413) res.setHeader('Connection', 'close');
    if (status >= 500) log('error', 'unhandled-request-error', { error: err instanceof Error ? err.message : String(err) });
    res
      .status(status)
      .json(
        errorBody(
          req.path,
          status === 413 ? 'request_too_large' : 'vaultcore_internal_error',
          status >= 500 ? 'Internal gateway error.' : String(err.message),
          typeof err?.code === 'string' ? err.code : 'gateway_error'
        )
      );
  }
);

/* -------------------------------------------------------------------------- */
/* Lifecycle                                                                  */
/* -------------------------------------------------------------------------- */

const server = app.listen(CONFIG.port, CONFIG.host, () => {
  log('info', 'vaultcore-listening', {
    host: CONFIG.host,
    port: CONFIG.port,
    workers: CONFIG.workerPoolSize,
    workerTimeoutMs: CONFIG.workerTimeoutMs,
    localRouting: CONFIG.localRouting,
  });
});
server.keepAliveTimeout = 65_000;
server.headersTimeout = 66_000;

/** @param {string} signal */
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log('info', 'shutdown-start', { signal });

  const force = setTimeout(() => {
    log('error', 'shutdown-forced');
    process.exit(1);
  }, CONFIG.shutdownGraceMs);
  force.unref();

  server.close(() => {
    void pool.close().then(() => {
      log('info', 'shutdown-complete');
      process.exit(0);
    });
  });
  server.closeIdleConnections?.();
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('unhandledRejection', (reason) => {
  log('error', 'unhandled-rejection', { error: reason instanceof Error ? reason.message : String(reason) });
});
process.on('uncaughtException', (err) => {
  log('error', 'uncaught-exception', { error: err.message });
  void shutdown('uncaughtException');
});
