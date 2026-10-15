/**
 * Vaultcore :: Purecore Engine
 * ---------------------------------------------------------------------------
 * Isolated worker-thread module. Receives { id, text } messages from the
 * Plexroute gateway, runs deterministic, allocation-light text transforms
 * entirely in transient RAM, and posts back a schema-valid verdict.
 *
 * Privacy note: this module never logs, persists, or transmits message text.
 *
 * @typedef {'whitespace-cleanup' | 'clinical-anon-extract' | 'sql-minify'} LocalIntent
 * @typedef {{ id: number, text: string }} EngineRequest
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
 * @typedef {{ routedLocally: boolean, intent: string, reason: string, output?: string, phiEntities?: PhiSummary }} Verdict
 * @typedef {{ type: string, regex: RegExp }} PhiRule
 */

import { parentPort } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

if (!parentPort) {
  throw new Error('localEngine.js must be started as a worker thread (node:worker_threads).');
}

const CHARS_PER_TOKEN = 4;

/* -------------------------------------------------------------------------- */
/* REASONING_GUARD                                                            */
/* -------------------------------------------------------------------------- */

/**
 * Anything that smells like diagnostic reasoning or creative generation is
 * never handled locally; it must go to a real model.
 */
const REASONING_GUARD =
  /\b(?:explain(?:s|ed|ing)?|analy[sz](?:e|es|ed|ing|is)|evaluat(?:e|es|ed|ing|ion)|assess(?:es|ed|ing|ment)?|diagnos(?:e|es|ed|ing|is)|differential|prognos(?:e|is|es)|treat(?:s|ed|ing|ment|ments)?|prescrib(?:e|es|ed|ing)|prescription|recommend(?:s|ed|ing|ation|ations)?|advis(?:e|es|ed|ing)|suggest(?:s|ed|ing|ion|ions)?|interpret(?:s|ed|ing|ation)?|summari[sz](?:e|es|ed|ing)|compar(?:e|es|ed|ing|ison)|reason(?:s|ed|ing)?|justif(?:y|ies|ied|ication)|predict(?:s|ed|ing|ion)?|brainstorm(?:s|ed|ing)?|compos(?:e|es|ed|ing)|draft(?:s|ed|ing)?|write|rewrite|translate|why|what\s+if|how\s+(?:should|would|do|does|can|could))\b/i;

/* -------------------------------------------------------------------------- */
/* PHI / PII detection matrix                                                 */
/* -------------------------------------------------------------------------- */

const MONTH =
  '(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)';

const DATE_CORE =
  '(?:\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{4}|\\d{2})' +
  '|\\d{4}[/.-]\\d{1,2}[/.-]\\d{1,2}' +
  `|${MONTH}\\.?\\s+\\d{1,2}(?:st|nd|rd|th)?,?\\s+\\d{4}` +
  `|\\d{1,2}(?:st|nd|rd|th)?\\s+${MONTH}\\.?,?\\s+\\d{4})`;

/**
 * Order matters: labeled/contextual identifiers run before bare numeric
 * patterns so that, e.g., "Member ID: 123456789" is not mistaken for an SSN.
 * Rules with a named `value` group keep their label and tokenize only the value.
 * @type {PhiRule[]}
 */
const PHI_RULES = [
  {
    type: 'DOB',
    regex: new RegExp(
      `\\b(?:DOB|D\\.O\\.B\\.?|Date\\s+of\\s+Birth|Birth\\s*date|Born(?:\\s+on)?)\\s*[:\\-]?\\s*(?<value>${DATE_CORE})`,
      'gi'
    ),
  },
  {
    type: 'MRN',
    regex:
      /\b(?:MRN|M\.R\.N\.|Medical\s+Record(?:\s+(?:Number|No\.?|#))?|Chart\s+(?:Number|No\.?|#))\s*[:#-]?\s*(?<value>(?=[A-Z0-9-]*\d)[A-Z0-9][A-Z0-9-]{4,13})/gi,
  },
  {
    type: 'INSURANCE_ID',
    regex:
      /\b(?:Member|Policy|Subscriber|Insurance|Medicare|Medicaid|Group|Beneficiary|Claim)\s*(?:ID|No\.?|Number|#)\s*[:#-]?\s*(?<value>(?=[A-Z0-9-]*\d)[A-Z0-9][A-Z0-9-]{4,17})/gi,
  },
  {
    type: 'SSN',
    regex: /\b(?!000|666|9\d{2})\d{3}([- ]?)(?!00)\d{2}\1(?!0000)\d{4}\b/g,
  },
  {
    type: 'PHONE',
    regex: /(?<![\w.])(?:\+?1[-.\s]?)?(?:\(\d{3}\)\s?|\d{3}[-.\s])\d{3}[-.\s]\d{4}\b/g,
  },
  {
    type: 'PHONE',
    regex: /(?<![\w.])\+\d{1,3}[-.\s]?\(?\d{2,5}\)?[-.\s]?\d{3,5}[-.\s]?\d{3,5}\b/g,
  },
  {
    type: 'EMAIL',
    regex: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,
  },
  {
    type: 'DX_CODE',
    regex:
      /\b(?:ICD-?10(?:-CM)?|ICD-?9(?:-CM)?|Dx|Diagnosis\s+Code|CPT|HCPCS|DRG)\s*(?:code)?s?\s*[:#-]?\s*(?<value>(?=[A-Z0-9.]*\d)[A-Z0-9][A-Z0-9.]{2,9})/gi,
  },
  {
    type: 'DX_CODE',
    regex: /\b[A-TV-Z]\d[0-9AB]\.[0-9A-TV-Z]{1,4}\b/g,
  },
  {
    type: 'NAME',
    regex:
      /\b(?:Dr|Mr|Mrs|Ms|Mx|Prof)\.?[ \t]+(?<value>[A-Z][a-z'’-]+(?:[ \t]+[A-Z]\.)?(?:[ \t]+[A-Z][a-z'’-]+)?)/g,
  },
  {
    type: 'NAME',
    regex:
      /\b(?:Patient(?:\s+Name)?|PATIENT(?:\s+NAME)?|Pt|PT|Name|NAME|Guarantor|GUARANTOR|Subscriber|SUBSCRIBER|Next\s+of\s+Kin|Emergency\s+Contact)\s*[:-]\s*(?<value>(?!(?:Male|Female|Unknown|Anonymous|None|Other|Stable|Critical|Deceased)\b)(?:[A-Z][a-z'’-]+,[ \t]*[A-Z][a-z'’-]+(?:[ \t]+[A-Z]\.?)?|[A-Z][a-z'’-]+(?:[ \t]+[A-Z]\.)?(?:[ \t]+[A-Z][a-z'’-]+){0,2}|[A-Z][A-Z'’-]+,[ \t]*[A-Z][A-Z'’-]+|[A-Z][A-Z'’-]+(?:[ \t]+[A-Z][A-Z'’-]+){1,2}))/g,
  },
];

/** @returns {PhiSummary} */
function emptyPhi() {
  return { total: 0, byType: {} };
}

/**
 * Deterministic tokenization: identical values (case-insensitive) within one
 * call map to the same token. The value->token vault lives only in this
 * function's scope and is garbage-collected when it returns.
 * @param {string} input
 * @returns {{ text: string, phi: PhiSummary }}
 */
function tokenizePhi(input) {
  /** @type {Map<string, string>} */
  const vault = new Map();
  /** @type {Record<string, number>} */
  const counters = {};
  const phi = emptyPhi();
  let out = input;

  for (const rule of PHI_RULES) {
    out = out.replace(rule.regex, (...args) => {
      const match = /** @type {string} */ (args[0]);
      const last = args[args.length - 1];
      const groups = typeof last === 'object' && last !== null ? last : undefined;
      const value = groups && typeof groups.value === 'string' ? groups.value : match;
      const prefix = match.slice(0, match.length - value.length);
      const key = `${rule.type}\u0000${value.toLowerCase()}`;
      let token = vault.get(key);
      if (token === undefined) {
        counters[rule.type] = (counters[rule.type] ?? 0) + 1;
        token = `[${rule.type}_${counters[rule.type]}]`;
        vault.set(key, token);
      }
      phi.byType[rule.type] = (phi.byType[rule.type] ?? 0) + 1;
      phi.total += 1;
      return prefix + token;
    });
  }

  return { text: out, phi };
}

/* -------------------------------------------------------------------------- */
/* Request parsing + intent detection                                         */
/* -------------------------------------------------------------------------- */

const FENCE = /```[^\n`]*\n?([\s\S]*?)```/g;

/**
 * Splits a message into the human instruction and the data payload it refers to.
 * Supports fenced blocks, "instruction\npayload", and "instruction: payload".
 * @param {string} text
 * @returns {{ instruction: string, payload: string }}
 */
function splitRequest(text) {
  const blocks = [...text.matchAll(FENCE)].map((m) => m[1]);
  if (blocks.length > 0) {
    return { instruction: text.replace(FENCE, ' ').trim(), payload: blocks.join('\n\n') };
  }

  const normalized = text.replace(/\r\n?/g, '\n').replace(/^\s+/, '');
  const firstBreak = normalized.indexOf('\n');
  if (firstBreak !== -1) {
    const rest = normalized.slice(firstBreak + 1);
    if (rest.trim().length > 0) {
      return { instruction: normalized.slice(0, firstBreak), payload: rest };
    }
  }

  const colon = normalized.search(/:(?!\/\/)/);
  if (colon > 0 && colon < 240 && normalized.slice(colon + 1).trim().length > 0) {
    return { instruction: normalized.slice(0, colon), payload: normalized.slice(colon + 1) };
  }

  return { instruction: normalized, payload: normalized };
}

/** @type {{ intent: LocalIntent, regex: RegExp }[]} */
const INTENT_MATCHERS = [
  {
    intent: 'sql-minify',
    regex:
      /\b(?:minif(?:y|ied|ication)|compress|condense|compact|(?:strip|remove)\s+comments(?:\s+from)?)\b[^\n]{0,60}\b(?:sql|quer(?:y|ies))\b|\bsql[-\s]?minif(?:y|ier)\b/i,
  },
  {
    intent: 'whitespace-cleanup',
    regex:
      /\b(?:clean(?:\s*-?\s*up)?|tidy|normali[sz]e|squash|collapse|strip|trim|remove)\b[^\n]{0,60}\b(?:whitespace|white\s+space|spaces|blank\s+lines|newlines|line\s+breaks|invisible|zero[-\s]?width)\b|\bwhitespace[-\s]cleanup\b/i,
  },
  {
    intent: 'clinical-anon-extract',
    regex:
      /\b(?:extract|pull(?:\s+out)?|collect|list)\b[^\n]{0,60}\b(?:tables?|outlines?|urls?|links?)\b|\bclinical[-\s]anon[-\s]extract\b/i,
  },
];

/**
 * @param {string} instruction
 * @returns {LocalIntent | null}
 */
function detectIntent(instruction) {
  for (const matcher of INTENT_MATCHERS) {
    if (matcher.regex.test(instruction)) return matcher.intent;
  }
  return null;
}

/* -------------------------------------------------------------------------- */
/* Module 1: whitespace-cleanup                                               */
/* -------------------------------------------------------------------------- */

const INVISIBLE_CHARS = /[\u00AD\u061C\u180E\u200B-\u200F\u202A-\u202E\u2060-\u2069\uFEFF]/g;

/**
 * Squashes extra newlines, strips trailing horizontal whitespace, and removes
 * invisible unicode characters.
 * @param {string} input
 * @returns {string}
 */
function whitespaceCleanup(input) {
  return input
    .replace(INVISIBLE_CHARS, '')
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t\f\v\u00A0]+$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/* -------------------------------------------------------------------------- */
/* Module 2: clinical-anon-extract                                            */
/* -------------------------------------------------------------------------- */

const OUTLINE_LINE =
  /^\s*(?:(?:[IVXLC]+|[A-Za-z]|\d{1,3}(?:\.\d{1,3})*)[.)]\s+\S|[-*•–]\s+\S|(?:Assessment|Plan|Diagnos(?:is|es)|Impression|History(?:\s+of\s+Present\s+Illness)?|HPI|Medications?|Allergies|Problem\s+List|Review\s+of\s+Systems|ROS|Past\s+Medical\s+History|PMH|Chief\s+Complaint|CC)\s*(?:\/\s*\w+)?\s*:)/i;

const URL_PATTERN = /\b(?:https?:\/\/|www\.)[^\s<>"'`]+/gi;

/** @param {string} line */
function isTableLine(line) {
  if ((line.match(/\|/g) ?? []).length >= 2) return true;
  return line.includes('\t') && line.trim().split(/\t+/).length >= 2;
}

/**
 * Extracts tables, plain-text diagnostic outlines, and bare URLs from chart
 * text, then tokenizes any PHI/PII in the assembled result.
 * @param {string} input
 * @returns {{ output: string, phi: PhiSummary } | null} null when nothing was found
 */
function clinicalAnonExtract(input) {
  const lines = input.replace(/\r\n?/g, '\n').split('\n');

  /** @type {string[][]} */
  const tables = [];
  const inTable = new Array(lines.length).fill(false);
  let run = [];
  let runStart = 0;
  const flush = (/** @type {number} */ endExclusive) => {
    if (run.length >= 2) {
      tables.push(run);
      for (let k = runStart; k < endExclusive; k += 1) inTable[k] = true;
    }
    run = [];
  };
  for (let i = 0; i < lines.length; i += 1) {
    if (isTableLine(lines[i])) {
      if (run.length === 0) runStart = i;
      run.push(lines[i].trimEnd());
    } else {
      flush(i);
    }
  }
  flush(lines.length);

  const outline = [];
  for (let i = 0; i < lines.length; i += 1) {
    if (!inTable[i] && OUTLINE_LINE.test(lines[i])) outline.push(lines[i].trimEnd());
  }

  const urls = new Set();
  for (const match of input.matchAll(URL_PATTERN)) {
    const cleaned = match[0].replace(/[.,;:!?)\]}]+$/, '');
    if (cleaned.length > 0) urls.add(cleaned);
  }

  if (tables.length === 0 && outline.length === 0 && urls.size === 0) return null;

  const sections = [];
  if (tables.length > 0) {
    sections.push(
      `## Tables (${tables.length})\n\n` + tables.map((t, i) => `Table ${i + 1}\n${t.join('\n')}`).join('\n\n')
    );
  }
  if (outline.length > 0) {
    sections.push(`## Diagnostic outline (${outline.length} lines)\n\n${outline.join('\n')}`);
  }
  if (urls.size > 0) {
    sections.push(`## URLs (${urls.size})\n\n${[...urls].map((u) => `- ${u}`).join('\n')}`);
  }

  const { text, phi } = tokenizePhi(sections.join('\n\n'));
  return { output: text, phi };
}

/* -------------------------------------------------------------------------- */
/* Module 3: sql-minify                                                       */
/* -------------------------------------------------------------------------- */

/**
 * Deterministic single-pass SQL minifier. Strips "--" and block comments
 * (preserving optimizer hints and MySQL executable comments), collapses
 * whitespace, and leaves quoted strings/identifiers and dollar-quoted bodies
 * byte-for-byte intact. Throws on unterminated quotes/comments so the caller
 * can fall back to the cloud rather than emit a corrupted query.
 * @param {string} sql
 * @returns {string}
 */
function minifySql(sql) {
  const n = sql.length;
  let out = '';
  let pendingSpace = false;
  let i = 0;

  /** @param {string} chunk */
  const emit = (chunk) => {
    if (pendingSpace && out.length > 0) {
      const prev = out[out.length - 1];
      const next = chunk[0];
      if (!'(,'.includes(prev) && !'),;'.includes(next)) out += ' ';
    }
    pendingSpace = false;
    out += chunk;
  };

  while (i < n) {
    const c = sql[i];
    const d = sql[i + 1];

    if (/\s/.test(c)) {
      pendingSpace = true;
      i += 1;
      continue;
    }

    if (c === '-' && d === '-') {
      const nl = sql.indexOf('\n', i + 2);
      i = nl === -1 ? n : nl;
      pendingSpace = true;
      continue;
    }

    if (c === '/' && d === '*') {
      const end = sql.indexOf('*/', i + 2);
      if (end === -1) throw new Error('unterminated-block-comment');
      const keep = sql[i + 2] === '+' || sql[i + 2] === '!';
      if (keep) emit(sql.slice(i, end + 2));
      else pendingSpace = true;
      i = end + 2;
      continue;
    }

    if (c === "'" || c === '"' || c === '`') {
      let j = i + 1;
      for (;;) {
        if (j >= n) throw new Error('unterminated-quote');
        if (sql[j] === c) {
          if (sql[j + 1] === c) {
            j += 2;
            continue;
          }
          break;
        }
        if (sql[j] === '\\' && c !== '`') {
          j += 2;
          continue;
        }
        j += 1;
      }
      emit(sql.slice(i, j + 1));
      i = j + 1;
      continue;
    }

    if (c === '$') {
      const m = /^(?:\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$)/.exec(sql.slice(i, i + 64));
      if (m) {
        const tag = m[0];
        const end = sql.indexOf(tag, i + tag.length);
        if (end === -1) throw new Error('unterminated-dollar-quote');
        emit(sql.slice(i, end + tag.length));
        i = end + tag.length;
        continue;
      }
    }

    emit(c);
    i += 1;
  }

  return out.trim();
}

/* -------------------------------------------------------------------------- */
/* Verdict assembly                                                           */
/* -------------------------------------------------------------------------- */

/**
 * @param {string} intent
 * @param {string} reason
 * @returns {Verdict}
 */
function reject(intent, reason) {
  return { routedLocally: false, intent, reason };
}

/**
 * @param {number} id
 * @param {number} start performance.now() timestamp
 * @param {string} text original message text
 * @param {Verdict} verdict
 * @returns {EngineResponse}
 */
function finish(id, start, text, verdict) {
  const routedLocally = verdict.routedLocally === true;
  const output = routedLocally ? (verdict.output ?? '') : '';
  const estimatedTokensSaved = routedLocally
    ? Math.ceil(text.length / CHARS_PER_TOKEN) + Math.ceil(output.length / CHARS_PER_TOKEN)
    : 0;
  return {
    id,
    routedLocally,
    intent: verdict.intent,
    reason: verdict.reason,
    output,
    latencyMs: Math.round((performance.now() - start) * 1000) / 1000,
    estimatedTokensSaved,
    phiEntities: verdict.phiEntities ?? emptyPhi(),
  };
}

/**
 * @param {unknown} message
 * @returns {EngineResponse}
 */
function processRequest(message) {
  const start = performance.now();
  const req = /** @type {Partial<EngineRequest> | null} */ (message);
  const id = req && typeof req.id === 'number' ? req.id : -1;

  try {
    if (!req || typeof req.id !== 'number' || typeof req.text !== 'string') {
      return finish(id, start, '', reject('none', 'invalid-message'));
    }

    const text = req.text;
    const { instruction, payload } = splitRequest(text);

    if (REASONING_GUARD.test(instruction)) {
      return finish(id, start, text, reject('reasoning-required', 'reasoning-required'));
    }

    const intent = detectIntent(instruction);
    if (intent === null) {
      return finish(id, start, text, reject('none', 'no-local-intent'));
    }

    const hasSeparatePayload = payload !== instruction;

    switch (intent) {
      case 'whitespace-cleanup': {
        if (!hasSeparatePayload) return finish(id, start, text, reject(intent, 'no-payload'));
        const output = whitespaceCleanup(payload);
        const { phi } = tokenizePhi(payload);
        return finish(id, start, text, {
          routedLocally: true,
          intent,
          reason: `local:${intent}`,
          output,
          phiEntities: phi,
        });
      }
      case 'sql-minify': {
        if (!hasSeparatePayload) return finish(id, start, text, reject(intent, 'no-payload'));
        let output;
        try {
          output = minifySql(payload);
        } catch (err) {
          const code = err instanceof Error ? err.message : 'sql-parse-error';
          return finish(id, start, text, reject(intent, code));
        }
        const { phi } = tokenizePhi(payload);
        return finish(id, start, text, {
          routedLocally: true,
          intent,
          reason: `local:${intent}`,
          output,
          phiEntities: phi,
        });
      }
      case 'clinical-anon-extract': {
        const result = clinicalAnonExtract(payload);
        if (result === null) return finish(id, start, text, reject(intent, 'nothing-extracted'));
        return finish(id, start, text, {
          routedLocally: true,
          intent,
          reason: `local:${intent}`,
          output: result.output,
          phiEntities: result.phi,
        });
      }
      default:
        return finish(id, start, text, reject('none', 'no-local-intent'));
    }
  } catch {
    return finish(id, start, '', reject('none', 'engine-error'));
  }
}

parentPort.on('message', (message) => {
  parentPort?.postMessage(processRequest(message));
});
