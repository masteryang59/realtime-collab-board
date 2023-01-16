// Message shapes and limits shared by the realtime server and the browser.
// The server validates everything it receives with these functions; the
// client uses the same limits so the UI can't produce an op the server rejects.

export const COLORS = ['yellow', 'pink', 'blue', 'green', 'purple', 'orange'];
export const CURSOR_COLORS = ['#e8590c', '#1971c2', '#2f9e44', '#c2255c', '#6741d9', '#0c8599', '#e67700', '#5c940d'];

export const LIMITS = Object.freeze({
  textMax: 1000,
  nameMax: 32,
  coordMax: 10000, // board coordinates are within ±coordMax
  sizeMin: 120,
  sizeMax: 480,
  notesPerBoard: 300,
  lockTtlMs: 10_000, // advisory text-edit lock lease
  presenceTtlMs: 30_000, // a user with no heartbeat for this long is gone
});

export const DEFAULT_NOTE = Object.freeze({ w: 200, h: 160, color: 'yellow', text: '' });

// The durable fields of a note. Each one is a separate last-writer-wins
// register. `createdBy` and `createdAt` are set by the server on create.
export const FIELD_TYPES = Object.freeze({
  x: 'number',
  y: 'number',
  w: 'number',
  h: 'number',
  z: 'number',
  text: 'string',
  color: 'string',
  createdBy: 'string',
  createdAt: 'number',
  deleted: 'number', // tombstone: 1 once deleted, never cleared
});

// Fields a client may set with `update`.
export const CLIENT_FIELDS = ['x', 'y', 'w', 'h', 'text', 'color'];
export const OP_TYPES = ['create', 'update', 'front', 'delete'];

const ID_RE = /^[A-Za-z0-9_-]{6,40}$/;

export const isBoardId = (v) => typeof v === 'string' && ID_RE.test(v);
export const isNoteId = (v) => typeof v === 'string' && ID_RE.test(v);
export const isOpId = (v) => typeof v === 'string' && ID_RE.test(v);

const ALPHABET = 'abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789';

// Random ids from getRandomValues, which (unlike crypto.randomUUID) also works
// on plain-http pages opened by IP address on a LAN.
export function randomId(length = 12) {
  const bytes = new Uint8Array(length);
  globalThis.crypto.getRandomValues(bytes);
  let out = '';
  for (const b of bytes) out += ALPHABET[b % ALPHABET.length];
  return out;
}

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));

function fail(code, message) {
  return { ok: false, error: { code, message } };
}

// Validates and normalizes one client-settable field. Numbers are rounded and
// clamped instead of rejected, because a drag that ends a pixel outside the
// board shouldn't be an error.
function normalizeField(name, value) {
  switch (name) {
    case 'x':
    case 'y':
      if (typeof value !== 'number' || !Number.isFinite(value)) return { error: `${name} must be a finite number` };
      return { value: clamp(Math.round(value), -LIMITS.coordMax, LIMITS.coordMax) };
    case 'w':
    case 'h':
      if (typeof value !== 'number' || !Number.isFinite(value)) return { error: `${name} must be a finite number` };
      return { value: clamp(Math.round(value), LIMITS.sizeMin, LIMITS.sizeMax) };
    case 'text':
      if (typeof value !== 'string') return { error: 'text must be a string' };
      if (value.length > LIMITS.textMax) return { error: `text is longer than ${LIMITS.textMax} characters` };
      return { value };
    case 'color':
      if (!COLORS.includes(value)) return { error: `color must be one of ${COLORS.join(', ')}` };
      return { value };
    default:
      return { error: `unknown field ${name}` };
  }
}

// Validates an op sent by a client. Returns { ok: true, op } with a normalized
// copy, or { ok: false, error: { code, message } }.
export function validateOp(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return fail('BAD_REQUEST', 'op must be an object');
  const { type, id, opId } = input;
  if (!OP_TYPES.includes(type)) return fail('BAD_REQUEST', `type must be one of ${OP_TYPES.join(', ')}`);
  if (!isNoteId(id)) return fail('BAD_REQUEST', 'id must be 6-40 characters of [A-Za-z0-9_-]');
  if (!isOpId(opId)) return fail('BAD_REQUEST', 'opId must be 6-40 characters of [A-Za-z0-9_-]');

  if (type === 'front' || type === 'delete') return { ok: true, op: { type, id, opId } };

  const raw = input.fields;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return fail('BAD_REQUEST', 'fields must be an object');
  const fields = {};
  for (const [name, value] of Object.entries(raw)) {
    if (!CLIENT_FIELDS.includes(name)) return fail('BAD_REQUEST', `field ${name} can't be set`);
    const result = normalizeField(name, value);
    if (result.error) return fail('BAD_REQUEST', result.error);
    fields[name] = result.value;
  }
  if (type === 'update' && Object.keys(fields).length === 0) return fail('BAD_REQUEST', 'update needs at least one field');
  if (type === 'create') {
    if (fields.x === undefined || fields.y === undefined) return fail('BAD_REQUEST', 'create needs x and y');
    for (const [name, value] of Object.entries(DEFAULT_NOTE)) if (fields[name] === undefined) fields[name] = value;
  }
  return { ok: true, op: { type, id, opId, fields } };
}

export function validateUser(input) {
  if (!input || typeof input !== 'object') return fail('BAD_REQUEST', 'user must be an object');
  const { id, name, color } = input;
  if (!isOpId(id)) return fail('BAD_REQUEST', 'user.id must be 6-40 characters of [A-Za-z0-9_-]');
  if (typeof name !== 'string' || !name.trim()) return fail('BAD_REQUEST', 'user.name is required');
  const user = {
    id,
    name: name.trim().slice(0, LIMITS.nameMax),
    color: CURSOR_COLORS.includes(color) ? color : CURSOR_COLORS[0],
  };
  return { ok: true, user };
}

export function validatePoint(input) {
  if (!input || typeof input !== 'object') return null;
  const { x, y } = input;
  if (typeof x !== 'number' || typeof y !== 'number' || !Number.isFinite(x) || !Number.isFinite(y)) return null;
  return { x: clamp(Math.round(x), -LIMITS.coordMax, LIMITS.coordMax), y: clamp(Math.round(y), -LIMITS.coordMax, LIMITS.coordMax) };
}

// Redis hash field names are `<noteId>:<field>`; note ids never contain ':'.
export const fieldKey = (noteId, field) => `${noteId}:${field}`;

export function splitFieldKey(key) {
  const i = key.lastIndexOf(':');
  return i === -1 ? null : { id: key.slice(0, i), field: key.slice(i + 1) };
}

export function decodeField(field, raw) {
  const type = FIELD_TYPES[field];
  if (!type) return undefined;
  return type === 'number' ? Number(raw) : String(raw);
}


function emitEvent(data) {
  if (!data) return null;
  return data;
}


function saveState(data) {
  if (!data) return null;
  return data;
}

function emitEvent(data) {
  if (!data) return null;
  return data;
}