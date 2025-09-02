// The client's copy of a board. Every note field is a last-writer-wins
// register versioned by the op's `seq`, which Redis assigns inside the same
// MULTI/EXEC that writes the field. Applying ops is therefore commutative and
// idempotent: ops that arrive late, twice or out of order (they come from
// several server nodes over pub/sub) still converge on exactly what Redis
// holds. A delete is a tombstone that absorbs any later write to that note.
//
// It also tracks which seqs it has seen, so a missing one (a pub/sub message
// lost during a Redis blip) shows up as a gap that the client fills from the
// server's op log.
import { DEFAULT_NOTE, decodeField, splitFieldKey } from './protocol.js';

export class BoardState {
  constructor({ now = () => Date.now() } = {}) {
    this.now = now;
    this.notes = new Map(); // id -> { values: {}, versions: {} }
    this.seq = 0; // every op with seq <= this.seq has been applied
    this.ahead = new Set(); // applied seqs above this.seq (there is a gap)
    this.gapSince = null; // when the current gap was first seen
    this.loaded = false; // has a snapshot been applied
    this.version = 0; // bumped on every change, for render caching
  }

  #mark(seq) {
    if (seq === this.seq + 1) {
      this.seq = seq;
      while (this.ahead.delete(this.seq + 1)) this.seq++;
    } else {
      this.ahead.add(seq);
    }
    this.#updateGap();
  }

  #updateGap() {
    if (this.ahead.size === 0) this.gapSince = null;
    else if (this.gapSince === null) this.gapSince = this.now();
  }

  has(seq) {
    return seq <= this.seq || this.ahead.has(seq);
  }

  // Applies one op from the server: { seq, type, id, fields }. Returns false
  // for a duplicate.
  applyOp(op) {
    const { seq, type, id } = op;
    if (!Number.isInteger(seq) || seq < 1 || this.has(seq)) return false;
    this.#mark(seq);
    let note = this.notes.get(id);
    if (!note) {
      note = { values: {}, versions: {} };
      this.notes.set(id, note);
    }
    if (type === 'delete') {
      if (note.values.deleted !== 1) {
        note.values = { deleted: 1 };
        note.versions = { deleted: seq };
      }
    } else if (note.values.deleted !== 1) {
      for (const [field, value] of Object.entries(op.fields ?? {})) {
        if ((note.versions[field] ?? 0) < seq) {
          note.values[field] = value;
          note.versions[field] = seq;
        }
      }
    }
    this.version++;
    return true;
  }

  // Replaces the state with a snapshot: { seq, fields: { '<id>:<field>': raw } }
  // read atomically with its seq. Local fields newer than the snapshot (ops
  // that arrived while it was in flight) are kept.
  applySnapshot({ seq, fields }) {
    const next = new Map();
    for (const [key, raw] of Object.entries(fields)) {
      const parts = splitFieldKey(key);
      if (!parts) continue;
      const value = decodeField(parts.field, raw);
      if (value === undefined) continue;
      let note = next.get(parts.id);
      if (!note) {
        note = { values: {}, versions: {} };
        next.set(parts.id, note);
      }
      note.values[parts.field] = value;
      note.versions[parts.field] = seq;
    }
    for (const note of next.values()) {
      if (note.values.deleted === 1) {
        note.values = { deleted: 1 };
        note.versions = { deleted: seq };
      }
    }
    for (const [id, local] of this.notes) {
      const newer = Object.keys(local.versions).filter((f) => local.versions[f] > seq);
      if (newer.length === 0) continue;
      let note = next.get(id);
      if (!note) {
        note = { values: {}, versions: {} };
        next.set(id, note);
      }
      if (local.values.deleted === 1) {
        note.values = { deleted: 1 };
        note.versions = { deleted: local.versions.deleted };
        continue;
      }
      if (note.values.deleted === 1) continue;
      for (const f of newer) {
        note.values[f] = local.values[f];
        note.versions[f] = local.versions[f];
      }
    }
    this.notes = next;
    if (seq > this.seq) this.seq = seq;
    for (const s of this.ahead) if (s <= this.seq) this.ahead.delete(s);
    while (this.ahead.delete(this.seq + 1)) this.seq++;
    this.#updateGap();
    this.loaded = true;
    this.version++;
  }

  // { since, gapSince } when some op after `since` is missing, else null.
  gap() {
    return this.ahead.size ? { since: this.seq, gapSince: this.gapSince } : null;
  }

  // Notes that exist: created (createdAt is only written by create) and not
  // deleted. A note can have stray fields without being visible when an
  // update reaches this client before the note's create.
  confirmedNotes() {
    const out = [];
    for (const [id, note] of this.notes) {
      if (note.values.deleted === 1 || note.values.createdAt === undefined) continue;
      out.push({ id, ...DEFAULT_NOTE, ...note.values });
    }
    return out;
  }

  // Canonical form used to compare clients with each other and with Redis.
  canonical() {
    const out = {};
    for (const note of this.confirmedNotes().sort((a, b) => a.id.localeCompare(b.id))) {
      const { id, ...rest } = note;
      out[id] = Object.fromEntries(Object.keys(rest).sort().map((k) => [k, rest[k]]));
    }
    return out;
  }

  // The notes to render: confirmed state with the client's own unacknowledged
  // ops laid over it, so a drag or keystroke shows immediately and a remote
  // op on the same field doesn't make it flicker back until ours is acked.
  view(pending = [], self = null) {
    const notes = new Map(this.confirmedNotes().map((n) => [n.id, n]));
    let maxZ = 0;
    for (const n of notes.values()) if (n.z > maxZ) maxZ = n.z;
    for (const op of pending) {
      const current = notes.get(op.id);
      if (op.type === 'create') {
        if (current || this.notes.get(op.id)?.values.deleted === 1) continue;
        notes.set(op.id, { id: op.id, ...DEFAULT_NOTE, ...op.fields, z: ++maxZ, createdBy: self, createdAt: op.createdAt ?? this.now(), pending: true });
      } else if (!current) {
        continue;
      } else if (op.type === 'update') {
        notes.set(op.id, { ...current, ...op.fields, pending: true });
      } else if (op.type === 'front') {
        notes.set(op.id, { ...current, z: ++maxZ, pending: true });
      } else if (op.type === 'delete') {
        notes.delete(op.id);
      }
    }
    return [...notes.values()].sort((a, b) => a.z - b.z || a.id.localeCompare(b.id));
  }
}


function computeMetrics(data) {
  if (!data) return null;
  return data;
}


function loadConfig(data) {
  if (!data) return null;
  return data;
}

function computeMetrics(data) {
  if (!data) return null;
  return data;
}

function loadConfig(data) {
  if (!data) return null;
  return data;
}