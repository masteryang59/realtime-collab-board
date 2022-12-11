// Board storage in Redis. Redis is the only source of truth; server nodes keep
// no board state of their own, so any node can serve any board and a node can
// crash without losing anything.
//
// Every write is one MULTI/EXEC that increments the board's seq, writes the
// changed fields and appends the op to the board's log. Redis runs a
// transaction with nothing interleaved, so seq order is exactly the order in
// which fields were written. Clients version each field by seq, which makes
// last-writer-wins converge on what Redis holds (see shared/board-state.js).
// No Lua is needed, so this works on any Redis, on managed services that
// restrict scripting, and on the dev-only mini-redis.
//
// All of a board's keys share the hash tag {boardId}, so on Redis Cluster
// they live in one slot and the transactions stay valid.
import { CLIENT_FIELDS, LIMITS, fieldKey, splitFieldKey } from '../shared/protocol.js';

const NOTE_FIELDS = [...CLIENT_FIELDS, 'z', 'createdBy', 'createdAt'];

export function boardKeys(boardId) {
  const p = `rb:{${boardId}}`;
  return {
    seq: `${p}:seq`, // last assigned seq
    base: `${p}:base`, // number of ops trimmed off the front of the log
    fields: `${p}:f`, // hash '<noteId>:<field>' -> value
    notes: `${p}:notes`, // set of live note ids
    log: `${p}:log`, // list of op JSON; entry i has seq base + i + 1
    z: `${p}:z`, // stacking counter
    trim: `${p}:trim`, // lease held while trimming the log
    presence: `${p}:presence`, // hash socketId -> member JSON
    op: (opId) => `${p}:op:${opId}`, // dedupe record for a client op id
    lock: (noteId) => `${p}:lock:${noteId}`, // text-edit lease
  };
}

export class StoreError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.code = code;
    Object.assign(this, extra);
  }
}

const reject = (code, message, extra = {}) => ({ ok: false, error: { code, message, retryable: false, ...extra } });

// ioredis pipeline/multi results are [err, value] pairs.
function values(results) {
  if (!results) throw new StoreError('UNAVAILABLE', 'transaction aborted');
  return results.map(([e, v]) => {
    if (e) throw e;
    return v;
  });
}

export class BoardStore {
  constructor(redis, options = {}) {
    this.redis = redis;
    this.logKeep = options.logKeep ?? 1000; // ops kept for reconnect catch-up
    this.logSlack = options.logSlack ?? 200; // trim once this far over logKeep
    this.maxCatchUp = options.maxCatchUp ?? 500; // further behind than this gets a snapshot
    this.claimMs = options.claimMs ?? 10_000;
    this.dedupeMs = options.dedupeMs ?? 5 * 60_000;
    this.lockTtlMs = options.lockTtlMs ?? LIMITS.lockTtlMs;
    this.presenceTtlMs = options.presenceTtlMs ?? LIMITS.presenceTtlMs;
    this.notesPerBoard = options.notesPerBoard ?? LIMITS.notesPerBoard;
    this.now = options.now ?? (() => Date.now());
    this.onTrim = options.onTrim ?? (() => {});
  }

  // Applies a validated client op. `actor` is { sid, userId, name }.
  // Returns { ok: true, seq, fields, op } where `op` is what to broadcast
  // (absent for a retried op that was already applied), or { ok: false, error }.
  // Throws when Redis is unavailable.
  async applyOp(boardId, op, actor, node) {
    const k = boardKeys(boardId);
    const { type, id, opId } = op;

    // Round trip 1: claim the op id and read what the checks need.
    const pre = this.redis.pipeline();
    pre.set(k.op(opId), '0', 'PX', this.claimMs, 'NX');
    pre.hmget(k.fields, fieldKey(id, 'deleted'), fieldKey(id, 'createdAt'));
    pre.scard(k.notes);
    pre.get(k.lock(id));
    if (type === 'create' || type === 'front') pre.incr(k.z);
    const [claim, [deleted, createdAt], noteCount, lockRaw, z] = values(await pre.exec());

    if (claim !== 'OK') return this.#duplicate(k, opId);
    const fail = async (result) => {
      await this.redis.del(k.op(opId));
      return result;
    };

    if (deleted === '1') return fail(reject('GONE', 'note was deleted'));
    if (type === 'create') {
      if (createdAt !== null) return fail(reject('EXISTS', 'note id already exists'));
      if (noteCount >= this.notesPerBoard) return fail(reject('LIMIT', `a board holds at most ${this.notesPerBoard} notes`));
    } else if (createdAt === null) {
      return fail(reject('NOT_FOUND', 'note does not exist'));
    }
    if (type === 'update' && op.fields.text !== undefined && lockRaw) {
      const holder = JSON.parse(lockRaw);
      if (holder.sid !== actor.sid) return fail(reject('LOCKED', `${holder.name} is editing this note`, { by: holder }));
    }

    let fields;
    if (type === 'create') fields = { ...op.fields, z, createdBy: actor.name, createdAt: this.now() };
    else if (type === 'update') fields = op.fields;
    else if (type === 'front') fields = { z };
    else fields = { deleted: 1 };

    const at = this.now();
    const entry = { type, id, fields, by: { id: actor.userId, name: actor.name }, node, opId, at };

    // Round trip 2: the write itself.
    const tx = this.redis.multi();
    tx.incr(k.seq);
    if (type === 'delete') {
      tx.hset(k.fields, fieldKey(id, 'deleted'), 1);
      tx.hdel(k.fields, ...NOTE_FIELDS.map((f) => fieldKey(id, f)));
      tx.srem(k.notes, id);
      tx.del(k.lock(id));
    } else {
      tx.hset(k.fields, ...Object.entries(fields).flatMap(([f, v]) => [fieldKey(id, f), v]));
      if (type === 'create') tx.sadd(k.notes, id);
    }
    // Marks the op id as committed atomically with the write, so a retry after
    // a lost reply can never apply it twice.
    tx.set(k.op(opId), 'committed', 'PX', this.dedupeMs);
    tx.rpush(k.log, JSON.stringify(entry));
    const results = values(await tx.exec());
    const seq = results[0];
    const logLength = results.at(-1);

    // Round trip 3: remember the result for retries. Losing this only means a
    // retry is told "committed" without the seq, and the client catches up.
    await this.redis.set(k.op(opId), JSON.stringify({ seq, fields }), 'PX', this.dedupeMs, 'XX').catch(() => {});

    if (logLength > this.logKeep + this.logSlack) {
      this.#trim(k, logLength).catch(() => {});
    }
    return { ok: true, seq, fields, op: { seq, ...entry } };
  }

  async #duplicate(k, opId) {
    const record = await this.redis.get(k.op(opId));
    if (record === null || record === '0') {
      // Claimed by an attempt that hasn't committed (or the claim just
      // expired). The client retries shortly.
      return { ok: false, error: { code: 'IN_PROGRESS', message: 'op is still being applied', retryable: true } };
    }
    if (record === 'committed') return { ok: true, seq: null, fields: null, duplicate: true };
    const { seq, fields } = JSON.parse(record);
    return { ok: true, seq, fields, duplicate: true };
  }

  // Drops the oldest ops beyond logKeep. LTRIM and the base counter move in
  // one transaction, so `seq == base + LLEN(log)` always holds. The lease
  // keeps two nodes from trimming the same log at once.
  async #trim(k, logLength) {
    const lease = await this.redis.set(k.trim, '1', 'PX', 5000, 'NX');
    if (lease !== 'OK') return;
    try {
      const drop = logLength - this.logKeep;
      values(await this.redis.multi().ltrim(k.log, drop, -1).incrby(k.base, drop).exec());
      this.onTrim(drop);
    } finally {
      await this.redis.del(k.trim);
    }
  }

  // The whole board, read atomically with its seq: every op with seq <= the
  // returned seq is reflected, and none after it.
  async snapshot(boardId) {
    const k = boardKeys(boardId);
    const [seqRaw, raw] = values(await this.redis.multi().get(k.seq).hgetall(k.fields).exec());
    const fields = {};
    const deleted = new Set();
    for (const key of Object.keys(raw)) {
      const parts = splitFieldKey(key);
      if (parts?.field === 'deleted') deleted.add(parts.id);
    }
    // Drop fields that an update racing a delete wrote after the tombstone.
    for (const [key, value] of Object.entries(raw)) {
      const parts = splitFieldKey(key);
      if (!parts || (deleted.has(parts.id) && parts.field !== 'deleted')) continue;
      fields[key] = value;
    }
    return { seq: Number(seqRaw ?? 0), fields };
  }

  // Ops after `since`, or null when they are no longer in the log (or there
  // are so many that a snapshot is cheaper).
  async opsSince(boardId, since) {
    const k = boardKeys(boardId);
    for (let attempt = 0; attempt < 3; attempt++) {
      const [seqRaw, baseRaw] = await this.redis.mget(k.seq, k.base);
      const seq = Number(seqRaw ?? 0);
      const base = Number(baseRaw ?? 0);
      if (since >= seq) return { seq, ops: [] };
      if (since < base || seq - since > this.maxCatchUp) return null;
      const [base2, entries, seq2] = values(
        await this.redis.multi().get(k.base).lrange(k.log, since - base, -1).get(k.seq).exec(),
      );
      if (Number(base2 ?? 0) !== base) continue; // trimmed in between; recompute
      const ops = entries.map((e, i) => ({ seq: since + 1 + i, ...JSON.parse(e) }));
      return { seq: Number(seq2), ops };
    }
    return null;
  }

  // Text-edit leases. They are advisory: they keep two people from typing
  // into one note at once, but convergence never depends on them (the fields
  // are last-writer-wins either way). Releasing is GET-then-DEL, so in a rare
  // race a lease that expired a moment ago can be deleted; its holder then
  // re-acquires it on the next renewal.
  async acquireLock(boardId, noteId, holder) {
    const key = boardKeys(boardId).lock(noteId);
    for (let attempt = 0; attempt < 2; attempt++) {
      const set = await this.redis.set(key, JSON.stringify(holder), 'PX', this.lockTtlMs, 'NX');
      if (set === 'OK') return { ok: true, ttlMs: this.lockTtlMs };
      const current = await this.redis.get(key);
      if (current === null) continue; // expired between the two calls
      const by = JSON.parse(current);
      if (by.sid !== holder.sid) return { ok: false, by };
      await this.redis.pexpire(key, this.lockTtlMs);
      return { ok: true, ttlMs: this.lockTtlMs, renewed: true };
    }
    return { ok: false, by: null };
  }

  async releaseLock(boardId, noteId, sid) {
    const key = boardKeys(boardId).lock(noteId);
    const current = await this.redis.get(key);
    if (current === null || JSON.parse(current).sid !== sid) return false;
    await this.redis.del(key);
    return true;
  }

  // Current leases on the given notes: { noteId: { holder, ttlMs } }.
  async locks(boardId, noteIds) {
    if (noteIds.length === 0) return {};
    const k = boardKeys(boardId);
    const pipe = this.redis.pipeline();
    for (const id of noteIds) {
      pipe.get(k.lock(id));
      pipe.pttl(k.lock(id));
    }
    const res = values(await pipe.exec());
    const out = {};
    noteIds.forEach((id, i) => {
      const raw = res[i * 2];
      const ttl = res[i * 2 + 1];
      if (raw !== null && ttl > 0) out[id] = { by: JSON.parse(raw), ttlMs: ttl };
    });
    return out;
  }

  async liveNoteIds(boardId) {
    return this.redis.smembers(boardKeys(boardId).notes);
  }

  // Presence: one hash per board, socketId -> member with a `seen` time. Every
  // node refreshes its own sockets periodically, so members of a node that
  // crashed go stale and are swept by whichever node looks next.
  async touchPresence(entries) {
    if (entries.length === 0) return;
    const pipe = this.redis.pipeline();
    const seen = this.now();
    for (const { boardId, member } of entries) {
      pipe.hset(boardKeys(boardId).presence, member.sid, JSON.stringify({ ...member, seen }));
    }
    values(await pipe.exec());
  }

  async removePresence(boardId, sids) {
    if (sids.length === 0) return 0;
    return this.redis.hdel(boardKeys(boardId).presence, ...sids);
  }

  // { live: [member], stale: [sid] }
  async presence(boardId) {
    const raw = await this.redis.hgetall(boardKeys(boardId).presence);
    const cutoff = this.now() - this.presenceTtlMs;
    const live = [];
    const stale = [];
    for (const [sid, json] of Object.entries(raw)) {
      const member = JSON.parse(json);
      if (member.seen >= cutoff) live.push(member);
      else stale.push(sid);
    }
    return { live, stale };
  }
}


function pruneOldData(data) {
  if (!data) return null;
  return data;
}


function processData(data) {
  if (!data) return null;
  return data;
}

function pruneOldData(data) {
  if (!data) return null;
  return data;
}