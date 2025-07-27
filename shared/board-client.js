// The board client used by the React UI and by the integration tests. It is
// plain JavaScript with no DOM dependency.
//
// - Local changes are applied at once as overlays and queued in an outbox.
//   The outbox is sent one op at a time, so ops reach Redis in the order the
//   user made them. Unsent updates to the same note are merged.
// - Every op carries an opId. A retry after a timeout or a reconnect reuses
//   it, and the server deduplicates, so an op is applied at most once.
// - On (re)connect the client joins with the last seq it has and gets either
//   the ops it missed from the server's log or a fresh snapshot.
// - A missing seq that doesn't show up within gapTimeoutMs triggers a catch-up.
// - If a node keeps refusing connections, the client moves to the next one.
import { io } from 'socket.io-client';
import { BoardState } from './board-state.js';
import { randomId, validateOp } from './protocol.js';

const ACTIVITY_MAX = 40;

const defaultConnect = (url) =>
  io(url, {
    transports: ['websocket'],
    reconnectionDelay: 300,
    reconnectionDelayMax: 3000,
    timeout: 5000,
  });

export class BoardClient {
  constructor({
    boardId,
    user,
    urls,
    connect = defaultConnect,
    nodeIndex = 0,
    ackTimeoutMs = 4000,
    gapTimeoutMs = 1500,
    failoverAfter = 3,
    throttleMs = 40,
    now = () => Date.now(),
  }) {
    if (!urls?.length) throw new Error('BoardClient needs at least one realtime node URL');
    this.boardId = boardId;
    this.user = user;
    this.urls = urls;
    this.connectFn = connect;
    this.nodeIndex = nodeIndex % urls.length;
    this.ackTimeoutMs = ackTimeoutMs;
    this.gapTimeoutMs = gapTimeoutMs;
    this.failoverAfter = failoverAfter;
    this.throttleMs = throttleMs;
    this.now = now;

    this.state = new BoardState({ now });
    this.outbox = []; // { op, attempts, inflight, createdAt }
    this.status = 'connecting'; // connecting | online | reconnecting | stopped
    this.node = null;
    this.you = null;
    this.joined = false;
    this.presence = new Map(); // sid -> member
    this.cursors = new Map(); // sid -> { x, y, at }
    this.drags = new Map(); // sid -> { id, x, y, at }
    this.locks = new Map(); // noteId -> { by, expiresAt }
    this.myLocks = new Map(); // noteId -> renewal timer
    this.activity = [];
    this.notice = null;
    this.stats = { acks: 0, retries: 0, syncs: 0, snapshots: 0, failovers: 0, joins: 0 };

    this.listeners = new Set();
    this.changes = 0;
    this.cached = null;
    this.generation = 0; // bumped per socket; stale callbacks compare against it
    this.sending = false;
    this.syncing = false;
    this.retryTimer = null;
    this.joinTimer = null;
    this.throttle = new Map(); // 'cursor' | 'drag' -> { timer, pending }
  }

  // Lifecycle

  start() {
    this.tick = setInterval(() => this.#tick(), 250);
    this.#open(this.nodeIndex);
    return this;
  }

  stop() {
    this.status = 'stopped';
    clearInterval(this.tick);
    clearTimeout(this.retryTimer);
    clearTimeout(this.joinTimer);
    for (const timer of this.myLocks.values()) clearInterval(timer);
    this.myLocks.clear();
    for (const t of this.throttle.values()) clearTimeout(t.timer);
    this.#closeSocket();
    this.#changed();
  }

  switchNode(index) {
    if (index === this.nodeIndex && this.socket?.connected) return;
    this.#open(index % this.urls.length);
  }

  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  // A cached, immutable view for rendering (stable between changes, as
  // React's useSyncExternalStore requires).
  getView() {
    if (this.cached?.changes === this.changes) return this.cached.view;
    const presence = [...this.presence.values()];
    const cursors = [];
    for (const [sid, c] of this.cursors) {
      const m = this.presence.get(sid);
      if (m) cursors.push({ sid, x: c.x, y: c.y, name: m.name, color: m.color });
    }
    const drags = {};
    for (const [sid, d] of this.drags) {
      const m = this.presence.get(sid);
      drags[d.id] = { x: d.x, y: d.y, name: m?.name ?? '', color: m?.color ?? '#888' };
    }
    const locks = {};
    for (const [id, l] of this.locks) locks[id] = l.by;
    const view = {
      status: this.status,
      node: this.node,
      nodeIndex: this.nodeIndex,
      urls: this.urls,
      you: this.you,
      seq: this.state.seq,
      loaded: this.state.loaded,
      notes: this.state.view(this.outbox.map((e) => e.op), this.user.name),
      presence,
      cursors,
      drags,
      locks,
      myLocks: [...this.myLocks.keys()],
      unsaved: this.outbox.length,
      activity: this.activity,
      notice: this.notice,
    };
    this.cached = { changes: this.changes, view };
    return view;
  }

  // Board actions

  createNote(fields) {
    const id = randomId();
    this.#enqueue({ type: 'create', id, opId: randomId(), fields });
    return id;
  }

  updateNote(id, fields) {
    this.#enqueue({ type: 'update', id, opId: randomId(), fields });
  }

  bringToFront(id) {
    const notes = this.getView().notes;
    if (notes.length && notes.at(-1).id === id) return; // already on top
    this.#enqueue({ type: 'front', id, opId: randomId() });
  }

  deleteNote(id) {
    if (this.myLocks.has(id)) this.unlock(id);
    this.#enqueue({ type: 'delete', id, opId: randomId() });
  }

  sendCursor(x, y) {
    this.#throttled('cursor', { x, y });
  }

  sendDrag(id, x, y) {
    this.#throttled('drag', { id, x, y });
  }

  endDrag(id) {
    const t = this.throttle.get('drag');
    if (t) {
      clearTimeout(t.timer);
      this.throttle.delete('drag');
    }
    if (this.joined) this.socket.emit('drag', { id, end: true });
  }

  // Takes the text-edit lease on a note. Resolves to { ok } or { ok: false, by }.
  async lock(id) {
    const result = await this.#lockRequest(id);
    if (result.ok && !this.myLocks.has(id)) {
      // Renew well before the lease runs out.
      const ttl = result.ttlMs ?? 10_000;
      this.myLocks.set(id, setInterval(() => this.#renew(id), Math.max(500, ttl / 3)));
      this.#changed();
    }
    return result;
  }

  unlock(id) {
    const timer = this.myLocks.get(id);
    if (timer === undefined) return;
    clearInterval(timer);
    this.myLocks.delete(id);
    if (this.joined) this.socket.emit('lock', { id, action: 'release' }, () => {});
    this.#changed();
  }

  async #lockRequest(id) {
    if (!this.joined) return { ok: false, error: { code: 'OFFLINE', message: 'not connected' } };
    try {
      const res = await this.socket.timeout(this.ackTimeoutMs).emitWithAck('lock', { id, action: 'acquire' });
      if (!res.ok) return { ok: false, by: res.error?.by ?? null, error: res.error };
      return { ok: true, ttlMs: res.ttlMs };
    } catch {
      return { ok: false, error: { code: 'TIMEOUT', message: 'no reply from server' } };
    }
  }

  async #renew(id) {
    const result = await this.#lockRequest(id);
    if (result.ok || !this.myLocks.has(id)) return;
    if (result.error?.code === 'LOCKED') {
      // Someone took the note after our lease lapsed (we were offline).
      clearInterval(this.myLocks.get(id));
      this.myLocks.delete(id);
      this.#setNotice(`${result.by?.name ?? 'Someone'} is editing that note now`);
    }
  }

  // Connection

  #closeSocket() {
    if (!this.socket) return;
    this.socket.removeAllListeners();
    this.socket.io.removeAllListeners();
    this.socket.disconnect();
    this.socket = null;
  }

  #open(index) {
    this.#closeSocket();
    const generation = ++this.generation;
    this.nodeIndex = index;
    this.joined = false;
    this.sending = false;
    for (const e of this.outbox) e.inflight = false;
    this.status = this.state.loaded ? 'reconnecting' : 'connecting';
    let failures = 0;
    const socket = this.connectFn(this.urls[index]);
    this.socket = socket;
    const current = () => generation === this.generation;

    socket.on('connect', () => {
      failures = 0;
      if (current()) this.#join(generation);
    });
    const failed = () => {
      if (!current()) return;
      failures++;
      if (failures >= this.failoverAfter && this.urls.length > 1) {
        this.stats.failovers++;
        this.#open((this.nodeIndex + 1) % this.urls.length);
      }
    };
    socket.on('connect_error', failed);
    socket.io.on('reconnect_error', failed);
    socket.on('disconnect', (reason) => {
      if (!current()) return;
      this.joined = false;
      this.sending = false;
      this.syncing = false;
      for (const e of this.outbox) e.inflight = false;
      this.presence.clear();
      this.cursors.clear();
      this.drags.clear();
      this.status = 'reconnecting';
      // The client doesn't reconnect on its own after a server-side disconnect.
      if (reason === 'io server disconnect') setTimeout(() => current() && socket.connect(), 500);
      this.#changed();
    });

    socket.on('op', (op) => current() && this.#remoteOps([op]));
    socket.on('presence', (msg) => current() && this.#presence(msg));
    socket.on('lock', (msg) => current() && this.#remoteLock(msg));
    socket.on('resync', () => current() && this.#sync());
    socket.on('cursor', ({ sid, x, y }) => {
      if (!current() || sid === this.you) return;
      this.cursors.set(sid, { x, y, at: this.now() });
      this.#changed();
    });
    socket.on('drag', ({ sid, id, x, y, end }) => {
      if (!current() || sid === this.you) return;
      if (end) this.drags.delete(sid);
      else this.drags.set(sid, { id, x, y, at: this.now() });
      this.#changed();
    });
    this.#changed();
  }

  async #join(generation) {
    clearTimeout(this.joinTimer);
    const since = this.state.loaded ? this.state.seq : null;
    let res;
    try {
      res = await this.socket.timeout(this.ackTimeoutMs).emitWithAck('join', { boardId: this.boardId, user: this.user, since });
    } catch {
      res = { ok: false, error: { retryable: true, message: 'no reply from server' } };
    }
    if (generation !== this.generation) return;
    if (!res.ok) {
      this.#setNotice(res.error?.message ?? 'could not join the board');
      if (res.error?.retryable !== false) {
        this.joinTimer = setTimeout(() => generation === this.generation && this.socket?.connected && this.#join(generation), 1000);
      }
      return;
    }
    this.stats.joins++;
    this.node = res.node;
    this.you = res.you;
    if (res.snapshot) {
      this.stats.snapshots++;
      this.state.applySnapshot(res.snapshot);
    } else {
      if (!this.state.loaded) this.state.applySnapshot({ seq: 0, fields: {} });
      this.#remoteOps(res.ops, { fromLog: true });
    }
    this.presence = new Map(res.presence.filter((m) => m.sid !== this.you).map((m) => [m.sid, m]));
    this.locks.clear();
    for (const [id, { by, ttlMs }] of Object.entries(res.locks ?? {})) {
      if (by.sid !== this.you) this.locks.set(id, { by, expiresAt: this.now() + ttlMs });
    }
    this.joined = true;
    this.status = 'online';
    if (this.notice?.connection) this.notice = null;
    // Leases held before a reconnect belonged to the old socket id.
    for (const id of this.myLocks.keys()) this.#renew(id);
    this.#changed();
    this.#flush();
  }

  async #sync() {
    if (this.syncing || !this.joined) return;
    this.syncing = true;
    const generation = this.generation;
    this.stats.syncs++;
    try {
      const res = await this.socket.timeout(this.ackTimeoutMs).emitWithAck('sync', { since: this.state.seq });
      if (generation !== this.generation || !res.ok) return;
      if (res.snapshot) {
        this.stats.snapshots++;
        this.state.applySnapshot(res.snapshot);
      } else {
        this.#remoteOps(res.ops, { fromLog: true });
      }
      this.#changed();
    } catch {
      // Timed out; the next gap check or reconnect tries again.
    } finally {
      if (generation === this.generation) this.syncing = false;
    }
  }

  #remoteOps(ops, { fromLog = false } = {}) {
    let changed = false;
    for (const op of ops) {
      // Our own op, seen in the log after a reconnect: it was committed, so it
      // no longer needs sending.
      if (fromLog && op.opId) {
        const i = this.outbox.findIndex((e) => e.op.opId === op.opId);
        if (i !== -1 && !this.outbox[i].inflight) this.outbox.splice(i, 1);
      }
      if (!this.state.applyOp(op)) continue;
      changed = true;
      this.#record({ seq: op.seq, type: op.type, id: op.id, fields: Object.keys(op.fields ?? {}), by: op.by?.name, node: op.node, own: op.by?.id === this.user.id });
      if (op.type === 'update' && (op.fields?.x !== undefined || op.fields?.y !== undefined)) {
        for (const [sid, d] of this.drags) if (d.id === op.id) this.drags.delete(sid);
      }
      if (op.type === 'delete') this.locks.delete(op.id);
    }
    if (changed) this.#changed();
  }

  #presence({ type, member }) {
    if (member.sid === this.you) return;
    if (type === 'join') this.presence.set(member.sid, member);
    else {
      this.presence.delete(member.sid);
      this.cursors.delete(member.sid);
      this.drags.delete(member.sid);
    }
    this.#changed();
  }

  #remoteLock({ id, by, ttlMs }) {
    if (by && by.sid === this.you) return;
    if (by) this.locks.set(id, { by, expiresAt: this.now() + (ttlMs ?? 10_000) });
    else this.locks.delete(id);
    this.#changed();
  }

  // Outbox

  #enqueue(raw) {
    const checked = validateOp(raw);
    if (!checked.ok) {
      this.#setNotice(checked.error.message);
      return;
    }
    const op = checked.op;
    const last = this.outbox.at(-1);
    const mergeable = last && !last.inflight && last.attempts === 0 && last.op.id === op.id;
    if (mergeable && op.type === 'update' && (last.op.type === 'update' || last.op.type === 'create')) {
      Object.assign(last.op.fields, op.fields);
    } else if (mergeable && op.type === 'delete' && last.op.type === 'create') {
      this.outbox.pop(); // created and deleted before it was ever sent
    } else {
      this.outbox.push({ op: { ...op, createdAt: this.now() }, attempts: 0, inflight: false });
    }
    this.#changed();
    this.#flush();
  }

  #flush() {
    if (!this.joined || this.sending || this.retryTimer) return;
    const entry = this.outbox[0];
    if (!entry) return;
    this.sending = true;
    entry.inflight = true;
    const generation = this.generation;
    const { createdAt, ...op } = entry.op;
    this.socket.timeout(this.ackTimeoutMs).emitWithAck('op', op).then(
      (res) => this.#acked(generation, entry, res),
      () => this.#acked(generation, entry, { ok: false, error: { code: 'TIMEOUT', retryable: true, message: 'no reply from server' } }),
    );
  }

  #acked(generation, entry, res) {
    if (generation !== this.generation) return; // a reconnect already reset the outbox
    this.sending = false;
    entry.inflight = false;
    if (this.outbox[0] !== entry) {
      // Removed meanwhile (seen in the log during a catch-up).
      this.#flush();
      return;
    }
    if (res.ok) {
      this.outbox.shift();
      this.stats.acks++;
      if (res.seq == null) {
        this.#sync(); // committed by an earlier attempt, seq unknown: read it from the log
      } else {
        this.state.applyOp({ seq: res.seq, type: entry.op.type, id: entry.op.id, fields: res.fields });
        if (!res.duplicate) this.#record({ seq: res.seq, type: entry.op.type, id: entry.op.id, fields: Object.keys(res.fields ?? {}), by: this.user.name, node: this.node, own: true });
      }
      this.#changed();
      this.#flush();
      return;
    }
    if (res.error?.retryable) {
      this.stats.retries++;
      entry.attempts++;
      const delay = res.error.retryAfterMs ?? Math.min(250 * 2 ** Math.min(entry.attempts, 4), 4000);
      if (res.error.code === 'UNAVAILABLE') this.#setNotice('Saving paused: board storage is unavailable. Retrying…', { connection: true });
      this.retryTimer = setTimeout(() => {
        this.retryTimer = null;
        this.#flush();
      }, delay);
      this.#changed();
      return;
    }
    // Rejected for good (the note was deleted, someone else is editing it, ...).
    this.outbox.shift();
    this.#setNotice(res.error?.message ?? 'change was rejected');
    this.#flush();
  }

  // Helpers

  #throttled(kind, payload) {
    const t = this.throttle.get(kind);
    if (t) {
      t.pending = payload;
      return;
    }
    if (this.joined) this.socket.emit(kind, payload);
    const entry = { pending: null };
    entry.timer = setTimeout(() => {
      this.throttle.delete(kind);
      if (entry.pending) this.#throttled(kind, entry.pending);
    }, this.throttleMs);
    this.throttle.set(kind, entry);
  }

  #record(item) {
    this.activity = [{ ...item, at: this.now() }, ...this.activity].slice(0, ACTIVITY_MAX);
  }

  #setNotice(message, extra = {}) {
    this.notice = { message, at: this.now(), ...extra };
    this.#changed();
  }

  #tick() {
    const t = this.now();
    let changed = false;
    for (const [sid, c] of this.cursors) if (t - c.at > 15_000) { this.cursors.delete(sid); changed = true; }
    for (const [sid, d] of this.drags) if (t - d.at > 2_000) { this.drags.delete(sid); changed = true; }
    for (const [id, l] of this.locks) if (l.expiresAt <= t) { this.locks.delete(id); changed = true; }
    if (this.notice && !this.notice.connection && t - this.notice.at > 6_000) { this.notice = null; changed = true; }
    if (this.notice?.connection && this.outbox.length === 0 && this.status === 'online') { this.notice = null; changed = true; }
    const gap = this.state.gap();
    if (gap && t - gap.gapSince > this.gapTimeoutMs) this.#sync();
    if (changed) this.#changed();
  }

  #changed() {
    this.changes++;
    for (const listener of this.listeners) listener();
  }
}


function aggregateStats(data) {
  if (!data) return null;
  return data;
}


function checkStatus(data) {
  if (!data) return null;
  return data;
}

function aggregateStats(data) {
  if (!data) return null;
  return data;
}

function checkStatus(data) {
  if (!data) return null;
  return data;
}