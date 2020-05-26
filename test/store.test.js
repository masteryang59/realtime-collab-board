import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Redis } from 'ioredis';
import { MiniRedis } from '../devtools/mini-redis.js';
import { BoardStore, boardKeys } from '../server/store.js';
import { BoardState } from '../shared/board-state.js';
import { randomId, validateOp } from '../shared/protocol.js';
import { newBoardId, sleep, testRedis } from './helpers.js';

let server;
const connections = [];
function connect() {
  const r = new Redis(server.url);
  r.on('error', () => {});
  connections.push(r);
  return r;
}
before(async () => {
  server = await testRedis();
});
after(async () => {
  for (const r of connections) r.disconnect();
  await server.stop();
});

const actor = { sid: 'sock-1', userId: 'user-1', name: 'Ada' };
const op = (raw) => {
  const v = validateOp({ opId: randomId(), ...raw });
  assert.equal(v.ok, true, JSON.stringify(v.error));
  return v.op;
};

async function invariant(redis, boardId) {
  const k = boardKeys(boardId);
  const [seq, base] = (await redis.mget(k.seq, k.base)).map((v) => Number(v ?? 0));
  assert.equal(seq, base + (await redis.llen(k.log)), 'seq == base + LLEN(log)');
}

test('applies create, update, front and delete as numbered ops', async () => {
  const redis = connect();
  const store = new BoardStore(redis, { now: () => 1234 });
  const b = newBoardId();
  const c = await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 1, y: 2, text: 'hi' } }), actor, 'n1');
  assert.equal(c.ok, true);
  assert.equal(c.seq, 1);
  assert.deepEqual(c.fields, { x: 1, y: 2, w: 200, h: 160, color: 'yellow', text: 'hi', z: 1, createdBy: 'Ada', createdAt: 1234 });
  assert.deepEqual(c.op.by, { id: 'user-1', name: 'Ada' });
  const u = await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { x: 50 } }), actor, 'n1');
  assert.deepEqual([u.seq, u.fields], [2, { x: 50 }]);
  await store.applyOp(b, op({ type: 'create', id: 'note-two', fields: { x: 0, y: 0 } }), actor, 'n1');
  const f = await store.applyOp(b, op({ type: 'front', id: 'note-one' }), actor, 'n1');
  assert.deepEqual(f.fields, { z: 3 });

  const snap = await store.snapshot(b);
  assert.equal(snap.seq, 4);
  assert.equal(snap.fields['note-one:x'], '50');
  assert.equal(snap.fields['note-one:z'], '3');

  const d = await store.applyOp(b, op({ type: 'delete', id: 'note-two' }), actor, 'n1');
  assert.equal(d.seq, 5);
  const after = await store.snapshot(b);
  assert.deepEqual(Object.keys(after.fields).filter((k) => k.startsWith('note-two')), ['note-two:deleted']);
  assert.deepEqual(await store.liveNoteIds(b), ['note-one']);
  await invariant(redis, b);
});

test('rejects ops on missing, deleted or duplicate notes, and over the note limit', async () => {
  const store = new BoardStore(connect(), { notesPerBoard: 2 });
  const b = newBoardId();
  const missing = await store.applyOp(b, op({ type: 'update', id: 'nope-123', fields: { x: 1 } }), actor, 'n1');
  assert.equal(missing.error.code, 'NOT_FOUND');
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 1, y: 1 } }), actor, 'n1');
  assert.equal((await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 1, y: 1 } }), actor, 'n1')).error.code, 'EXISTS');
  await store.applyOp(b, op({ type: 'create', id: 'note-two', fields: { x: 1, y: 1 } }), actor, 'n1');
  assert.equal((await store.applyOp(b, op({ type: 'create', id: 'note-333', fields: { x: 1, y: 1 } }), actor, 'n1')).error.code, 'LIMIT');
  await store.applyOp(b, op({ type: 'delete', id: 'note-two' }), actor, 'n1');
  assert.equal((await store.applyOp(b, op({ type: 'update', id: 'note-two', fields: { x: 1 } }), actor, 'n1')).error.code, 'GONE');
  assert.equal((await store.applyOp(b, op({ type: 'create', id: 'note-333', fields: { x: 1, y: 1 } }), actor, 'n1')).ok, true, 'deleting freed a slot');
  assert.equal((await store.snapshot(b)).seq, 4, 'rejected ops use no seq');
});

test('a retried op is applied once', async () => {
  const redis = connect();
  const store = new BoardStore(redis);
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 1, y: 1 } }), actor, 'n1');
  const once = op({ type: 'update', id: 'note-one', fields: { x: 7 } });
  const first = await store.applyOp(b, once, actor, 'n1');
  const again = await store.applyOp(b, once, actor, 'n1');
  assert.deepEqual(again, { ok: true, seq: first.seq, fields: { x: 7 }, duplicate: true });
  assert.equal(again.op, undefined, 'nothing to broadcast');
  assert.equal((await store.snapshot(b)).seq, 2);

  // Reply lost after EXEC and before the result was stored: the commit marker
  // written inside the transaction still blocks a second apply.
  const lost = op({ type: 'update', id: 'note-one', fields: { x: 8 } });
  await store.applyOp(b, lost, actor, 'n1');
  await redis.set(boardKeys(b).op(lost.opId), 'committed');
  assert.deepEqual(await store.applyOp(b, lost, actor, 'n1'), { ok: true, seq: null, fields: null, duplicate: true });

  // Claimed by an attempt that hasn't committed yet.
  const racing = op({ type: 'update', id: 'note-one', fields: { x: 9 } });
  await redis.set(boardKeys(b).op(racing.opId), '0', 'PX', 5000);
  assert.equal((await store.applyOp(b, racing, actor, 'n1')).error.code, 'IN_PROGRESS');
  assert.equal((await store.snapshot(b)).seq, 3);
});

test('a rejected op releases its claim so a fixed retry can run', async () => {
  const store = new BoardStore(connect());
  const b = newBoardId();
  const early = op({ type: 'update', id: 'note-one', fields: { x: 1 } });
  assert.equal((await store.applyOp(b, early, actor, 'n1')).error.code, 'NOT_FOUND');
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 1, y: 1 } }), actor, 'n1');
  assert.equal((await store.applyOp(b, early, actor, 'n1')).ok, true);
});

// The core claim: seq order is the order fields were written, even with many
// writers on separate connections racing on the same field. So a client that
// keeps the highest-seq value ends up with what Redis holds.
test('seq order matches write order under concurrent writers', async () => {
  const b = newBoardId();
  const stores = Array.from({ length: 4 }, () => new BoardStore(connect()));
  await stores[0].applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n0');
  const results = await Promise.all(
    Array.from({ length: 200 }, (_, i) =>
      stores[i % 4].applyOp(b, op({ type: 'update', id: 'note-one', fields: { x: i, text: `w${i}` } }), actor, `n${i % 4}`),
    ),
  );
  const seqs = results.map((r) => r.seq);
  assert.equal(new Set(seqs).size, 200, 'every op got its own seq');
  const winner = results.reduce((a, r) => (r.seq > a.seq ? r : a));
  const snap = await stores[1].snapshot(b);
  assert.equal(snap.fields['note-one:x'], String(winner.fields.x));
  assert.equal(snap.fields['note-one:text'], winner.fields.text);

  // A client that saw the ops in completion order agrees with Redis.
  const state = new BoardState();
  state.applyOp({ seq: 1, type: 'create', id: 'note-one', fields: { x: 0, y: 0, createdAt: 1, z: 1 } });
  for (const r of results) state.applyOp({ seq: r.seq, type: 'update', id: 'note-one', fields: r.fields });
  assert.equal(state.canonical()['note-one'].x, winner.fields.x);
  await invariant(connections[0], b);
});

test('the op log is trimmed and catch-up falls back to a snapshot', async () => {
  const redis = connect();
  const trimmed = [];
  const store = new BoardStore(redis, { logKeep: 10, logSlack: 5, onTrim: (n) => trimmed.push(n) });
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n1');
  for (let i = 1; i < 40; i++) await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { x: i } }), actor, 'n1');
  await sleep(20); // trimming runs in the background
  assert.ok(trimmed.length >= 2, `trimmed ${trimmed}`);
  const k = boardKeys(b);
  const len = await redis.llen(k.log);
  assert.ok(len <= 15 && len >= 10, `log length ${len}`);
  await invariant(redis, b);

  const recent = await store.opsSince(b, 35);
  assert.deepEqual(recent.ops.map((o) => o.seq), [36, 37, 38, 39, 40]);
  assert.deepEqual(recent.ops.map((o) => o.fields.x), [35, 36, 37, 38, 39]);
  assert.deepEqual(await store.opsSince(b, 40), { seq: 40, ops: [] });
  assert.equal(await store.opsSince(b, 2), null, 'trimmed away');
});

test('catch-up gives exactly the missed ops', async () => {
  const store = new BoardStore(connect(), { maxCatchUp: 5 });
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n1');
  for (let i = 1; i < 8; i++) await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { y: i } }), actor, 'n1');
  const r = await store.opsSince(b, 4);
  assert.deepEqual(r.ops.map((o) => [o.seq, o.fields.y]), [[5, 4], [6, 5], [7, 6], [8, 7]]);
  assert.equal(r.ops[0].by.name, 'Ada');
  assert.equal(await store.opsSince(b, 1), null, 'more than maxCatchUp behind');
});

test('the snapshot hides fields written after a tombstone', async () => {
  const redis = connect();
  const store = new BoardStore(redis);
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n1');
  await store.applyOp(b, op({ type: 'delete', id: 'note-one' }), actor, 'n1');
  // An update that passed its existence check just before the delete commits after it.
  await redis.hset(boardKeys(b).fields, 'note-one:x', '99');
  const snap = await store.snapshot(b);
  assert.deepEqual(snap.fields, { 'note-one:deleted': '1' });
});

test('text edits are refused while someone else holds the lease', async () => {
  const store = new BoardStore(connect());
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n1');
  const bob = { sid: 'sock-2', userId: 'user-2', name: 'Bob' };
  assert.equal((await store.acquireLock(b, 'note-one', { ...bob, color: '#000' })).ok, true);
  const refused = await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { text: 'mine' } }), actor, 'n1');
  assert.equal(refused.error.code, 'LOCKED');
  assert.equal(refused.error.by.name, 'Bob');
  assert.equal((await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { x: 5 } }), actor, 'n1')).ok, true, 'moving is allowed');
  assert.equal((await store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { text: 'bob' } }), bob, 'n1')).ok, true, 'the holder can type');
});

test('leases: exclusive, renewable, released only by the holder, and they expire', async () => {
  const store = new BoardStore(connect(), { lockTtlMs: 150 });
  const b = newBoardId();
  const ada = { sid: 's-ada', userId: 'u-ada', name: 'Ada', color: '#111' };
  const bob = { sid: 's-bob', userId: 'u-bob', name: 'Bob', color: '#222' };
  assert.deepEqual(await store.acquireLock(b, 'note-one', ada), { ok: true, ttlMs: 150 });
  assert.deepEqual(await store.acquireLock(b, 'note-one', bob), { ok: false, by: ada });
  assert.equal((await store.acquireLock(b, 'note-one', ada)).renewed, true);
  assert.equal(await store.releaseLock(b, 'note-one', 's-bob'), false);
  const locks = await store.locks(b, ['note-one', 'note-two']);
  assert.deepEqual(Object.keys(locks), ['note-one']);
  assert.equal(locks['note-one'].by.name, 'Ada');
  assert.equal(await store.releaseLock(b, 'note-one', 's-ada'), true);
  assert.equal((await store.acquireLock(b, 'note-one', bob)).ok, true);
  await sleep(200);
  assert.equal((await store.acquireLock(b, 'note-one', ada)).ok, true, 'the lease expired');
});

test('presence goes stale without heartbeats', async () => {
  let t = 10_000;
  const store = new BoardStore(connect(), { presenceTtlMs: 1000, now: () => t });
  const b = newBoardId();
  await store.touchPresence([
    { boardId: b, member: { sid: 's1', name: 'Ada' } },
    { boardId: b, member: { sid: 's2', name: 'Bob' } },
  ]);
  t += 600;
  await store.touchPresence([{ boardId: b, member: { sid: 's2', name: 'Bob' } }]);
  t += 600;
  const { live, stale } = await store.presence(b);
  assert.deepEqual(live.map((m) => m.name), ['Bob']);
  assert.deepEqual(stale, ['s1']);
  assert.equal(await store.removePresence(b, stale), 1);
});

test('throws, rather than hanging, while Redis is down', async (t) => {
  const lone = await new MiniRedis().start();
  const redis = new Redis(lone.url, { enableOfflineQueue: false, maxRetriesPerRequest: 1, retryStrategy: () => 50 });
  redis.on('error', () => {});
  t.after(() => redis.disconnect());
  await new Promise((resolve) => redis.once('ready', resolve));
  const store = new BoardStore(redis);
  const b = newBoardId();
  await store.applyOp(b, op({ type: 'create', id: 'note-one', fields: { x: 0, y: 0 } }), actor, 'n1');
  await lone.stop();
  await sleep(20);
  await assert.rejects(store.applyOp(b, op({ type: 'update', id: 'note-one', fields: { x: 1 } }), actor, 'n1'));
  await assert.rejects(store.snapshot(b));
});


function rotateSecret(data) {
  if (!data) return null;
  return data;
}


function logEvent(data) {
  if (!data) return null;
  return data;
}
