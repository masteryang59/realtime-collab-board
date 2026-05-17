// Integration tests: real Socket.io nodes joined by the Redis adapter over a
// real TCP Redis protocol (mini-redis), driven by the same BoardClient the UI uses.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { io } from 'socket.io-client';
import { BoardStore } from '../server/store.js';
import { converged, externalRedis, newBoardId, sleep, startCluster, waitFor } from './helpers.js';

async function withCluster(t, options) {
  const cluster = await startCluster(options);
  t.after(() => cluster.stop());
  return cluster;
}

const noteIds = (client) => client.getView().notes.map((n) => n.id).sort();

test('changes made on one node reach clients on the other', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  assert.deepEqual([ada.node, bob.node], ['node-a', 'node-b']);

  const id = ada.createNote({ x: 100, y: 80, text: 'Ship it', color: 'pink' });
  await waitFor(() => bob.getView().notes.length === 1, { message: 'Bob sees the note' });
  const seen = bob.getView().notes[0];
  assert.deepEqual([seen.id, seen.text, seen.color, seen.createdBy], [id, 'Ship it', 'pink', 'Ada']);

  bob.updateNote(id, { x: 400, text: 'Ship it Friday' });
  await waitFor(() => ada.getView().notes[0]?.text === 'Ship it Friday', { message: 'Ada sees the edit' });
  assert.equal(ada.getView().notes[0].x, 400);
  assert.equal(ada.getView().activity[0].node, 'node-b', 'the activity feed shows which node took the write');

  bob.deleteNote(id);
  await waitFor(() => ada.getView().notes.length === 0, { message: 'Ada sees the delete' });
  await converged([ada, bob]);
});

test('presence and cursors cross nodes', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  await waitFor(() => ada.getView().presence.some((m) => m.name === 'Bob'), { message: 'Ada sees Bob' });
  assert.deepEqual(bob.getView().presence.map((m) => [m.name, m.node]), [['Ada', 'node-a']]);

  bob.sendCursor(321, 123);
  await waitFor(() => ada.getView().cursors.length === 1, { message: 'cursor' });
  assert.deepEqual(ada.getView().cursors[0], { sid: bob.you, x: 321, y: 123, name: 'Bob', color: bob.user.color });

  bob.stop();
  await waitFor(() => ada.getView().presence.length === 0, { message: 'Bob leaves' });
  assert.equal(ada.getView().cursors.length, 0);
});

test('drag previews are relayed and cleared by the committed move', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);
  ada.sendDrag(id, 50, 60);
  await waitFor(() => bob.getView().drags[id], { message: 'drag preview' });
  assert.deepEqual(bob.getView().drags[id], { x: 50, y: 60, name: 'Ada', color: ada.user.color });
  ada.endDrag(id);
  ada.updateNote(id, { x: 55, y: 66 });
  await waitFor(() => !bob.getView().drags[id] && bob.getView().notes[0].x === 55, { message: 'preview cleared' });
});

test('one client\'s rapid updates land in the order they were made', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0 });
  const bob = await cluster.client(board, { node: 1 });
  const id = ada.createNote({ x: 0, y: 0 });
  for (let i = 1; i <= 80; i++) ada.updateNote(id, { x: i * 10, text: `step ${i}` });
  const state = await converged([ada, bob]);
  assert.equal(state[id].x, 800);
  assert.equal(state[id].text, 'step 80');
  assert.ok(ada.stats.acks < 80, `unsent updates were merged (${ada.stats.acks} acks)`);
});

// Several users on both nodes editing the same notes at the same time. They
// all have to end up identical, and identical to what Redis holds (a fresh
// client loads it as a snapshot).
test('concurrent edits from many clients on two nodes converge', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const clients = [];
  for (let i = 0; i < 6; i++) clients.push(await cluster.client(board, { node: i % 2, name: `u${i}` }));

  let seed = 7;
  const rand = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
  const colors = ['yellow', 'pink', 'blue', 'green'];
  await Promise.all(
    clients.map(async (c) => {
      for (let step = 0; step < 60; step++) {
        const notes = c.getView().notes;
        const r = rand();
        if (notes.length < 4 || r < 0.12) c.createNote({ x: Math.floor(rand() * 900), y: Math.floor(rand() * 600) });
        else {
          const n = notes[Math.floor(rand() * notes.length)];
          if (r < 0.17) c.deleteNote(n.id);
          else if (r < 0.3) c.bringToFront(n.id);
          else c.updateNote(n.id, rand() < 0.5 ? { x: Math.floor(rand() * 900), y: Math.floor(rand() * 600) } : { text: `${c.user.name}-${step}`, color: colors[step % 4] });
        }
        if (step % 5 === 0) await sleep(Math.floor(rand() * 8));
      }
    }),
  );
  const state = await converged(clients, { timeout: 10_000 });
  assert.ok(Object.keys(state).length > 0);
  const fresh = await cluster.client(board, { node: 1, name: 'late' });
  assert.deepEqual(fresh.state.canonical(), state, 'clients match Redis');

  // Sanity: the board seq is the number of applied ops, with no gaps.
  const store = new BoardStore(cluster.servers[0].redis.cmd);
  assert.equal((await store.snapshot(board)).seq, fresh.state.seq);
  assert.ok(fresh.state.seq > 150, `${fresh.state.seq} ops applied`);
});

test('text-edit leases: one editor at a time, released on disconnect', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);

  assert.equal((await ada.lock(id)).ok, true);
  await waitFor(() => bob.getView().locks[id]?.name === 'Ada', { message: 'Bob sees the lease' });
  const refused = await bob.lock(id);
  assert.equal(refused.ok, false);
  assert.equal(refused.by.name, 'Ada');

  // Typing anyway is refused by the server; moving is allowed.
  bob.updateNote(id, { text: 'sneaky' });
  bob.updateNote(id, { x: 250 });
  ada.updateNote(id, { text: 'Ada was here' });
  const state = await converged([ada, bob]);
  assert.equal(state[id].text, 'Ada was here');
  assert.equal(state[id].x, 250);
  assert.match(bob.getView().notice.message, /Ada is editing/);

  ada.stop();
  await waitFor(() => !bob.getView().locks[id], { message: 'lease released' });
  assert.equal((await bob.lock(id)).ok, true);
});

test('a crashed node\'s leases and presence expire', async (t) => {
  const cluster = await withCluster(t, { nodeOptions: { heartbeatMs: 100, storeOptions: { lockTtlMs: 400, presenceTtlMs: 400 } } });
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada', urls: [cluster.servers[0].url] });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);
  assert.equal((await ada.lock(id)).ok, true);
  await waitFor(() => bob.getView().presence.length === 1);

  await cluster.servers[0].kill(); // no cleanup reaches Redis
  ada.stop();
  await waitFor(async () => (await bob.lock(id)).ok, { timeout: 3000, interval: 100, message: 'lease expiry' });
  await waitFor(() => bob.getView().presence.length === 0, { timeout: 3000, message: 'stale presence swept' });
});

test('a client that drops off catches up from the op log, not a snapshot', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);

  bob.socket.io.engine.close(); // network blip: transport closes, client reconnects
  for (let i = 0; i < 20; i++) ada.updateNote(id, { y: i });
  ada.createNote({ x: 5, y: 5, text: 'while you were away' });
  bob.updateNote(id, { color: 'green' }); // made offline, sent after reconnecting
  await converged([ada, bob]);
  assert.equal(bob.stats.joins, 2);
  assert.equal(bob.stats.snapshots, 1, 'only the first join loaded a snapshot');
  assert.equal(noteIds(bob).length, 2);
  assert.equal(ada.getView().notes.find((n) => n.id === id).color, 'green');
});

test('a client too far behind gets a snapshot', async (t) => {
  const cluster = await withCluster(t, { nodeOptions: { storeOptions: { logKeep: 5, logSlack: 2 } } });
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0 });
  const bob = await cluster.client(board, { node: 1 });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);
  bob.socket.disconnect(); // stays offline until reconnected below
  await waitFor(() => bob.status !== 'online');
  for (let i = 0; i < 30; i++) {
    ada.updateNote(id, { x: i });
    await waitFor(() => ada.outbox.length === 0); // one op each, no merging
  }
  bob.socket.connect();
  await converged([ada, bob]);
  assert.equal(bob.stats.snapshots, 2);
});

test('when a node dies its clients move to another node and send what they had queued', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada', failoverAfter: 2 });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);

  await cluster.servers[0].kill();
  await waitFor(() => ada.status === 'reconnecting');
  ada.updateNote(id, { text: 'typed while offline' });
  ada.createNote({ x: 9, y: 9 });
  assert.equal(ada.getView().unsaved, 2);
  assert.equal(ada.getView().notes.find((n) => n.id === id).text, 'typed while offline', 'shown optimistically');

  await waitFor(() => ada.status === 'online' && ada.node === 'node-b', { timeout: 6000, message: 'failover' });
  const state = await converged([ada, bob]);
  assert.equal(state[id].text, 'typed while offline');
  assert.equal(Object.keys(state).length, 2);
  assert.ok(ada.stats.failovers >= 1);
});

test('writes during a Redis outage wait in the outbox and apply when it is back', { skip: externalRedis ? 'needs a Redis the test can stop' : false }, async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);

  const port = cluster.redis.port;
  await cluster.redis.stop();
  const health = await fetch(`${cluster.servers[0].url}/health`);
  assert.equal(health.status, 503);
  assert.equal((await health.json()).status, 'degraded');

  ada.updateNote(id, { text: 'saved later' });
  await waitFor(() => ada.stats.retries > 0, { message: 'a retry' });
  assert.match(ada.getView().notice.message, /Saving paused/);
  assert.equal(ada.getView().notes[0].text, 'saved later');

  await cluster.redis.start(port);
  const state = await converged([ada, bob], { timeout: 10_000 });
  assert.equal(state[id].text, 'saved later');
  await waitFor(() => ada.getView().notice === null, { message: 'the notice clears once saved' });
  assert.equal((await fetch(`${cluster.servers[0].url}/health`)).status, 200);
});

test('pub/sub messages lost while a node\'s subscriber was down are recovered', async (t) => {
  const cluster = await withCluster(t);
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0, name: 'Ada' });
  const bob = await cluster.client(board, { node: 1, name: 'Bob' });
  const id = ada.createNote({ x: 0, y: 0 });
  await converged([ada, bob]);

  const sub = cluster.servers[1].redis.sub;
  sub.disconnect(true); // node-b's subscriber drops and reconnects shortly
  ada.updateNote(id, { text: 'published into the void' });
  await waitFor(() => ada.outbox.length === 0);
  await converged([ada, bob]);
  assert.equal(bob.getView().notes[0].text, 'published into the void');
  assert.ok(bob.stats.syncs >= 1, 'Bob caught up from the log');
  assert.ok(cluster.servers[1].stats.resyncHints >= 1);
});

test('too many changes are rate limited and retried, not lost', async (t) => {
  const cluster = await withCluster(t, { nodeOptions: { opRate: { rate: 50, burst: 5 } } });
  const board = newBoardId();
  const ada = await cluster.client(board, { node: 0 });
  const bob = await cluster.client(board, { node: 1 });
  for (let i = 0; i < 25; i++) ada.createNote({ x: i, y: i });
  const state = await converged([ada, bob], { timeout: 8000 });
  assert.equal(Object.keys(state).length, 25);
  assert.ok(ada.stats.retries > 0);
});

test('the server validates everything a socket sends', async (t) => {
  const cluster = await withCluster(t);
  const socket = io(cluster.servers[0].url, { transports: ['websocket'], forceNew: true });
  t.after(() => socket.disconnect());
  await waitFor(() => socket.connected);
  const ask = (event, payload) => socket.timeout(2000).emitWithAck(event, payload);

  assert.equal((await ask('op', { type: 'create', id: 'note-123', opId: 'op-12345', fields: { x: 1, y: 1 } })).error.code, 'BAD_REQUEST', 'op before join');
  assert.equal((await ask('join', { boardId: '../etc', user: { id: 'user-1234', name: 'x' } })).error.code, 'BAD_REQUEST');
  assert.equal((await ask('join', { boardId: newBoardId(), user: { id: 'u', name: '' } })).error.code, 'BAD_REQUEST');
  const joined = await ask('join', { boardId: newBoardId(), user: { id: 'user-1234', name: 'Raw' } });
  assert.equal(joined.ok, true);
  assert.deepEqual(joined.snapshot, { seq: 0, fields: {} });
  assert.equal((await ask('join', { boardId: newBoardId(), user: { id: 'user-1234', name: 'Raw' } })).error.code, 'BAD_REQUEST', 'one board per socket');
  assert.equal((await ask('op', { type: 'update', id: 'note-123', opId: 'op-12345', fields: { z: 1 } })).error.code, 'BAD_REQUEST');
  assert.equal((await ask('op', { type: 'update', id: 'note-123', opId: 'op-12345', fields: { x: 1 } })).error.code, 'NOT_FOUND');
  assert.equal((await ask('lock', { id: 'note-123', action: 'steal' })).error.code, 'BAD_REQUEST');
  assert.equal((await ask('sync', { since: 0 })).ok, true);
});

test('health reports the node and its Redis connections', async (t) => {
  const cluster = await withCluster(t, { nodes: 1 });
  const res = await fetch(`${cluster.servers[0].url}/health`);
  const body = await res.json();
  assert.equal(res.status, 200);
  assert.equal(body.node, 'node-a');
  assert.deepEqual(body.redis, { cmd: 'ready', pub: 'ready', sub: 'ready' });
});


function auditAction(data) {
  if (!data) return null;
  return data;
}


function cleanupResources(data) {
  if (!data) return null;
  return data;
}

function auditAction(data) {
  if (!data) return null;
  return data;
}

function cleanupResources(data) {
  if (!data) return null;
  return data;
}