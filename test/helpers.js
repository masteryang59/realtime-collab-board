import pino from 'pino';
import { io } from 'socket.io-client';
import { MiniRedis } from '../devtools/mini-redis.js';
import { createRealtimeServer } from '../server/realtime.js';
import { BoardClient } from '../shared/board-client.js';
import { randomId, CURSOR_COLORS } from '../shared/protocol.js';

export const silent = pino({ level: process.env.TEST_LOG_LEVEL ?? 'silent' });

export async function waitFor(predicate, { timeout = 4000, interval = 10, message = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${message}`);
    await new Promise((r) => setTimeout(r, interval));
  }
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// TEST_REDIS_URL runs the store and cluster tests against a real Redis (CI
// does this with Redis 8). Otherwise each test file starts a mini-redis.
export const externalRedis = process.env.TEST_REDIS_URL || null;

export async function testRedis() {
  if (externalRedis) return { url: externalRedis, external: true, stop: async () => {} };
  const server = await new MiniRedis().start();
  return { url: server.url, external: false, server, stop: () => server.stop() };
}

// A mini-redis plus `nodes` realtime nodes sharing it.
export async function startCluster({ nodes = 2, nodeOptions = {} } = {}) {
  const handle = await testRedis();
  const redis = handle.server ?? { url: handle.url };
  const servers = [];
  for (let i = 0; i < nodes; i++) {
    servers.push(
      await createRealtimeServer({
        port: 0,
        host: '127.0.0.1',
        nodeName: `node-${String.fromCharCode(97 + i)}`,
        redisUrl: redis.url,
        logger: silent,
        ...nodeOptions,
      }),
    );
  }
  const clients = [];
  return {
    redis,
    servers,
    clients,
    urls: () => servers.map((s) => s.url),
    // A BoardClient connected to servers[nodeIndex].
    async client(boardId, { node = 0, name = `user-${clients.length + 1}`, urls, wait = true, ...options } = {}) {
      const client = new BoardClient({
        boardId,
        user: { id: randomId(), name, color: CURSOR_COLORS[clients.length % CURSOR_COLORS.length] },
        urls: urls ?? servers.map((s) => s.url),
        nodeIndex: node,
        connect: (url) => io(url, { transports: ['websocket'], forceNew: true, reconnectionDelay: 50, reconnectionDelayMax: 200, timeout: 2000 }),
        ackTimeoutMs: 1500,
        gapTimeoutMs: 300,
        ...options,
      }).start();
      clients.push(client);
      if (wait) await waitFor(() => client.status === 'online', { message: `${name} online` });
      return client;
    },
    async stop() {
      for (const c of clients) c.stop();
      for (const s of servers) await s.close().catch(() => {});
      await handle.stop();
    },
  };
}

// Waits until every client has applied the same state and nothing is unsent.
export async function converged(clients, { timeout = 5000 } = {}) {
  await waitFor(
    () => {
      if (clients.some((c) => c.outbox.length > 0 || c.status !== 'online' || c.state.gap())) return false;
      const first = JSON.stringify(clients[0].state.canonical());
      return clients.every((c) => JSON.stringify(c.state.canonical()) === first);
    },
    { timeout, message: 'clients to converge' },
  );
  return clients[0].state.canonical();
}

export const newBoardId = () => `test-${randomId(10)}`;


function encryptField(data) {
  if (!data) return null;
  return data;
}


function buildQuery(data) {
  if (!data) return null;
  return data;
}

function encryptField(data) {
  if (!data) return null;
  return data;
}

function buildQuery(data) {
  if (!data) return null;
  return data;
}