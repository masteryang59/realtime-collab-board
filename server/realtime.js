// One realtime node: an HTTP server with Socket.io on it, joined to every other
// node through the Redis adapter. Run several behind any load balancer; a
// client can talk to any of them because board state lives only in Redis.
import http from 'node:http';
import { Server } from 'socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import pino from 'pino';
import { isBoardId, isNoteId, validateOp, validatePoint, validateUser } from '../shared/protocol.js';
import { createRedisClients, waitForReady } from './redis.js';
import { BoardStore } from './store.js';
import { TokenBucket } from './rate-limit.js';

const room = (boardId) => `board:${boardId}`;

const UNAVAILABLE = { code: 'UNAVAILABLE', message: 'board storage is unavailable, retrying', retryable: true };

export async function createRealtimeServer(options = {}) {
  const {
    port = 4001,
    host = '0.0.0.0',
    nodeName = `node-${port}`,
    redisUrl = 'redis://127.0.0.1:6379',
    corsOrigins = ['http://localhost:3000'],
    logger = pino({ level: 'info' }),
    heartbeatMs = 10_000,
    opRate = { rate: 30, burst: 60 },
    ephemeralRate = { rate: 40, burst: 80 },
    maxQueued = 100,
    storeOptions = {},
  } = options;
  const log = logger.child({ node: nodeName });

  const redis = createRedisClients(redisUrl, log);
  const store = new BoardStore(redis.cmd, { ...storeOptions, onTrim: (n) => stats.trimmed += n });
  const stats = { ops: 0, rejected: 0, unavailable: 0, trimmed: 0, resyncHints: 0 };

  const httpServer = http.createServer((req, res) => {
    if (req.url === '/health') {
      const healthy = redis.cmd.status === 'ready' && redis.sub.status === 'ready';
      res.writeHead(healthy ? 200 : 503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        status: healthy ? 'ok' : 'degraded',
        node: nodeName,
        redis: { cmd: redis.cmd.status, pub: redis.pub.status, sub: redis.sub.status },
        sockets: io.engine.clientsCount,
        boards: localBoards.size,
        publishesDropped: redis.pub.dropped,
        ...stats,
      }));
      return;
    }
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('realtime-collab-board node. Socket.io is on /socket.io/, health on /health\n');
  });

  // WebSocket only: long-polling needs sticky sessions across several nodes,
  // and every browser this targets has WebSockets.
  const io = new Server(httpServer, {
    transports: ['websocket'],
    cors: { origin: corsOrigins },
    maxHttpBufferSize: 16 * 1024,
    pingInterval: 10_000,
    pingTimeout: 5_000,
    adapter: createAdapter(redis.pub, redis.sub, { key: 'rb-io' }),
  });

  // boardId -> Set<socket> for the sockets on this node, for heartbeats and
  // resync hints.
  const localBoards = new Map();
  let crashed = false; // set by close()/kill(): skip per-socket cleanup

  io.on('connection', (socket) => {
    const opBucket = new TokenBucket(opRate);
    const ephemeralBucket = new TokenBucket(ephemeralRate);
    socket.data.locks = new Set();
    let queue = Promise.resolve();
    let queued = 0;

    // Handlers with an ack run one at a time per socket, in arrival order, so
    // two updates from one client reach Redis in the order the client sent them.
    const serial = (handler) => (...args) => {
      const ack = typeof args.at(-1) === 'function' ? args.pop() : () => {};
      if (++queued > maxQueued) {
        log.warn({ sid: socket.id }, 'too many queued requests, disconnecting');
        socket.disconnect(true);
        return;
      }
      queue = queue
        .then(() => handler(...args))
        .catch((error) => {
          if (error?.code === 'BAD_REQUEST') return { ok: false, error: { code: 'BAD_REQUEST', message: error.message, retryable: false } };
          stats.unavailable++;
          log.warn({ err: error.message, sid: socket.id }, 'request failed');
          return { ok: false, error: UNAVAILABLE };
        })
        .then((reply) => {
          queued--;
          ack(reply);
        });
    };

    const joined = () => {
      if (!socket.data.boardId) throw Object.assign(new Error('join a board first'), { code: 'BAD_REQUEST' });
      return socket.data.boardId;
    };

    socket.on('join', serial(async (msg = {}) => {
      if (socket.data.boardId) throw Object.assign(new Error('already joined a board'), { code: 'BAD_REQUEST' });
      if (!isBoardId(msg.boardId)) throw Object.assign(new Error('invalid board id'), { code: 'BAD_REQUEST' });
      const user = validateUser(msg.user);
      if (!user.ok) throw Object.assign(new Error(user.error.message), { code: 'BAD_REQUEST' });
      const { boardId } = msg;

      // Join the room before reading, so every op committed after the read is
      // delivered to this socket. Ops committed before it are in the read.
      socket.join(room(boardId));
      try {
        let state = null;
        if (Number.isInteger(msg.since) && msg.since >= 0) state = await store.opsSince(boardId, msg.since);
        const payload = state ? { seq: state.seq, ops: state.ops } : { snapshot: await store.snapshot(boardId) };
        socket.data.boardId = boardId;
        socket.data.user = user.user;
        await store.touchPresence([{ boardId, member: memberOf(socket) }]);
        const [{ live }, noteIds] = await Promise.all([store.presence(boardId), store.liveNoteIds(boardId)]);
        const locks = await store.locks(boardId, noteIds);
        if (!localBoards.has(boardId)) localBoards.set(boardId, new Set());
        localBoards.get(boardId).add(socket);
        socket.to(room(boardId)).emit('presence', { type: 'join', member: memberOf(socket) });
        log.debug({ sid: socket.id, boardId, since: msg.since, mode: state ? 'ops' : 'snapshot' }, 'joined');
        return { ok: true, node: nodeName, you: socket.id, ...payload, presence: live, locks };
      } catch (error) {
        socket.leave(room(boardId));
        socket.data.boardId = undefined;
        throw error;
      }
    }));

    socket.on('op', serial(async (msg) => {
      const boardId = joined();
      const wait = opBucket.take();
      if (wait) return { ok: false, error: { code: 'RATE_LIMITED', message: 'too many changes', retryable: true, retryAfterMs: wait } };
      const v = validateOp(msg);
      if (!v.ok) {
        stats.rejected++;
        return { ok: false, error: { ...v.error, retryable: false } };
      }
      const actor = { sid: socket.id, userId: socket.data.user.id, name: socket.data.user.name };
      const result = await store.applyOp(boardId, v.op, actor, nodeName);
      if (!result.ok) {
        stats.rejected++;
        return result;
      }
      if (result.op) {
        stats.ops++;
        // To every other socket in the room, on every node, via Redis pub/sub.
        socket.to(room(boardId)).emit('op', result.op);
        if (v.op.type === 'delete') socket.to(room(boardId)).emit('lock', { id: v.op.id, by: null });
      }
      return { ok: true, seq: result.seq, fields: result.fields, duplicate: !!result.duplicate };
    }));

    socket.on('sync', serial(async (msg = {}) => {
      const boardId = joined();
      const since = Number.isInteger(msg.since) && msg.since >= 0 ? msg.since : null;
      const state = since === null ? null : await store.opsSince(boardId, since);
      if (state) return { ok: true, seq: state.seq, ops: state.ops };
      return { ok: true, snapshot: await store.snapshot(boardId) };
    }));

    socket.on('lock', serial(async (msg = {}) => {
      const boardId = joined();
      if (!isNoteId(msg.id)) throw Object.assign(new Error('invalid note id'), { code: 'BAD_REQUEST' });
      const holder = { sid: socket.id, userId: socket.data.user.id, name: socket.data.user.name, color: socket.data.user.color };
      if (msg.action === 'release') {
        socket.data.locks.delete(msg.id);
        if (await store.releaseLock(boardId, msg.id, socket.id)) socket.to(room(boardId)).emit('lock', { id: msg.id, by: null });
        return { ok: true };
      }
      if (msg.action !== 'acquire') throw Object.assign(new Error('action must be acquire or release'), { code: 'BAD_REQUEST' });
      const result = await store.acquireLock(boardId, msg.id, holder);
      if (!result.ok) return { ok: false, error: { code: 'LOCKED', message: `${result.by?.name ?? 'someone'} is editing this note`, retryable: false, by: result.by } };
      socket.data.locks.add(msg.id);
      const target = result.renewed ? socket.to(room(boardId)).volatile : socket.to(room(boardId));
      target.emit('lock', { id: msg.id, by: holder, ttlMs: result.ttlMs });
      return { ok: true, ttlMs: result.ttlMs };
    }));

    // Ephemeral: cursors and drag previews. Not stored and not acked; volatile,
    // so a slow socket drops them instead of buffering. Dropped ones don't
    // matter because the next one replaces them.
    socket.on('cursor', (msg) => {
      if (!socket.data.boardId || ephemeralBucket.take()) return;
      const p = validatePoint(msg);
      if (p) socket.to(room(socket.data.boardId)).volatile.emit('cursor', { sid: socket.id, ...p });
    });
    socket.on('drag', (msg = {}) => {
      if (!socket.data.boardId || !isNoteId(msg.id) || ephemeralBucket.take()) return;
      const p = msg.end ? { end: true } : validatePoint(msg);
      if (p) socket.to(room(socket.data.boardId)).volatile.emit('drag', { sid: socket.id, id: msg.id, ...p });
    });

    socket.on('disconnect', async () => {
      const boardId = socket.data.boardId;
      if (!boardId || crashed) return;
      localBoards.get(boardId)?.delete(socket);
      if (localBoards.get(boardId)?.size === 0) localBoards.delete(boardId);
      io.to(room(boardId)).emit('presence', { type: 'leave', member: { sid: socket.id } });
      try {
        for (const id of socket.data.locks) {
          if (await store.releaseLock(boardId, id, socket.id)) io.to(room(boardId)).emit('lock', { id, by: null });
        }
        await store.removePresence(boardId, [socket.id]);
      } catch (error) {
        // Redis is down: the lease and the presence entry expire on their own.
        log.debug({ err: error.message }, 'cleanup after disconnect failed');
      }
    });
  });

  // Refresh presence for this node's sockets and sweep members whose node
  // stopped refreshing (it crashed or lost Redis for longer than the TTL).
  const heartbeat = setInterval(async () => {
    const entries = [];
    for (const [boardId, sockets] of localBoards) {
      for (const socket of sockets) if (socket.data.user) entries.push({ boardId, member: memberOf(socket) });
    }
    try {
      await store.touchPresence(entries);
      for (const boardId of localBoards.keys()) {
        const { stale } = await store.presence(boardId);
        if (stale.length === 0) continue;
        await store.removePresence(boardId, stale);
        for (const sid of stale) io.to(room(boardId)).emit('presence', { type: 'leave', member: { sid } });
      }
    } catch (error) {
      log.debug({ err: error.message }, 'presence heartbeat failed');
    }
  }, heartbeatMs);
  heartbeat.unref();
  function memberOf(socket) {
    const u = socket.data.user;
    return { sid: socket.id, userId: u.id, name: u.name, color: u.color, node: nodeName };
  }

  // Pub/sub messages published while the subscriber was disconnected are gone
  // for good. When it's back, tell this node's clients to catch up from the
  // log; a client that doesn't receive another op would never see a gap.
  let subscriberReadyOnce = redis.sub.status === 'ready';
  redis.sub.on('ready', () => {
    if (!subscriberReadyOnce) {
      subscriberReadyOnce = true;
      return;
    }
    stats.resyncHints++;
    for (const boardId of localBoards.keys()) io.local.to(room(boardId)).emit('resync');
  });

  await Promise.all([waitForReady(redis.cmd, 5000), waitForReady(redis.sub, 5000)]);
  subscriberReadyOnce = true;
  if (redis.cmd.status !== 'ready') log.warn('redis is not reachable yet; serving in degraded mode');

  await new Promise((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(port, host, () => {
      httpServer.off('error', reject);
      resolve();
    });
  });
  const actualPort = httpServer.address().port;
  const connections = new Set();
  httpServer.on('connection', (c) => {
    connections.add(c);
    c.on('close', () => connections.delete(c));
  });
  log.info({ port: actualPort }, 'realtime node listening');

  async function stop(closeRedis) {
    for (const c of connections) c.destroy();
    // Close Socket.io while Redis is still connected: the adapter unsubscribes
    // on close, and that would wait forever on a disconnected subscriber.
    await new Promise((resolve) => io.close(() => resolve()));
    await closeRedis();
  }

  return {
    io,
    store,
    redis,
    stats,
    nodeName,
    port: actualPort,
    url: `http://127.0.0.1:${actualPort}`,
    // Graceful shutdown: release this node's leases and presence, tell the
    // room, then drop the connections so clients reconnect to another node.
    async close() {
      clearInterval(heartbeat);
      crashed = true; // the cleanup below replaces the per-socket handlers
      await Promise.all([...localBoards].map(async ([boardId, sockets]) => {
        try {
          for (const s of sockets) {
            for (const id of s.data.locks) {
              if (await store.releaseLock(boardId, id, s.id)) io.to(room(boardId)).emit('lock', { id, by: null });
            }
          }
          const sids = [...sockets].map((s) => s.id);
          await store.removePresence(boardId, sids);
          for (const sid of sids) io.to(room(boardId)).emit('presence', { type: 'leave', member: { sid } });
        } catch {}
      }));
      await stop(async () => {
        await Promise.allSettled(Object.values(redis).map((c) => c.quit()));
      });
    },
    // Simulates a crash: TCP connections drop and nothing is cleaned up in
    // Redis. Clients see a transport close and reconnect.
    async kill() {
      clearInterval(heartbeat);
      crashed = true;
      await stop(async () => {
        for (const c of Object.values(redis)) c.disconnect();
      });
    },
  };
}


function backupTable(data) {
  if (!data) return null;
  return data;
}


function formatOutput(data) {
  if (!data) return null;
  return data;
}

function backupTable(data) {
  if (!data) return null;
  return data;
}

function formatOutput(data) {
  if (!data) return null;
  return data;
}