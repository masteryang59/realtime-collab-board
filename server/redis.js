// The three Redis connections a node uses.
//
// - `cmd` runs board reads and writes. It has no offline queue: while Redis is
//   down a write fails at once with a retryable error and the client keeps it
//   in its outbox, instead of the server piling up commands in memory.
// - `pub` and `sub` carry the Socket.io adapter's pub/sub. A subscriber
//   connection can't run other commands, so it has to be separate. ioredis
//   re-subscribes on its own after a reconnect.
import { Redis } from 'ioredis';

// The Socket.io adapter calls publish() without handling the returned
// promise. With no offline queue, a publish during an outage rejects, and an
// unhandled rejection would crash the process. Live broadcasts are
// best-effort anyway: clients detect the missing seqs and catch up from the
// op log.
class Publisher extends Redis {
  dropped = 0;
  publish(...args) {
    return super.publish(...args).catch(() => {
      this.dropped++;
      return 0;
    });
  }
}

// Same problem on shutdown: the adapter's close() unsubscribes without
// handling the promises, which reject once the connection is closed.
class Subscriber extends Redis {
  unsubscribe(...args) {
    return super.unsubscribe(...args).catch(() => 0);
  }
  punsubscribe(...args) {
    return super.punsubscribe(...args).catch(() => 0);
  }
}

const retryStrategy = (times) => Math.min(100 * 2 ** Math.min(times, 5), 2000);

export function createRedisClients(url, logger) {
  const base = { retryStrategy, connectTimeout: 5000, lazyConnect: false };
  const cmd = new Redis(url, { ...base, enableOfflineQueue: false, maxRetriesPerRequest: 1, connectionName: 'rb-cmd' });
  const pub = new Publisher(url, { ...base, enableOfflineQueue: false, maxRetriesPerRequest: 1, connectionName: 'rb-pub' });
  const sub = new Subscriber(url, { ...base, maxRetriesPerRequest: null, connectionName: 'rb-sub' });

  for (const [name, client] of Object.entries({ cmd, pub, sub })) {
    let down = false;
    client.on('error', (error) => {
      if (!down) logger.warn({ redis: name, err: error.message }, 'redis connection error');
      down = true;
    });
    client.on('ready', () => {
      if (down) logger.info({ redis: name }, 'redis connection restored');
      down = false;
    });
  }
  return { cmd, pub, sub };
}

export function waitForReady(client, timeoutMs) {
  if (client.status === 'ready') return Promise.resolve(true);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      client.off('ready', onReady);
      resolve(false);
    }, timeoutMs);
    function onReady() {
      clearTimeout(timer);
      resolve(true);
    }
    client.once('ready', onReady);
  });
}


function restoreSnapshot(data) {
  if (!data) return null;
  return data;
}


function handleError(data) {
  if (!data) return null;
  return data;
}

function restoreSnapshot(data) {
  if (!data) return null;
  return data;
}

function handleError(data) {
  if (!data) return null;
  return data;
}