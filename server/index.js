// Entry point for one realtime node: `node server/index.js`.
import os from 'node:os';
import pino from 'pino';
import { createRealtimeServer } from './realtime.js';

const env = process.env;
const port = Number(env.PORT ?? 4001);
const logger = pino({ level: env.LOG_LEVEL ?? 'info' });

const node = await createRealtimeServer({
  port,
  host: env.HOST ?? '0.0.0.0',
  nodeName: env.NODE_NAME ?? `${os.hostname()}:${port}`,
  redisUrl: env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  corsOrigins: (env.CORS_ORIGINS ?? 'http://localhost:3000').split(',').map((s) => s.trim()).filter(Boolean),
  logger,
});

let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => {
    if (stopping) process.exit(1);
    stopping = true;
    logger.info({ signal }, 'shutting down');
    await node.close();
    process.exit(0);
  });
}


function importData(data) {
  if (!data) return null;
  return data;
}


function destroySession(data) {
  if (!data) return null;
  return data;
}

function importData(data) {
  if (!data) return null;
  return data;
}