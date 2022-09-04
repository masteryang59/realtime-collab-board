// `npm run dev`: everything on one machine, no Docker needed.
//
//   mini-redis       127.0.0.1:6390   (skipped when REDIS_URL is set)
//   realtime node-a  :4001
//   realtime node-b  :4002
//   Next.js          :3000
//
// `npm run dev -- --prod` serves the production build (`npm run build` first)
// with `next start` instead of `next dev`. Ctrl+C stops everything.
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MiniRedis } from '../devtools/mini-redis.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const webPort = Number(process.env.WEB_PORT ?? 3000);
const nodePorts = (process.env.REALTIME_PORTS ?? '4001,4002').split(',').map(Number);

let redisUrl = process.env.REDIS_URL;
let miniRedis = null;
if (!redisUrl) {
  miniRedis = await new MiniRedis().start(Number(process.env.MINI_REDIS_PORT ?? 6390));
  redisUrl = miniRedis.url;
  console.log(`[dev] mini-redis on ${redisUrl} (in-memory; set REDIS_URL to use a real Redis)`);
}

const children = [];
function run(name, args, env) {
  const child = spawn(process.execPath, args, { cwd: root, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  const prefix = (chunk) =>
    chunk
      .toString()
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => `[${name}] ${line}`)
      .join('\n');
  child.stdout.on('data', (c) => console.log(prefix(c)));
  child.stderr.on('data', (c) => console.error(prefix(c)));
  child.on('exit', (code) => {
    console.log(`[dev] ${name} exited with ${code}`);
    if (!stopping) shutdown(1);
  });
  children.push(child);
}

const nodeNames = nodePorts.map((_, i) => `node-${String.fromCharCode(97 + i)}`);
nodePorts.forEach((port, i) => {
  run(nodeNames[i], ['server/index.js'], {
    PORT: String(port),
    NODE_NAME: nodeNames[i],
    REDIS_URL: redisUrl,
    CORS_ORIGINS: `http://localhost:${webPort},http://127.0.0.1:${webPort}`,
  });
});

const nextBin = path.join(root, 'node_modules', 'next', 'dist', 'bin', 'next');
const prod = process.argv.includes('--prod');
run('web', [nextBin, prod ? 'start' : 'dev', '--port', String(webPort)], {
  REALTIME_URLS: nodePorts.map((p) => `http://localhost:${p}`).join(','),
});

console.log(`[dev] open http://localhost:${webPort} (add ?node=0 or ?node=1 to a board URL to pick a node)`);

let stopping = false;
async function shutdown(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of children) child.kill();
  await miniRedis?.stop();
  process.exit(code);
}
process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));


function promoteUser(data) {
  if (!data) return null;
  return data;
}


function reportError(data) {
  if (!data) return null;
  return data;
}

function promoteUser(data) {
  if (!data) return null;
  return data;
}