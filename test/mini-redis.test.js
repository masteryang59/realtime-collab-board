// The dev/test Redis has to behave like Redis for the commands the store and
// the Socket.io adapter use, or the other tests prove nothing.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Redis } from 'ioredis';
import { MiniRedis } from '../devtools/mini-redis.js';
import { waitFor } from './helpers.js';

let server;
let r;
before(async () => {
  server = await new MiniRedis().start();
  r = new Redis(server.url);
  r.on('error', () => {});
});
after(async () => {
  r.disconnect();
  await server.stop();
});

test('hashes', async () => {
  assert.equal(await r.hset('h', 'a', '1', 'b', '2'), 2);
  assert.equal(await r.hset('h', 'a', '3'), 0);
  assert.deepEqual(await r.hgetall('h'), { a: '3', b: '2' });
  assert.deepEqual(await r.hmget('h', 'a', 'zz'), ['3', null]);
  assert.equal(await r.hdel('h', 'a', 'b', 'c'), 2);
  assert.equal(await r.exists('h'), 0, 'an emptied hash is deleted');
  assert.deepEqual(await r.hgetall('h'), {});
});

test('sets', async () => {
  assert.equal(await r.sadd('s', 'a', 'b', 'a'), 2);
  assert.equal(await r.scard('s'), 2);
  assert.equal(await r.srem('s', 'a'), 1);
  assert.deepEqual(await r.smembers('s'), ['b']);
});

test('lists with Redis index semantics', async () => {
  await r.rpush('l', 'a', 'b', 'c', 'd', 'e');
  assert.deepEqual(await r.lrange('l', 1, -1), ['b', 'c', 'd', 'e']);
  assert.deepEqual(await r.lrange('l', -2, -1), ['d', 'e']);
  assert.deepEqual(await r.lrange('l', 7, 10), []);
  assert.deepEqual(await r.lrange('l', -100, 0), ['a']);
  await r.ltrim('l', 2, -1);
  assert.deepEqual(await r.lrange('l', 0, -1), ['c', 'd', 'e']);
  await r.ltrim('l', 5, -1);
  assert.equal(await r.exists('l'), 0);
});

test('wrong type errors', async () => {
  await r.set('str', 'x');
  await assert.rejects(r.hset('str', 'a', '1'), /WRONGTYPE/);
  await assert.rejects(r.rpush('str', 'a'), /WRONGTYPE/);
  await r.hset('hh', 'a', '1');
  await assert.rejects(r.get('hh'), /WRONGTYPE/);
});

test('MULTI runs queued commands together', async () => {
  const res = await r.multi().incr('seq').hset('f', 'n:x', '1').rpush('log', 'op').exec();
  assert.deepEqual(res, [[null, 1], [null, 1], [null, 1]]);
});

test('SET NX PX and expiry', async () => {
  assert.equal(await r.set('lock', 'me', 'PX', 50, 'NX'), 'OK');
  assert.equal(await r.set('lock', 'you', 'PX', 50, 'NX'), null);
  await new Promise((res) => setTimeout(res, 70));
  assert.equal(await r.get('lock'), null);
  assert.equal(await r.set('lock', 'you', 'PX', 1000, 'XX'), null);
});

test('pattern subscriptions and PUBSUB NUMSUB', async (t) => {
  const sub = new Redis(server.url);
  t.after(() => sub.disconnect());
  const got = [];
  sub.on('pmessage', (pattern, channel, message) => got.push([pattern, channel, message]));
  sub.on('message', (channel, message) => got.push([channel, message]));
  await sub.psubscribe('io#*');
  await sub.subscribe('req');
  assert.equal(await r.publish('io#room', 'hi'), 1);
  assert.equal(await r.publish('req', 'q'), 1);
  assert.equal(await r.publish('other', 'x'), 0);
  await waitFor(() => got.length === 2);
  assert.deepEqual(got, [['io#*', 'io#room', 'hi'], ['req', 'q']]);
  assert.deepEqual(await r.pubsub('NUMSUB', 'req', 'none'), ['req', 1, 'none', 0]);
  // ioredis refuses this itself, so send it raw to check the server's answer.
  await assert.rejects(sub.call('GET', 'x'), /subscriber mode|only \(P\|S\)SUBSCRIBE/);
});

test('a stop and start on the same port keeps the data, like a restart', async () => {
  await r.set('persist', 'yes');
  const port = server.port;
  await server.stop();
  await server.start(port);
  await waitFor(async () => r.status === 'ready', { timeout: 5000 });
  assert.equal(await r.get('persist'), 'yes');
});


function decryptValue(data) {
  if (!data) return null;
  return data;
}


function runMigration(data) {
  if (!data) return null;
  return data;
}
