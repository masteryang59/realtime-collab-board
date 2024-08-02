// A small Redis-compatible server for local development and tests on machines
// without Redis or Docker. It speaks RESP2 over TCP, so the app and the
// Socket.io Redis adapter talk to it with the real ioredis client, exactly as
// they talk to Redis in production.
//
// It implements only the commands this project uses: strings, TTLs, counters,
// hashes, sets, lists, MULTI/EXEC, pub/sub with patterns and PUBSUB NUMSUB,
// plus what ioredis sends on connect. It is single-process and in-memory.
// stop() followed by start() on the same port keeps the data, which is how the
// tests simulate a Redis restart. Use real Redis anywhere else.
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const OK = '+OK\r\n';
const NIL = '$-1\r\n';
const WRONGTYPE = '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n';

function bulk(value) {
  if (value === null || value === undefined) return NIL;
  const buf = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
  return Buffer.concat([Buffer.from(`$${buf.length}\r\n`), buf, Buffer.from('\r\n')]);
}
const int = (n) => `:${n}\r\n`;
const err = (msg) => `-${msg}\r\n`;
function array(items) {
  if (items === null) return '*-1\r\n';
  const parts = [Buffer.from(`*${items.length}\r\n`)];
  for (const item of items) {
    const encoded = typeof item === 'number' ? int(item) : bulk(item);
    parts.push(Buffer.isBuffer(encoded) ? encoded : Buffer.from(encoded));
  }
  return Buffer.concat(parts);
}

// Parses as many complete commands as the buffer holds. Returns
// { commands, rest }, where rest is the unconsumed tail.
export function parseCommands(buffer) {
  const commands = [];
  let pos = 0;
  while (pos < buffer.length) {
    if (buffer[pos] !== 0x2a /* '*' */) {
      // Inline command (what telnet or `redis-cli` in some modes sends).
      const end = buffer.indexOf('\r\n', pos);
      if (end === -1) break;
      const line = buffer.subarray(pos, end).toString().trim();
      pos = end + 2;
      if (line) commands.push(line.split(/\s+/).map((s) => Buffer.from(s)));
      continue;
    }
    const headerEnd = buffer.indexOf('\r\n', pos);
    if (headerEnd === -1) break;
    const count = Number(buffer.subarray(pos + 1, headerEnd).toString());
    let cursor = headerEnd + 2;
    const args = [];
    let complete = true;
    for (let i = 0; i < count; i++) {
      if (cursor >= buffer.length || buffer[cursor] !== 0x24 /* '$' */) { complete = false; break; }
      const lenEnd = buffer.indexOf('\r\n', cursor);
      if (lenEnd === -1) { complete = false; break; }
      const len = Number(buffer.subarray(cursor + 1, lenEnd).toString());
      const start = lenEnd + 2;
      if (start + len + 2 > buffer.length) { complete = false; break; }
      args.push(buffer.subarray(start, start + len));
      cursor = start + len + 2;
    }
    if (!complete) break;
    commands.push(args);
    pos = cursor;
  }
  return { commands, rest: buffer.subarray(pos) };
}

function globToRegExp(glob) {
  let source = '';
  for (const ch of glob) {
    if (ch === '*') source += '.*';
    else if (ch === '?') source += '.';
    else source += ch.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${source}$`, 's');
}

// Redis list index semantics: negative counts from the end, then clamp.
function listRange(length, start, stop) {
  let s = start < 0 ? length + start : start;
  let e = stop < 0 ? length + stop : stop;
  if (s < 0) s = 0;
  if (e >= length) e = length - 1;
  return [s, e];
}

const SUBSCRIBE_MODE_COMMANDS = new Set(['SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE', 'PING', 'QUIT']);

export class MiniRedis {
  constructor() {
    this.data = new Map(); // key -> { type: 'string'|'hash'|'set'|'list', value }
    this.expires = new Map(); // key -> epoch ms
    this.channels = new Map(); // channel -> Set<Client>
    this.patterns = new Map(); // pattern -> { re, clients: Set<Client> }
    this.clients = new Set();
    this.server = null;
    this.commandCount = 0;
    this.commandStats = new Map(); // command name -> count
    this.sweeper = null;
  }

  async start(port = 0, host = '127.0.0.1') {
    this.server = net.createServer((socket) => this.#accept(socket));
    await new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(port, host, () => {
        this.server.off('error', reject);
        resolve();
      });
    });
    this.port = this.server.address().port;
    this.host = host;
    this.sweeper = setInterval(() => this.#sweep(), 1000);
    this.sweeper.unref();
    return this;
  }

  get url() {
    return `redis://${this.host}:${this.port}`;
  }

  // Closes the listener and drops every connection, like a crashed server.
  // The data stays, so start(samePort) behaves like a restart with persistence.
  async stop() {
    clearInterval(this.sweeper);
    for (const client of this.clients) client.socket.destroy();
    this.clients.clear();
    this.channels.clear();
    this.patterns.clear();
    if (this.server) await new Promise((resolve) => this.server.close(() => resolve()));
    this.server = null;
  }

  flush() {
    this.data.clear();
    this.expires.clear();
  }

  #accept(socket) {
    socket.setNoDelay(true);
    const client = { socket, buffer: Buffer.alloc(0), subscriptions: new Set(), psubscriptions: new Set(), multi: null };
    this.clients.add(client);
    socket.on('data', (chunk) => {
      client.buffer = client.buffer.length ? Buffer.concat([client.buffer, chunk]) : chunk;
      const { commands, rest } = parseCommands(client.buffer);
      client.buffer = Buffer.from(rest);
      for (const args of commands) this.#dispatch(client, args);
    });
    const drop = () => {
      this.clients.delete(client);
      for (const channel of client.subscriptions) this.channels.get(channel)?.delete(client);
      for (const pattern of client.psubscriptions) this.patterns.get(pattern)?.clients.delete(client);
    };
    socket.on('close', drop);
    socket.on('error', drop);
  }

  #write(client, reply) {
    if (!client.socket.destroyed) client.socket.write(reply);
  }

  #subCount(client) {
    return client.subscriptions.size + client.psubscriptions.size;
  }

  #dispatch(client, args) {
    if (args.length === 0) return;
    const name = args[0].toString().toUpperCase();
    this.commandCount++;
    this.commandStats.set(name, (this.commandStats.get(name) ?? 0) + 1);

    if (this.#subCount(client) > 0 && !SUBSCRIBE_MODE_COMMANDS.has(name)) {
      this.#write(client, err(`ERR Can't execute '${name.toLowerCase()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context`));
      return;
    }

    if (client.multi && !['EXEC', 'DISCARD', 'MULTI'].includes(name)) {
      client.multi.push(args);
      this.#write(client, '+QUEUED\r\n');
      return;
    }

    switch (name) {
      case 'MULTI':
        if (client.multi) return this.#write(client, err('ERR MULTI calls can not be nested'));
        client.multi = [];
        return this.#write(client, OK);
      case 'DISCARD':
        if (!client.multi) return this.#write(client, err('ERR DISCARD without MULTI'));
        client.multi = null;
        return this.#write(client, OK);
      case 'EXEC': {
        if (!client.multi) return this.#write(client, err('ERR EXEC without MULTI'));
        const queued = client.multi;
        client.multi = null;
        // Single-threaded and synchronous, so the queued commands run with
        // nothing interleaved, which is the guarantee MULTI/EXEC gives.
        const replies = queued.map((cmd) => this.#run(client, cmd[0].toString().toUpperCase(), cmd.slice(1)));
        const parts = [Buffer.from(`*${replies.length}\r\n`)];
        for (const r of replies) parts.push(Buffer.isBuffer(r) ? r : Buffer.from(r));
        return this.#write(client, Buffer.concat(parts));
      }
      case 'SUBSCRIBE':
        for (const raw of args.slice(1)) {
          const channel = raw.toString();
          client.subscriptions.add(channel);
          if (!this.channels.has(channel)) this.channels.set(channel, new Set());
          this.channels.get(channel).add(client);
          this.#write(client, array(['subscribe', channel, this.#subCount(client)]));
        }
        return;
      case 'UNSUBSCRIBE': {
        const targets = args.length > 1 ? args.slice(1).map(String) : [...client.subscriptions];
        if (targets.length === 0) return this.#write(client, array(['unsubscribe', null, this.#subCount(client)]));
        for (const channel of targets) {
          client.subscriptions.delete(channel);
          this.channels.get(channel)?.delete(client);
          this.#write(client, array(['unsubscribe', channel, this.#subCount(client)]));
        }
        return;
      }
      case 'PSUBSCRIBE':
        for (const raw of args.slice(1)) {
          const pattern = raw.toString();
          client.psubscriptions.add(pattern);
          if (!this.patterns.has(pattern)) this.patterns.set(pattern, { re: globToRegExp(pattern), clients: new Set() });
          this.patterns.get(pattern).clients.add(client);
          this.#write(client, array(['psubscribe', pattern, this.#subCount(client)]));
        }
        return;
      case 'PUNSUBSCRIBE': {
        const targets = args.length > 1 ? args.slice(1).map(String) : [...client.psubscriptions];
        if (targets.length === 0) return this.#write(client, array(['punsubscribe', null, this.#subCount(client)]));
        for (const pattern of targets) {
          client.psubscriptions.delete(pattern);
          this.patterns.get(pattern)?.clients.delete(client);
          this.#write(client, array(['punsubscribe', pattern, this.#subCount(client)]));
        }
        return;
      }
      case 'PING':
        if (this.#subCount(client) > 0) return this.#write(client, array(['pong', args[1] ?? '']));
        return this.#write(client, args[1] ? bulk(args[1]) : '+PONG\r\n');
      case 'QUIT':
        this.#write(client, OK);
        client.socket.end();
        return;
      default:
        return this.#write(client, this.#run(client, name, args.slice(1)));
    }
  }

  #alive(key) {
    const at = this.expires.get(key);
    if (at !== undefined && at <= Date.now()) {
      this.data.delete(key);
      this.expires.delete(key);
      return false;
    }
    return this.data.has(key);
  }

  #sweep() {
    const now = Date.now();
    for (const [key, at] of this.expires) {
      if (at <= now) {
        this.data.delete(key);
        this.expires.delete(key);
      }
    }
  }

  // Returns the entry if it exists and has the given type, null if the key is
  // missing, or false on a type mismatch.
  #entry(key, type) {
    if (!this.#alive(key)) return null;
    const entry = this.data.get(key);
    return entry.type === type ? entry : false;
  }

  // Like #entry but creates an empty value of that type when the key is missing.
  #entryForWrite(key, type) {
    const entry = this.#entry(key, type);
    if (entry !== null) return entry;
    const value = type === 'hash' ? new Map() : type === 'set' ? new Set() : [];
    const created = { type, value };
    this.data.set(key, created);
    return created;
  }

  // Redis deletes a collection when its last element is removed.
  #dropIfEmpty(key, entry) {
    const size = entry.type === 'list' ? entry.value.length : entry.value.size;
    if (size === 0) {
      this.data.delete(key);
      this.expires.delete(key);
    }
  }

  #get(key) {
    const entry = this.#entry(key, 'string');
    return entry ? entry.value : entry;
  }

  #setString(key, value) {
    this.data.set(key, { type: 'string', value: Buffer.from(value) });
  }

  #incrBy(key, by) {
    const current = this.#get(key);
    if (current === false) return WRONGTYPE;
    const n = current === null ? 0 : Number(current.toString());
    if (!Number.isInteger(n) || !Number.isInteger(by)) return err('ERR value is not an integer or out of range');
    const next = n + by;
    this.#setString(key, String(next));
    return int(next);
  }

  #expire(key, ms, flag) {
    if (!this.#alive(key)) return int(0);
    const has = this.expires.has(key);
    if (flag === 'NX' && has) return int(0);
    if (flag === 'XX' && !has) return int(0);
    this.expires.set(key, Date.now() + ms);
    return int(1);
  }

  #publish(channel, message) {
    let receivers = 0;
    const subscribers = this.channels.get(channel);
    if (subscribers?.size) {
      const encoded = array(['message', channel, message]);
      for (const sub of subscribers) this.#write(sub, encoded);
      receivers += subscribers.size;
    }
    for (const [pattern, { re, clients }] of this.patterns) {
      if (!clients.size || !re.test(channel)) continue;
      const encoded = array(['pmessage', pattern, channel, message]);
      for (const sub of clients) this.#write(sub, encoded);
      receivers += clients.size;
    }
    return receivers;
  }

  #run(client, name, a) {
    const s = (i) => a[i]?.toString();
    switch (name) {
      case 'ECHO':
        return bulk(a[0]);
      case 'INFO':
        return bulk('# Server\r\nredis_version:7.4.0\r\nredis_mode:standalone\r\nmini_redis:1\r\n# Persistence\r\nloading:0\r\n');
      case 'CLIENT':
      case 'SELECT':
        return OK;
      case 'TYPE':
        return this.#alive(s(0)) ? `+${this.data.get(s(0)).type}\r\n` : '+none\r\n';

      // Strings and keys
      case 'GET': {
        const v = this.#get(s(0));
        return v === false ? WRONGTYPE : bulk(v);
      }
      case 'MGET':
        return array(a.map((k) => this.#get(k.toString()) || null));
      case 'SET': {
        const key = s(0);
        let ttl = null;
        let nx = false;
        let xx = false;
        let get = false;
        let keepTtl = false;
        for (let i = 2; i < a.length; i++) {
          const opt = s(i).toUpperCase();
          if (opt === 'EX') ttl = Number(s(++i)) * 1000;
          else if (opt === 'PX') ttl = Number(s(++i));
          else if (opt === 'NX') nx = true;
          else if (opt === 'XX') xx = true;
          else if (opt === 'GET') get = true;
          else if (opt === 'KEEPTTL') keepTtl = true;
          else return err('ERR syntax error');
        }
        if (ttl !== null && !(ttl > 0)) return err("ERR invalid expire time in 'set' command");
        const previous = this.#get(key);
        if (get && previous === false) return WRONGTYPE;
        const exists = this.#alive(key);
        if ((nx && exists) || (xx && !exists)) return get ? bulk(previous) : NIL;
        this.#setString(key, a[1]);
        if (ttl !== null) this.expires.set(key, Date.now() + ttl);
        else if (!keepTtl) this.expires.delete(key);
        return get ? bulk(previous || null) : OK;
      }
      case 'DEL':
      case 'UNLINK': {
        let removed = 0;
        for (const k of a) {
          const key = k.toString();
          if (this.#alive(key)) removed++;
          this.data.delete(key);
          this.expires.delete(key);
        }
        return int(removed);
      }
      case 'EXISTS':
        return int(a.filter((k) => this.#alive(k.toString())).length);
      case 'INCR':
        return this.#incrBy(s(0), 1);
      case 'DECR':
        return this.#incrBy(s(0), -1);
      case 'INCRBY':
        return this.#incrBy(s(0), Number(s(1)));
      case 'EXPIRE':
        return this.#expire(s(0), Number(s(1)) * 1000, s(2)?.toUpperCase());
      case 'PEXPIRE':
        return this.#expire(s(0), Number(s(1)), s(2)?.toUpperCase());
      case 'TTL':
      case 'PTTL': {
        const key = s(0);
        if (!this.#alive(key)) return int(-2);
        const at = this.expires.get(key);
        if (at === undefined) return int(-1);
        const ms = at - Date.now();
        return int(name === 'TTL' ? Math.ceil(ms / 1000) : ms);
      }
      case 'KEYS': {
        const re = globToRegExp(s(0));
        return array([...this.data.keys()].filter((k) => this.#alive(k) && re.test(k)));
      }
      case 'DBSIZE':
        return int([...this.data.keys()].filter((k) => this.#alive(k)).length);
      case 'FLUSHALL':
      case 'FLUSHDB':
        this.flush();
        return OK;

      // Hashes
      case 'HSET': {
        if (a.length < 3 || a.length % 2 !== 1) return err("ERR wrong number of arguments for 'hset' command");
        const entry = this.#entryForWrite(s(0), 'hash');
        if (!entry) return WRONGTYPE;
        let added = 0;
        for (let i = 1; i < a.length; i += 2) {
          const field = a[i].toString();
          if (!entry.value.has(field)) added++;
          entry.value.set(field, Buffer.from(a[i + 1]));
        }
        return int(added);
      }
      case 'HGET': {
        const entry = this.#entry(s(0), 'hash');
        if (entry === false) return WRONGTYPE;
        return bulk(entry ? entry.value.get(s(1)) : null);
      }
      case 'HMGET': {
        const entry = this.#entry(s(0), 'hash');
        if (entry === false) return WRONGTYPE;
        return array(a.slice(1).map((f) => (entry ? entry.value.get(f.toString()) ?? null : null)));
      }
      case 'HGETALL': {
        const entry = this.#entry(s(0), 'hash');
        if (entry === false) return WRONGTYPE;
        const flat = [];
        if (entry) for (const [f, v] of entry.value) flat.push(f, v);
        return array(flat);
      }
      case 'HDEL': {
        const key = s(0);
        const entry = this.#entry(key, 'hash');
        if (entry === false) return WRONGTYPE;
        if (!entry) return int(0);
        let removed = 0;
        for (const f of a.slice(1)) if (entry.value.delete(f.toString())) removed++;
        this.#dropIfEmpty(key, entry);
        return int(removed);
      }
      case 'HLEN': {
        const entry = this.#entry(s(0), 'hash');
        if (entry === false) return WRONGTYPE;
        return int(entry ? entry.value.size : 0);
      }
      case 'HEXISTS': {
        const entry = this.#entry(s(0), 'hash');
        if (entry === false) return WRONGTYPE;
        return int(entry && entry.value.has(s(1)) ? 1 : 0);
      }

      // Sets
      case 'SADD': {
        const entry = this.#entryForWrite(s(0), 'set');
        if (!entry) return WRONGTYPE;
        let added = 0;
        for (const m of a.slice(1)) {
          const member = m.toString();
          if (!entry.value.has(member)) { entry.value.add(member); added++; }
        }
        return int(added);
      }
      case 'SREM': {
        const key = s(0);
        const entry = this.#entry(key, 'set');
        if (entry === false) return WRONGTYPE;
        if (!entry) return int(0);
        let removed = 0;
        for (const m of a.slice(1)) if (entry.value.delete(m.toString())) removed++;
        this.#dropIfEmpty(key, entry);
        return int(removed);
      }
      case 'SMEMBERS': {
        const entry = this.#entry(s(0), 'set');
        if (entry === false) return WRONGTYPE;
        return array(entry ? [...entry.value] : []);
      }
      case 'SCARD': {
        const entry = this.#entry(s(0), 'set');
        if (entry === false) return WRONGTYPE;
        return int(entry ? entry.value.size : 0);
      }
      case 'SISMEMBER': {
        const entry = this.#entry(s(0), 'set');
        if (entry === false) return WRONGTYPE;
        return int(entry && entry.value.has(s(1)) ? 1 : 0);
      }

      // Lists
      case 'RPUSH': {
        const entry = this.#entryForWrite(s(0), 'list');
        if (!entry) return WRONGTYPE;
        for (const v of a.slice(1)) entry.value.push(Buffer.from(v));
        return int(entry.value.length);
      }
      case 'LLEN': {
        const entry = this.#entry(s(0), 'list');
        if (entry === false) return WRONGTYPE;
        return int(entry ? entry.value.length : 0);
      }
      case 'LRANGE': {
        const entry = this.#entry(s(0), 'list');
        if (entry === false) return WRONGTYPE;
        if (!entry) return array([]);
        const [start, stop] = listRange(entry.value.length, Number(s(1)), Number(s(2)));
        return array(start > stop ? [] : entry.value.slice(start, stop + 1));
      }
      case 'LTRIM': {
        const key = s(0);
        const entry = this.#entry(key, 'list');
        if (entry === false) return WRONGTYPE;
        if (!entry) return OK;
        const [start, stop] = listRange(entry.value.length, Number(s(1)), Number(s(2)));
        entry.value = start > stop ? [] : entry.value.slice(start, stop + 1);
        this.#dropIfEmpty(key, entry);
        return OK;
      }

      // Pub/sub
      case 'PUBLISH':
        return int(this.#publish(s(0), a[1]));
      case 'PUBSUB': {
        const sub = s(0)?.toUpperCase();
        if (sub === 'NUMSUB') {
          const flat = [];
          for (const ch of a.slice(1).map(String)) flat.push(ch, this.channels.get(ch)?.size ?? 0);
          return array(flat);
        }
        if (sub === 'NUMPAT') {
          let n = 0;
          for (const { clients } of this.patterns.values()) if (clients.size) n++;
          return int(n);
        }
        if (sub === 'CHANNELS') {
          const re = a[1] ? globToRegExp(s(1)) : null;
          return array([...this.channels].filter(([ch, subs]) => subs.size && (!re || re.test(ch))).map(([ch]) => ch));
        }
        return err(`ERR unknown subcommand '${s(0)}'`);
      }
      default:
        return err(`ERR unknown command '${name.toLowerCase()}'`);
    }
  }
}

// `node devtools/mini-redis.js [port]` runs it standalone.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.argv[2] ?? process.env.MINI_REDIS_PORT ?? 6379);
  const server = await new MiniRedis().start(port);
  console.log(`mini-redis listening on ${server.url}`);
}


function generateReport(data) {
  if (!data) return null;
  return data;
}


function dequeueBatch(data) {
  if (!data) return null;
  return data;
}

function generateReport(data) {
  if (!data) return null;
  return data;
}

function dequeueBatch(data) {
  if (!data) return null;
  return data;
}