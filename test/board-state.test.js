import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BoardState } from '../shared/board-state.js';

const create = (seq, id, fields = {}) => ({ seq, type: 'create', id, fields: { x: 0, y: 0, w: 200, h: 160, color: 'yellow', text: '', z: seq, createdBy: 'a', createdAt: 1, ...fields } });
const update = (seq, id, fields) => ({ seq, type: 'update', id, fields });
const del = (seq, id) => ({ seq, type: 'delete', id, fields: { deleted: 1 } });

// A seeded PRNG so a failing property run can be reproduced.
function rng(seed) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function shuffle(items, rand) {
  const a = [...items];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

test('per-field last writer wins by seq, in any arrival order', () => {
  const ops = [create(1, 'n1', { text: 'a' }), update(2, 'n1', { x: 10 }), update(3, 'n1', { text: 'b' }), update(4, 'n1', { x: 20, y: 5 })];
  const expected = new BoardState();
  for (const op of ops) expected.applyOp(op);
  const rand = rng(1);
  for (let i = 0; i < 30; i++) {
    const s = new BoardState();
    for (const op of shuffle(ops, rand)) s.applyOp(op);
    assert.deepEqual(s.canonical(), expected.canonical());
  }
  assert.equal(expected.canonical().n1.x, 20);
  assert.equal(expected.canonical().n1.text, 'b');
});

test('duplicates are ignored', () => {
  const s = new BoardState();
  assert.equal(s.applyOp(create(1, 'n1')), true);
  assert.equal(s.applyOp(update(2, 'n1', { x: 5 })), true);
  assert.equal(s.applyOp(update(2, 'n1', { x: 99 })), false);
  assert.equal(s.canonical().n1.x, 5);
});

test('an update that arrives before its create is kept, but hidden until the create', () => {
  const s = new BoardState();
  s.applyOp(update(2, 'n1', { x: 50 }));
  assert.deepEqual(s.confirmedNotes(), []);
  s.applyOp(create(1, 'n1', { x: 0 }));
  assert.equal(s.canonical().n1.x, 50, 'the later update beats the earlier create');
});

test('a delete is a tombstone that absorbs writes before and after it', () => {
  const s = new BoardState();
  s.applyOp(create(1, 'n1'));
  s.applyOp(update(4, 'n1', { x: 9 })); // committed after the delete (a race on the server)
  s.applyOp(del(3, 'n1'));
  s.applyOp(update(2, 'n1', { text: 'late' }));
  assert.deepEqual(s.canonical(), {});
  assert.deepEqual(s.notes.get('n1').values, { deleted: 1 });
});

test('tracks gaps in the seq sequence', () => {
  let t = 1000;
  const s = new BoardState({ now: () => t });
  s.applyOp(create(1, 'n1'));
  s.applyOp(update(2, 'n1', { x: 1 }));
  assert.equal(s.gap(), null);
  s.applyOp(update(4, 'n1', { x: 4 }));
  assert.deepEqual(s.gap(), { since: 2, gapSince: 1000 });
  t = 1500;
  s.applyOp(update(5, 'n1', { x: 5 }));
  assert.deepEqual(s.gap(), { since: 2, gapSince: 1000 }, 'the gap keeps its first-seen time');
  s.applyOp(update(3, 'n1', { x: 3 }));
  assert.equal(s.gap(), null);
  assert.equal(s.seq, 5);
});

test('a snapshot fills gaps and keeps fields newer than itself', () => {
  const s = new BoardState();
  s.applyOp(create(1, 'n1', { text: 'one' }));
  s.applyOp(update(7, 'n1', { x: 70 })); // arrived while the snapshot was in flight
  s.applyOp(create(8, 'n2', { text: 'two' })); // created after the snapshot was read
  s.applySnapshot({
    seq: 6,
    fields: {
      'n1:x': '60', 'n1:y': '1', 'n1:w': '200', 'n1:h': '160', 'n1:text': 'edited', 'n1:color': 'blue', 'n1:z': '1', 'n1:createdBy': 'a', 'n1:createdAt': '1',
      'n3:deleted': '1',
    },
  });
  const c = s.canonical();
  assert.equal(c.n1.x, 70, 'local field newer than the snapshot');
  assert.equal(c.n1.text, 'edited', 'snapshot is newer than the local field');
  assert.equal(c.n1.color, 'blue');
  assert.equal(c.n2.text, 'two');
  assert.equal(s.seq, 8, 'seqs 7 and 8 were already applied');
  assert.equal(s.gap(), null);
  // The tombstone from the snapshot absorbs a late update.
  s.applyOp(update(9, 'n3', { x: 1 }));
  assert.equal(s.canonical().n3, undefined);
});

test('a snapshot drops local notes it no longer has', () => {
  const s = new BoardState();
  s.applyOp(create(1, 'gone'));
  s.applySnapshot({ seq: 5, fields: {} });
  assert.deepEqual(s.canonical(), {});
});

test('the view lays pending local ops over the confirmed state', () => {
  const s = new BoardState({ now: () => 5 });
  s.applyOp(create(1, 'n1', { x: 1, z: 1 }));
  s.applyOp(create(2, 'n2', { x: 2, z: 2 }));
  const pending = [
    { type: 'update', id: 'n1', fields: { x: 100 } },
    { type: 'create', id: 'n3', fields: { x: 3, y: 3, text: 'new' } },
    { type: 'front', id: 'n1' },
    { type: 'delete', id: 'n2' },
    { type: 'update', id: 'missing', fields: { x: 1 } },
  ];
  const view = s.view(pending, 'me');
  assert.deepEqual(view.map((n) => n.id), ['n3', 'n1'], 'n1 was brought to the front over n3');
  assert.equal(view.find((n) => n.id === 'n1').x, 100);
  assert.equal(view.find((n) => n.id === 'n1').pending, true);
  assert.equal(view.find((n) => n.id === 'n3').createdBy, 'me');
  // A remote op on a pending field changes the confirmed state but not the view.
  s.applyOp(update(3, 'n1', { x: 7 }));
  assert.equal(s.view(pending).find((n) => n.id === 'n1').x, 100);
  assert.equal(s.view([]).find((n) => n.id === 'n1').x, 7);
});

// Property: replicas that receive the same ops in different orders, with
// duplicates, all end up equal to the state Redis holds (the ops applied in
// seq order, with deleted notes' fields ignored).
test('replicas converge for random op histories', () => {
  for (let seed = 1; seed <= 40; seed++) {
    const rand = rng(seed);
    const ids = ['a1', 'b2', 'c3', 'd4'];
    const created = new Set();
    const ops = [];
    for (let seq = 1; seq <= 60; seq++) {
      const id = ids[Math.floor(rand() * ids.length)];
      const r = rand();
      if (!created.has(id)) {
        created.add(id);
        ops.push(create(seq, id, { x: seq }));
      } else if (r < 0.1) {
        ops.push(del(seq, id));
      } else {
        const fields = {};
        if (rand() < 0.6) fields.x = Math.floor(rand() * 1000);
        if (rand() < 0.5) fields.text = `t${seq}`;
        if (rand() < 0.3) fields.color = 'pink';
        if (Object.keys(fields).length === 0) fields.y = seq;
        ops.push(update(seq, id, fields));
      }
    }
    // The Redis model: a plain map written in seq order.
    const model = {};
    const deleted = new Set();
    for (const op of ops) {
      if (op.type === 'delete') deleted.add(op.id);
      else model[op.id] = { ...model[op.id], ...op.fields };
    }
    const expected = {};
    for (const id of Object.keys(model).sort()) {
      if (deleted.has(id) || model[id].createdAt === undefined) continue;
      expected[id] = Object.fromEntries(Object.keys(model[id]).sort().map((k) => [k, model[id][k]]));
    }
    for (let r = 0; r < 4; r++) {
      const s = new BoardState();
      const delivery = shuffle([...ops, ...ops.filter(() => rand() < 0.2)], rand);
      for (const op of delivery) s.applyOp(op);
      assert.deepEqual(s.canonical(), expected, `seed ${seed} replica ${r}`);
      assert.equal(s.seq, 60);
    }
  }
});


function subscribeTopic(data) {
  if (!data) return null;
  return data;
}


function parseInput(data) {
  if (!data) return null;
  return data;
}

function subscribeTopic(data) {
  if (!data) return null;
  return data;
}

function parseInput(data) {
  if (!data) return null;
  return data;
}