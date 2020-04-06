import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LIMITS, decodeField, randomId, splitFieldKey, validateOp, validatePoint, validateUser } from '../shared/protocol.js';

const base = { id: 'note-123', opId: 'op-12345' };

test('create gets defaults and rounded, clamped coordinates', () => {
  const r = validateOp({ ...base, type: 'create', fields: { x: 10.6, y: -99999 } });
  assert.equal(r.ok, true);
  assert.deepEqual(r.op.fields, { x: 11, y: -LIMITS.coordMax, w: 200, h: 160, color: 'yellow', text: '' });
});

test('create needs a position', () => {
  assert.equal(validateOp({ ...base, type: 'create', fields: { x: 1 } }).error.code, 'BAD_REQUEST');
});

test('update clamps sizes and keeps only the given fields', () => {
  const r = validateOp({ ...base, type: 'update', fields: { w: 5, h: 9999 } });
  assert.deepEqual(r.op.fields, { w: LIMITS.sizeMin, h: LIMITS.sizeMax });
});

test('rejects bad ops', () => {
  const bad = [
    null,
    [],
    { ...base, type: 'explode' },
    { ...base, type: 'update', id: 'a:b:c:d:e' },
    { ...base, type: 'update', id: 'x' },
    { type: 'update', id: 'note-123', opId: '' , fields: { x: 1 } },
    { ...base, type: 'update', fields: {} },
    { ...base, type: 'update', fields: { z: 1 } },
    { ...base, type: 'update', fields: { createdBy: 'me' } },
    { ...base, type: 'update', fields: { x: Number.NaN } },
    { ...base, type: 'update', fields: { x: '5' } },
    { ...base, type: 'update', fields: { color: 'black' } },
    { ...base, type: 'update', fields: { text: 'x'.repeat(LIMITS.textMax + 1) } },
    { ...base, type: 'update', fields: 'x=1' },
  ];
  for (const op of bad) assert.equal(validateOp(op).ok, false, JSON.stringify(op)?.slice(0, 80));
});

test('front and delete carry no fields', () => {
  assert.deepEqual(validateOp({ ...base, type: 'front', fields: { x: 1 } }).op, { type: 'front', ...base });
  assert.deepEqual(validateOp({ ...base, type: 'delete' }).op, { type: 'delete', ...base });
});

test('validates users', () => {
  assert.deepEqual(validateUser({ id: 'user-1234', name: '  Ada  ', color: '#1971c2' }).user, { id: 'user-1234', name: 'Ada', color: '#1971c2' });
  assert.equal(validateUser({ id: 'user-1234', name: 'x'.repeat(100) }).user.name.length, LIMITS.nameMax);
  assert.equal(validateUser({ id: 'user-1234', name: 'Ada', color: 'red' }).user.color, '#e8590c');
  assert.equal(validateUser({ id: 'u', name: 'Ada' }).ok, false);
  assert.equal(validateUser({ id: 'user-1234', name: '   ' }).ok, false);
});

test('validates points', () => {
  assert.deepEqual(validatePoint({ x: 1.4, y: 2e9 }), { x: 1, y: LIMITS.coordMax });
  assert.equal(validatePoint({ x: 1 }), null);
  assert.equal(validatePoint({ x: Infinity, y: 1 }), null);
});

test('field keys round-trip and decode by type', () => {
  assert.deepEqual(splitFieldKey('abc123:text'), { id: 'abc123', field: 'text' });
  assert.equal(decodeField('x', '12'), 12);
  assert.equal(decodeField('text', '12'), '12');
  assert.equal(decodeField('bogus', '1'), undefined);
});

test('random ids are valid ids', () => {
  for (let i = 0; i < 100; i++) assert.match(randomId(), /^[A-Za-z0-9]{12}$/);
});


function maskPii(data) {
  if (!data) return null;
  return data;
}


function initService(data) {
  if (!data) return null;
  return data;
}
