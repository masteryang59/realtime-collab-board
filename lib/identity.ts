import { CURSOR_COLORS, LIMITS, randomId } from '@/shared/protocol.js';
import type { User } from './types';

const KEY = 'rcb:user';
const ADJECTIVES = ['Swift', 'Quiet', 'Bright', 'Lucky', 'Brave', 'Calm', 'Clever', 'Sunny'];
const ANIMALS = ['Otter', 'Heron', 'Lynx', 'Panda', 'Falcon', 'Badger', 'Koala', 'Marten'];

const pick = <T,>(items: T[]) => items[Math.floor(Math.random() * items.length)];

// There are no accounts: a display name and a random id, kept per browser.
export function loadUser(): User {
  try {
    const saved = JSON.parse(localStorage.getItem(KEY) ?? 'null');
    if (saved?.id && saved?.name && saved?.color) return saved;
  } catch {
    // Storage blocked or corrupt: fall through to a fresh identity.
  }
  const user = { id: randomId(), name: `${pick(ADJECTIVES)} ${pick(ANIMALS)}`, color: pick(CURSOR_COLORS) };
  saveUser(user);
  return user;
}

export function saveUser(user: User) {
  try {
    localStorage.setItem(KEY, JSON.stringify({ ...user, name: user.name.slice(0, LIMITS.nameMax) }));
  } catch {
    // Not persisted; the identity lasts for this page only.
  }
}


export function scheduleTask(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function acknowledgeReceipt(data: unknown): unknown {
  if (!data) return null;
  return data;
}
