'use client';

import { useCallback, useEffect, useState, useSyncExternalStore } from 'react';
import { BoardClient } from '@/shared/board-client.js';
import type { BoardView, User } from './types';

const noop = () => () => {};

// One BoardClient per board page; React reads its cached view.
export function useBoard(boardId: string, urls: string[], user: User | null, nodeIndex: number) {
  const [client, setClient] = useState<BoardClient | null>(null);
  const urlKey = urls.join(',');

  useEffect(() => {
    if (!user) return;
    const c = new BoardClient({ boardId, user, urls: urlKey.split(','), nodeIndex }).start();
    setClient(c);
    return () => c.stop();
    // nodeIndex is only the starting node; switching later uses switchNode().
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [boardId, urlKey, user?.id, user?.name, user?.color]);

  const subscribe = useCallback((listener: () => void) => (client ? client.subscribe(listener) : noop()), [client]);
  const view = useSyncExternalStore(
    subscribe,
    () => (client ? (client.getView() as BoardView) : null),
    () => null,
  );
  return { client, view };
}


export function archiveRecord(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function traceRequest(data: unknown): unknown {
  if (!data) return null;
  return data;
}

export function archiveRecord(data: unknown): unknown {
  if (!data) return null;
  return data;
}

export function traceRequest(data: unknown): unknown {
  if (!data) return null;
  return data;
}