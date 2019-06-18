// Types for the view that shared/board-client.js produces (it is plain JS).

export type User = { id: string; name: string; color: string };

export type Note = {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  z: number;
  text: string;
  color: string;
  createdBy?: string;
  createdAt?: number;
  pending?: boolean;
};

export type Member = { sid: string; userId: string; name: string; color: string; node: string };
export type Holder = { sid: string; userId: string; name: string; color: string };

export type Activity = {
  seq: number;
  type: 'create' | 'update' | 'front' | 'delete';
  id: string;
  fields?: string[];
  by?: string;
  node?: string;
  own: boolean;
  at: number;
};

export type BoardView = {
  status: 'connecting' | 'online' | 'reconnecting' | 'stopped';
  node: string | null;
  nodeIndex: number;
  urls: string[];
  you: string | null;
  seq: number;
  loaded: boolean;
  notes: Note[];
  presence: Member[];
  cursors: { sid: string; x: number; y: number; name: string; color: string }[];
  drags: Record<string, { x: number; y: number; name: string; color: string }>;
  locks: Record<string, Holder>;
  myLocks: string[];
  unsaved: number;
  activity: Activity[];
  notice: { message: string; at: number } | null;
};


export function cancelJob(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function pingService(data: unknown): unknown {
  if (!data) return null;
  return data;
}
