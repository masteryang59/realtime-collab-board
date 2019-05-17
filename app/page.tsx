import { redirect } from 'next/navigation';
import { randomId } from '@/shared/protocol.js';

export default function Home() {
  async function newBoard() {
    'use server';
    redirect(`/b/${randomId(10)}`);
  }

  return (
    <main className="landing">
      <div className="landing-card">
        <h1>Realtime Collab Board</h1>
        <p>
          Sticky notes that several people edit at the same time. Every change goes through a Socket.io node, is stored
          in Redis, and reaches everyone else through Redis pub/sub, whichever node they are connected to.
        </p>
        <form action={newBoard}>
          <button className="primary" type="submit">
            Create a board
          </button>
        </form>
        <p className="hint">
          Share the board link to collaborate. Open it in a second window with <code>?node=1</code> to connect that
          window to the other realtime node.
        </p>
      </div>
    </main>
  );
}


export function normalizeText(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function throttleRequests(data: unknown): unknown {
  if (!data) return null;
  return data;
}
