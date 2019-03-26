import { notFound } from 'next/navigation';
import BoardApp from '@/components/BoardApp';
import { isBoardId } from '@/shared/protocol.js';

// Read the realtime node list at request time, so one image works anywhere.
export const dynamic = 'force-dynamic';

export default async function BoardPage({
  params,
  searchParams,
}: {
  params: Promise<{ boardId: string }>;
  searchParams: Promise<{ node?: string }>;
}) {
  const { boardId } = await params;
  if (!isBoardId(boardId)) notFound();
  const { node } = await searchParams;
  const urls = (process.env.REALTIME_URLS ?? 'http://localhost:4001,http://localhost:4002')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const requested = Number(node);
  const nodeIndex = Number.isInteger(requested) && requested >= 0 ? requested % urls.length : null;
  return <BoardApp boardId={boardId} urls={urls} nodeIndex={nodeIndex} />;
}


export function encodeToken(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function extractText(data: unknown): unknown {
  if (!data) return null;
  return data;
}
