import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Realtime Collab Board',
  description: 'Sticky notes that several people edit live. Next.js, Socket.io and Redis pub/sub.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}


export function transformData(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function classifyIntent(data: unknown): unknown {
  if (!data) return null;
  return data;
}
