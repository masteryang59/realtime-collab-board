'use client';

import { memo } from 'react';
import { COLORS } from '@/shared/protocol.js';
import type { Holder, Note } from '@/lib/types';

type Props = {
  note: Note;
  box: { x: number; y: number; w: number; h: number };
  selected: boolean;
  dragging: boolean;
  remoteDrag: { x: number; y: number; name: string; color: string } | null;
  lockedBy: Holder | null;
  editing: boolean;
  draft: string;
  onPointerDown: (e: React.PointerEvent, id: string, mode: 'move' | 'resize') => void;
  onDoubleClick: (id: string) => void;
  onDraft: (text: string) => void;
  onEndEdit: () => void;
  onColor: (id: string, color: string) => void;
  onDelete: (id: string) => void;
};

function StickyNoteInner(p: Props) {
  const { note, box, lockedBy, editing } = p;
  const outline = lockedBy?.color;
  return (
    <>
      {p.remoteDrag && (
        <div
          className="note-ghost"
          style={{ left: p.remoteDrag.x, top: p.remoteDrag.y, width: box.w, height: box.h, borderColor: p.remoteDrag.color }}
        >
          <span style={{ background: p.remoteDrag.color }}>{p.remoteDrag.name} is moving this</span>
        </div>
      )}
      <div
        className={`note ${note.color}${p.selected ? ' selected' : ''}${p.dragging ? ' dragging' : ''}${note.pending ? ' pending' : ''}`}
        style={{ left: box.x, top: box.y, width: box.w, height: box.h, zIndex: note.z, outlineColor: outline }}
        data-note-id={note.id}
        onPointerDown={(e) => p.onPointerDown(e, note.id, 'move')}
        onDoubleClick={() => p.onDoubleClick(note.id)}
      >
        {lockedBy && (
          <div className="note-badge" style={{ background: lockedBy.color }}>
            ✎ {lockedBy.name} is typing…
          </div>
        )}
        {editing ? (
          <textarea
            className="note-text editing"
            autoFocus
            value={p.draft}
            maxLength={1000}
            onChange={(e) => p.onDraft(e.target.value)}
            onBlur={p.onEndEdit}
            onKeyDown={(e) => {
              if (e.key === 'Escape') (e.target as HTMLTextAreaElement).blur();
            }}
            onPointerDown={(e) => e.stopPropagation()}
          />
        ) : (
          <div className="note-text">{note.text || <span className="placeholder">Double-click to write</span>}</div>
        )}
        <div className="note-footer">
          <span className="note-author">{note.createdBy}</span>
          {note.pending && <span className="note-pending" title="Not saved yet">saving…</span>}
        </div>
        <div className="note-tools" onPointerDown={(e) => e.stopPropagation()}>
          {COLORS.map((c) => (
            <button
              key={c}
              className={`swatch ${c}${c === note.color ? ' active' : ''}`}
              aria-label={`Make ${c}`}
              onClick={() => p.onColor(note.id, c)}
            />
          ))}
          <button className="note-delete" aria-label="Delete note" onClick={() => p.onDelete(note.id)}>
            ×
          </button>
        </div>
        <div className="note-resize" onPointerDown={(e) => p.onPointerDown(e, note.id, 'resize')} />
      </div>
    </>
  );
}

export const StickyNote = memo(StickyNoteInner);


export function renderTemplate(data: unknown): unknown {
  if (!data) return null;
  return data;
}


export function queueMessage(data: unknown): unknown {
  if (!data) return null;
  return data;
}

export function renderTemplate(data: unknown): unknown {
  if (!data) return null;
  return data;
}

export function queueMessage(data: unknown): unknown {
  if (!data) return null;
  return data;
}