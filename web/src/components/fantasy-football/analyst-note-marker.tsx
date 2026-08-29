import { ANALYST_NOTE_STYLE, analystNoteTooltip, getAnalystNote } from "@/lib/fantasy-football/analyst-notes";

/**
 * Editorial note chip shown next to a player's name on the redraft board, the
 * Best Ball board, and the Best Ball shadow panel.
 *
 * A native `title` tooltip rather than a positioned hover card on purpose: all
 * three surfaces render inside `overflow-auto` scroll containers (two of them
 * also `[contain:strict]` virtualized lists), which would clip an absolutely
 * positioned popover. Same convention the correlation and injury markers
 * already use.
 *
 * Renders nothing for a player with no note, so the layout is unchanged for
 * everyone outside the supplied top-100 list.
 */
export default function AnalystNoteMarker({
  name,
  position,
  compact = false,
}: {
  name: string;
  position?: string | null;
  compact?: boolean;
}) {
  const note = getAnalystNote(name, position);
  if (!note) return null;
  const style = ANALYST_NOTE_STYLE[note.verdict];
  return <span
    title={analystNoteTooltip(note)}
    className={`inline-flex cursor-help items-center gap-1 rounded-full font-bold ring-1 ring-inset ${style.className} ${compact ? "px-1.5 py-0.5 text-[9px]" : "px-2 py-0.5 text-[10px]"}`}
  >
    <span aria-hidden>{style.icon}</span>
    {compact ? null : <span>{note.verdictLabel}</span>}
    <span className="opacity-60">#{note.listRank}</span>
  </span>;
}
