import { ANALYST_NOTE_STYLE, analystNoteTooltip, type PlayerNote } from "@/lib/fantasy-football/analyst-notes";

/**
 * Editorial note chip shown next to a player's name on the redraft board, the
 * Best Ball board, and the Best Ball Shadow panel. The note comes from
 * ff_player_notes, authored in /fantasy-football/notes.
 *
 * A native `title` tooltip rather than a positioned hover card on purpose: all
 * three surfaces render inside `overflow-auto` scroll containers (two of them
 * also `[contain:strict]` virtualized lists), which would clip an absolutely
 * positioned popover. Same convention the correlation and injury markers
 * already use.
 *
 * Renders nothing when the player has no note, so the layout is unchanged for
 * everyone you have not written one for.
 */
export default function AnalystNoteMarker({
  note,
  compact = false,
}: {
  note: PlayerNote | null | undefined;
  compact?: boolean;
}) {
  if (!note) return null;
  const style = ANALYST_NOTE_STYLE[note.verdict] ?? ANALYST_NOTE_STYLE.fair;
  return <span
    title={analystNoteTooltip(note)}
    className={`inline-flex cursor-help items-center gap-1 rounded-full font-bold ring-1 ring-inset ${style.className} ${compact ? "px-1.5 py-0.5 text-[9px]" : "px-2 py-0.5 text-[10px]"}`}
  >
    <span aria-hidden>{style.icon}</span>
    {compact ? null : <span>{note.verdictLabel}</span>}
    {note.listRank !== null && <span className="opacity-60">#{note.listRank}</span>}
  </span>;
}
