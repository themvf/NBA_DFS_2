import {
  ANALYST_NOTE_STYLE,
  NOTE_CATEGORIES,
  analystNoteTooltip,
  type PlayerNote,
} from "@/lib/fantasy-football/analyst-notes";

/**
 * Editorial note chips shown next to a player's name on the redraft board, the
 * Best Ball board, and the Best Ball Shadow panel. Notes come from
 * ff_player_notes, authored in /fantasy-football/notes.
 *
 * One chip per category, so two ranking exercises that disagree about the same
 * player both stay visible -- collapsing them to a single chip would have to
 * pick one verdict and one color, which is the information we most want.
 *
 * A native `title` tooltip rather than a positioned hover card on purpose: all
 * three surfaces render inside `overflow-auto` scroll containers (two of them
 * also `[contain:strict]` virtualized lists), which would clip an absolutely
 * positioned popover. Same convention the correlation and injury markers
 * already use.
 *
 * Renders nothing when the player has no notes, so the layout is unchanged for
 * everyone you have not written one for.
 */
export default function AnalystNoteMarker({
  notes,
  compact = false,
}: {
  notes: PlayerNote[] | null | undefined;
  compact?: boolean;
}) {
  if (!notes?.length) return null;
  // Stable display order regardless of what the query returned.
  const ordered = [...notes].sort(
    (a, b) => categoryOrder(a.category) - categoryOrder(b.category),
  );
  return <span className="flex flex-wrap gap-1">
    {ordered.map((note) => {
      const style = ANALYST_NOTE_STYLE[note.verdict] ?? ANALYST_NOTE_STYLE.fair;
      return <span
        key={note.category}
        title={analystNoteTooltip(note)}
        className={`inline-flex cursor-help items-center gap-1 rounded-full font-bold ring-1 ring-inset ${style.className} ${compact ? "px-1.5 py-0.5 text-[9px]" : "px-2 py-0.5 text-[10px]"}`}
      >
        <span aria-hidden>{style.icon}</span>
        {compact ? null : <span>{note.verdictLabel}</span>}
        {note.listRank !== null && <span className="opacity-60">#{note.listRank}</span>}
      </span>;
    })}
  </span>;
}

function categoryOrder(id: string): number {
  const index = NOTE_CATEGORIES.findIndex((category) => category.id === id);
  return index === -1 ? NOTE_CATEGORIES.length : index;
}
