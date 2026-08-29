// Hand-written per-player scouting notes.
//
// EDITORIAL commentary, not model output. A note renders as a chip next to the
// player's name (redraft board, Best Ball board, Best Ball Shadow panel) with
// the full text in the tooltip. It never touches projections, VOR, rank, ADP,
// or draft order -- the same read-only boundary this repo already applies to
// FantasyPros projections and DraftKings ADP (see CLAUDE.md). Board ordering is
// identical whether or not a player carries a note.
//
// The `ff_player_notes` table is the single source of truth, authored in the
// /fantasy-football/notes admin page. This module holds only the shared types,
// presentation, and the name normalization the seed/admin lookups use; the
// original hardcoded 100 notes now live in scripts/seed-analyst-notes.ts as a
// one-time migration input.

export type AnalystVerdict = "target" | "fair" | "caution" | "fade";

export const ANALYST_VERDICTS: AnalystVerdict[] = ["target", "fair", "caution", "fade"];

export function isAnalystVerdict(value: unknown): value is AnalystVerdict {
  return typeof value === "string" && (ANALYST_VERDICTS as string[]).includes(value);
}

/**
 * Note categories. A player carries at most one note per category, so two
 * different ranking exercises can disagree about the same player without one
 * overwriting the other -- e.g. Jameson Williams is "Caution" on the draft
 * board and "Volatile" on the PPR-consistency list, and both are true of
 * different questions.
 *
 * Adding a list: append an entry here, then seed it. Nothing else needs to
 * change -- the query, the chips, and the admin page are all driven off this.
 */
export const NOTE_CATEGORIES = [
  { id: "draft-board", label: "Draft Board", blurb: "Value against ADP" },
  { id: "ppr-consistency", label: "PPR Consistency", blurb: "Weekly floor / reception volume" },
] as const;

export type NoteCategory = (typeof NOTE_CATEGORIES)[number]["id"];

export const DEFAULT_NOTE_CATEGORY: NoteCategory = "draft-board";

export function isNoteCategory(value: unknown): value is NoteCategory {
  return typeof value === "string" && NOTE_CATEGORIES.some((category) => category.id === value);
}

export function noteCategoryLabel(id: string): string {
  return NOTE_CATEGORIES.find((category) => category.id === id)?.label ?? id;
}

/** A note as stored and as handed to the display layer. */
export type PlayerNote = {
  playerId: number;
  category: NoteCategory;
  verdict: AnalystVerdict;
  /** Free text, e.g. "Target", "Strong target", "Fade at this price". */
  verdictLabel: string;
  note: string;
  updatedAt: string | null;
  /** Position in the original supplied list. Null for notes written by hand. */
  listRank: number | null;
  /** Team/ADP as written in the source list. Display only -- never used to match. */
  sourceTeam: string | null;
  sourceAdp: number | null;
};

export const ANALYST_NOTE_STYLE: Record<AnalystVerdict, { icon: string; label: string; className: string }> = {
  target: { icon: "\u{1F7E2}", label: "Target", className: "bg-emerald-100 text-emerald-900 ring-emerald-300" },
  fair: { icon: "⚪", label: "Fair", className: "bg-slate-100 text-slate-700 ring-slate-300" },
  caution: { icon: "\u{1F7E1}", label: "Caution", className: "bg-amber-100 text-amber-900 ring-amber-300" },
  fade: { icon: "\u{1F534}", label: "Fade", className: "bg-rose-100 text-rose-900 ring-rose-300" },
};

/**
 * Mirrors ingest/ff_fantasypros.py::normalize_name so a note keyed on
 * "Kenneth Walker III" still finds the board's "Kenneth Walker".
 */
export function normalizeAnalystName(value: string): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/\p{M}/gu, "")
    .replace(/\b(jr|sr|ii|iii|iv)\.?\b/gi, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

/** Full tooltip text: verdict, provenance when present, then the note itself. */
export function analystNoteTooltip(note: PlayerNote): string {
  const style = ANALYST_NOTE_STYLE[note.verdict];
  const provenance: string[] = [];
  if (note.listRank !== null) provenance.push(`#${note.listRank} on the analyst board`);
  if (note.sourceTeam) provenance.push(note.sourceTeam);
  if (note.sourceAdp !== null) provenance.push(`listed ADP ${note.sourceAdp}`);

  const heading = provenance.length
    ? `${style.icon} ${noteCategoryLabel(note.category)}: ${note.verdictLabel} — ${provenance.join(" · ")}`
    : `${style.icon} ${noteCategoryLabel(note.category)}: ${note.verdictLabel}`;

  const footer = note.updatedAt
    ? `Your note, last edited ${new Date(note.updatedAt).toLocaleDateString()} — it does not change our projection, rank, or ADP.`
    : "Your note — it does not change our projection, rank, or ADP.";

  return [heading, "", note.note, "", footer].join("\n");
}

/** Trim + validate an admin-submitted note. Returns an error string, or null. */
export const MAX_NOTE_LENGTH = 2000;
export const MAX_VERDICT_LABEL_LENGTH = 40;

export function validateNoteInput(note: string, verdictLabel: string): string | null {
  if (!note.trim()) return "Write something in the note before saving.";
  if (note.length > MAX_NOTE_LENGTH) return `Note is ${note.length} characters; the limit is ${MAX_NOTE_LENGTH}.`;
  if (verdictLabel.length > MAX_VERDICT_LABEL_LENGTH) {
    return `Verdict label is ${verdictLabel.length} characters; the limit is ${MAX_VERDICT_LABEL_LENGTH}.`;
  }
  return null;
}
