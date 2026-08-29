"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { NoteEditorPlayer } from "@/db/queries-fantasy-football";
import {
  ANALYST_NOTE_STYLE,
  ANALYST_VERDICTS,
  DEFAULT_NOTE_CATEGORY,
  MAX_NOTE_LENGTH,
  MAX_VERDICT_LABEL_LENGTH,
  NOTE_CATEGORIES,
  noteCategoryLabel,
  type AnalystVerdict,
  type NoteCategory,
} from "@/lib/fantasy-football/analyst-notes";
import AnalystNoteMarker from "@/components/fantasy-football/analyst-note-marker";
import { deleteFantasyPlayerNote, saveFantasyPlayerNote } from "../actions";

const POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"];

type Filter = "all" | "with-note" | "without-note";

export default function NotesEditor({ players }: { players: NoteEditorPlayer[] }) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  const [search, setSearch] = useState("");
  const [position, setPosition] = useState("");
  const [filter, setFilter] = useState<Filter>("all");

  const [selectedId, setSelectedId] = useState<number | null>(null);
  // Which list is being edited. A player carries one note per category, so this
  // decides both which existing note loads and which one a save writes.
  const [category, setCategory] = useState<NoteCategory>(DEFAULT_NOTE_CATEGORY);
  const [verdict, setVerdict] = useState<AnalystVerdict>("fair");
  const [verdictLabel, setVerdictLabel] = useState("");
  const [note, setNote] = useState("");
  const [message, setMessage] = useState<{ tone: "ok" | "error"; text: string } | null>(null);

  const selected = useMemo(
    () => players.find((player) => player.playerId === selectedId) ?? null,
    [players, selectedId],
  );

  // The note for the list currently being edited, if one exists.
  const selectedNote = selected?.notes.find((row) => row.category === category) ?? null;

  const visible = useMemo(() => {
    const term = search.trim().toLocaleLowerCase();
    return players.filter((player) => {
      const note = player.notes.find((row) => row.category === category) ?? null;
      return (!term || player.name.toLocaleLowerCase().includes(term))
        && (!position || player.position === position)
        && (filter === "all" || (filter === "with-note" ? Boolean(note) : !note));
    });
  }, [players, search, position, filter, category]);

  // Loading a player replaces the form wholesale. A blank verdict label falls
  // back to the verdict's own name at save time, so it is fine to leave empty.
  const select = (player: NoteEditorPlayer, forCategory: NoteCategory = category) => {
    const existing = player.notes.find((row) => row.category === forCategory) ?? null;
    setSelectedId(player.playerId);
    setVerdict(existing?.verdict ?? "fair");
    setVerdictLabel(existing?.verdictLabel ?? "");
    setNote(existing?.note ?? "");
    setMessage(null);
  };

  // Switching lists reloads the form from that list's note for the same player,
  // so you never save one list's text into another.
  const switchCategory = (next: NoteCategory) => {
    setCategory(next);
    if (selected) {
      const existing = selected.notes.find((row) => row.category === next) ?? null;
      setVerdict(existing?.verdict ?? "fair");
      setVerdictLabel(existing?.verdictLabel ?? "");
      setNote(existing?.note ?? "");
    }
    setMessage(null);
  };

  const save = () => {
    if (!selected) return;
    setMessage(null);
    startTransition(async () => {
      const result = await saveFantasyPlayerNote({
        playerId: selected.playerId,
        category,
        verdict,
        verdictLabel,
        note,
      });
      if (result.ok) {
        setMessage({ tone: "ok", text: `Saved ${selected.name} (${noteCategoryLabel(category)}). It is live on the boards now.` });
        router.refresh();
      } else {
        setMessage({ tone: "error", text: result.error ?? "Save failed." });
      }
    });
  };

  const remove = () => {
    if (!selectedNote) return;
    setMessage(null);
    startTransition(async () => {
      const result = await deleteFantasyPlayerNote({ playerId: selected!.playerId, category });
      if (result.ok) {
        setNote("");
        setVerdictLabel("");
        setVerdict("fair");
        setMessage({ tone: "ok", text: `Deleted the ${noteCategoryLabel(category)} note for ${selected!.name}.` });
        router.refresh();
      } else {
        setMessage({ tone: "error", text: result.error ?? "Delete failed." });
      }
    });
  };

  // A preview built from the unsaved form, so you can see the chip and tooltip
  // exactly as the boards will render them before committing.
  const preview = selected
    ? {
        playerId: selected.playerId,
        verdict,
        verdictLabel: verdictLabel.trim() || ANALYST_NOTE_STYLE[verdict].label,
        note: note.trim() || "(nothing written yet)",
        updatedAt: null,
        category,
        listRank: selectedNote?.listRank ?? null,
        sourceTeam: selectedNote?.sourceTeam ?? null,
        sourceAdp: selectedNote?.sourceAdp ?? null,
      }
    : null;

  return <div className="grid gap-4 lg:grid-cols-[minmax(320px,420px)_1fr] lg:items-start">
    <section className="space-y-3 rounded-2xl border bg-card p-4">
      <div className="space-y-2">
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search player"
          className="w-full rounded-lg border bg-background px-3 py-2 text-sm"
        />
        <div className="flex gap-2">
          <select value={position} onChange={(event) => setPosition(event.target.value)} className="flex-1 rounded-lg border bg-background px-2 py-2 text-sm">
            <option value="">All positions</option>
            {POSITIONS.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
          <select value={filter} onChange={(event) => setFilter(event.target.value as Filter)} className="flex-1 rounded-lg border bg-background px-2 py-2 text-sm">
            <option value="all">All players</option>
            <option value="with-note">Has a note</option>
            <option value="without-note">No note yet</option>
          </select>
        </div>
      </div>

      <p className="text-xs text-muted-foreground">{visible.length} shown</p>

      <div className="max-h-[62vh] divide-y overflow-auto rounded-xl border">
        {visible.slice(0, 400).map((player) => <button
          key={player.playerId}
          type="button"
          onClick={() => select(player)}
          className={`flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-sm hover:bg-muted ${player.playerId === selectedId ? "bg-violet-50" : ""}`}
        >
          <span className="min-w-0">
            <span className="block truncate font-bold">{player.name}</span>
            <span className="block text-xs text-muted-foreground">
              {player.position} · {player.team ?? "FA"}{player.ourRank !== null ? ` · #${player.ourRank}` : ""}
            </span>
          </span>
          <span className="flex shrink-0 gap-1">
            {NOTE_CATEGORIES.map((option) => {
              const existing = player.notes.find((row) => row.category === option.id);
              // The list being edited shows its verdict chip; the other lists
              // show a dim dot, so you can tell a player already has a take
              // elsewhere without leaving this list.
              if (!existing) return <span key={option.id} className="w-5 text-center text-[10px] text-muted-foreground">·</span>;
              return <span
                key={option.id}
                title={`${option.label}: ${existing.verdictLabel}`}
                className={`w-5 rounded-full text-center text-[10px] font-bold ring-1 ring-inset ${ANALYST_NOTE_STYLE[existing.verdict].className} ${option.id === category ? "" : "opacity-40"}`}
              >{ANALYST_NOTE_STYLE[existing.verdict].icon}</span>;
            })}
          </span>
        </button>)}
        {visible.length === 0 && <p className="p-6 text-center text-sm text-muted-foreground">No players match these filters.</p>}
        {visible.length > 400 && <p className="p-3 text-center text-xs text-muted-foreground">Showing the first 400 &mdash; narrow the search to reach the rest.</p>}
      </div>
    </section>

    <section className="space-y-4 rounded-2xl border bg-card p-5">
      {!selected ? <p className="py-16 text-center text-sm text-muted-foreground">Pick a player on the left to write or edit their note.</p> : <>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-2xl font-black">{selected.name}</h2>
            <p className="text-sm text-muted-foreground">
              {selected.position} · {selected.team ?? "FA"}
              {selected.ourRank !== null ? ` · our #${selected.ourRank}` : ""}
              {selected.adp !== null ? ` · ADP ${selected.adp.toFixed(1)}` : ""}
            </p>
          </div>
          {selectedNote?.updatedAt && <p className="text-xs text-muted-foreground">Last edited {new Date(selectedNote.updatedAt).toLocaleString()}</p>}
        </div>

        <div className="space-y-2">
          <label className="text-xs font-bold uppercase tracking-wide text-muted-foreground">Note list</label>
          <div className="flex flex-wrap gap-2">
            {NOTE_CATEGORIES.map((option) => {
              const existing = selected.notes.find((row) => row.category === option.id);
              return <button
                key={option.id}
                type="button"
                onClick={() => switchCategory(option.id)}
                className={`rounded-xl border px-3 py-2 text-left text-sm ${option.id === category ? "border-violet-500 bg-violet-50 ring-1 ring-violet-400" : "hover:bg-muted"}`}
              >
                <span className="block font-bold">{option.label}</span>
                <span className="block text-[11px] text-muted-foreground">
                  {existing ? `${ANALYST_NOTE_STYLE[existing.verdict].icon} ${existing.verdictLabel}` : option.blurb}
                </span>
              </button>;
            })}
          </div>
          <p className="text-xs text-muted-foreground">Each list holds its own note for this player. Saving one never touches the other.</p>
        </div>

        <div className="space-y-2">
          <label className="text-xs font-bold uppercase tracking-wide text-muted-foreground">Verdict</label>
          <div className="flex flex-wrap gap-2">
            {ANALYST_VERDICTS.map((value) => <button
              key={value}
              type="button"
              onClick={() => setVerdict(value)}
              className={`rounded-full px-3 py-1.5 text-sm font-bold ring-1 ring-inset ${ANALYST_NOTE_STYLE[value].className} ${verdict === value ? "outline outline-2 outline-offset-1 outline-violet-500" : "opacity-60"}`}
            >{ANALYST_NOTE_STYLE[value].icon} {ANALYST_NOTE_STYLE[value].label}</button>)}
          </div>
        </div>

        <div className="space-y-1">
          <label htmlFor="verdict-label" className="text-xs font-bold uppercase tracking-wide text-muted-foreground">
            Verdict label <span className="font-normal normal-case">(optional &mdash; defaults to &ldquo;{ANALYST_NOTE_STYLE[verdict].label}&rdquo;)</span>
          </label>
          <input
            id="verdict-label"
            value={verdictLabel}
            maxLength={MAX_VERDICT_LABEL_LENGTH}
            onChange={(event) => setVerdictLabel(event.target.value)}
            placeholder='e.g. "Strong target", "Fade at this price"'
            className="w-full rounded-lg border bg-background px-3 py-2 text-sm"
          />
        </div>

        <div className="space-y-1">
          <label htmlFor="note-body" className="text-xs font-bold uppercase tracking-wide text-muted-foreground">Note</label>
          <textarea
            id="note-body"
            value={note}
            rows={8}
            maxLength={MAX_NOTE_LENGTH}
            onChange={(event) => setNote(event.target.value)}
            placeholder="What you want to remember about this player on the clock."
            className="w-full rounded-lg border bg-background px-3 py-2 text-sm leading-relaxed"
          />
          <p className="text-right text-xs text-muted-foreground">{note.length} / {MAX_NOTE_LENGTH}</p>
        </div>

        <div className="rounded-xl border bg-muted/40 p-3">
          <p className="text-xs font-bold uppercase tracking-wide text-muted-foreground">Preview on the board</p>
          <div className="mt-2 flex items-center gap-2">
            <span className="font-bold">{selected.name}</span>
            <AnalystNoteMarker notes={preview ? [preview] : null} />
          </div>
          <p className="mt-1 text-xs text-muted-foreground">Hover the chip to see the tooltip exactly as the boards render it.</p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={save}
            disabled={pending || !note.trim()}
            className="rounded-xl bg-violet-700 px-4 py-2.5 text-sm font-black text-white disabled:cursor-not-allowed disabled:opacity-40"
          >{pending ? "Saving…" : selectedNote ? "Update note" : "Save note"}</button>
          {selectedNote && <button
            type="button"
            onClick={remove}
            disabled={pending}
            className="rounded-xl border border-rose-300 px-4 py-2.5 text-sm font-bold text-rose-700 disabled:opacity-40"
          >Delete</button>}
          {message && <span className={`text-sm font-semibold ${message.tone === "ok" ? "text-emerald-700" : "text-rose-700"}`}>{message.text}</span>}
        </div>
      </>}
    </section>
  </div>;
}
