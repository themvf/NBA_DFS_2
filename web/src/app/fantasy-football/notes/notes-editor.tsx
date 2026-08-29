"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { NoteEditorPlayer } from "@/db/queries-fantasy-football";
import {
  ANALYST_NOTE_STYLE,
  ANALYST_VERDICTS,
  MAX_NOTE_LENGTH,
  MAX_VERDICT_LABEL_LENGTH,
  type AnalystVerdict,
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
  const [verdict, setVerdict] = useState<AnalystVerdict>("fair");
  const [verdictLabel, setVerdictLabel] = useState("");
  const [note, setNote] = useState("");
  const [message, setMessage] = useState<{ tone: "ok" | "error"; text: string } | null>(null);

  const selected = useMemo(
    () => players.find((player) => player.playerId === selectedId) ?? null,
    [players, selectedId],
  );

  const visible = useMemo(() => {
    const term = search.trim().toLocaleLowerCase();
    return players.filter((player) => (
      (!term || player.name.toLocaleLowerCase().includes(term))
      && (!position || player.position === position)
      && (filter === "all" || (filter === "with-note" ? Boolean(player.note) : !player.note))
    ));
  }, [players, search, position, filter]);

  // Loading a player replaces the form wholesale. A blank verdict label falls
  // back to the verdict's own name at save time, so it is fine to leave empty.
  const select = (player: NoteEditorPlayer) => {
    setSelectedId(player.playerId);
    setVerdict(player.note?.verdict ?? "fair");
    setVerdictLabel(player.note?.verdictLabel ?? "");
    setNote(player.note?.note ?? "");
    setMessage(null);
  };

  const save = () => {
    if (!selected) return;
    setMessage(null);
    startTransition(async () => {
      const result = await saveFantasyPlayerNote({
        playerId: selected.playerId,
        verdict,
        verdictLabel,
        note,
      });
      if (result.ok) {
        setMessage({ tone: "ok", text: `Saved ${selected.name}. It is live on the boards now.` });
        router.refresh();
      } else {
        setMessage({ tone: "error", text: result.error ?? "Save failed." });
      }
    });
  };

  const remove = () => {
    if (!selected?.note) return;
    setMessage(null);
    startTransition(async () => {
      const result = await deleteFantasyPlayerNote({ playerId: selected.playerId });
      if (result.ok) {
        setNote("");
        setVerdictLabel("");
        setVerdict("fair");
        setMessage({ tone: "ok", text: `Deleted the note for ${selected.name}.` });
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
        listRank: selected.note?.listRank ?? null,
        sourceTeam: selected.note?.sourceTeam ?? null,
        sourceAdp: selected.note?.sourceAdp ?? null,
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
          {player.note
            ? <span className={`shrink-0 rounded-full px-2 py-0.5 text-[10px] font-bold ring-1 ring-inset ${ANALYST_NOTE_STYLE[player.note.verdict].className}`}>{ANALYST_NOTE_STYLE[player.note.verdict].icon}</span>
            : <span className="shrink-0 text-[10px] text-muted-foreground">—</span>}
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
          {selected.note?.updatedAt && <p className="text-xs text-muted-foreground">Last edited {new Date(selected.note.updatedAt).toLocaleString()}</p>}
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
            <AnalystNoteMarker note={preview} />
          </div>
          <p className="mt-1 text-xs text-muted-foreground">Hover the chip to see the tooltip exactly as the boards render it.</p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={save}
            disabled={pending || !note.trim()}
            className="rounded-xl bg-violet-700 px-4 py-2.5 text-sm font-black text-white disabled:cursor-not-allowed disabled:opacity-40"
          >{pending ? "Saving…" : selected.note ? "Update note" : "Save note"}</button>
          {selected.note && <button
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
