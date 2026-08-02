"use client";

import { useCallback, useMemo, useState, useSyncExternalStore } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import {
  BEST_BALL_POSITIONS,
  BEST_BALL_TARGETS,
  canAddBestBallPlayer,
  getBestBallRosterStatus,
} from "@/lib/fantasy-football/best-ball";
import ProjectionNotation from "../rankings/projection-notation";

const ROSTER_EVENT = "dfs-vegas-best-ball-roster";

function subscribeToRoster(onChange: () => void) {
  window.addEventListener("storage", onChange);
  window.addEventListener(ROSTER_EVENT, onChange);
  return () => {
    window.removeEventListener("storage", onChange);
    window.removeEventListener(ROSTER_EVENT, onChange);
  };
}

function emptyRosterSnapshot() { return "[]"; }

export default function BestBallClient({ rankings, rankingSetId }: { rankings: FantasyRankingRow[]; rankingSetId: number }) {
  const storageKey = `dfs-vegas:dk-best-ball:${rankingSetId}`;
  const [name, setName] = useState("");
  const [position, setPosition] = useState("");
  const [team, setTeam] = useState("");
  const getRosterSnapshot = useCallback(() => localStorage.getItem(storageKey) || "[]", [storageKey]);
  const rosterSnapshot = useSyncExternalStore(subscribeToRoster, getRosterSnapshot, emptyRosterSnapshot);
  const selectedIds = useMemo(() => {
    try {
      const saved = JSON.parse(rosterSnapshot);
      return Array.isArray(saved) ? saved.filter(Number.isInteger) : [];
    } catch { return []; }
  }, [rosterSnapshot]);
  const setSelectedIds = (value: number[] | ((current: number[]) => number[])) => {
    const next = typeof value === "function" ? value(selectedIds) : value;
    localStorage.setItem(storageKey, JSON.stringify(next));
    window.dispatchEvent(new Event(ROSTER_EVENT));
  };

  const teams = useMemo(() => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(), [rankings]);
  const selected = useMemo(() => selectedIds.flatMap((id) => {
    const player = rankings.find((row) => row.playerId === id);
    return player ? [player] : [];
  }), [rankings, selectedIds]);
  const status = useMemo(() => getBestBallRosterStatus(selected), [selected]);
  const filtered = useMemo(() => {
    const search = name.trim().toLocaleLowerCase();
    return rankings.filter((player) => (
      (!search || player.name.toLocaleLowerCase().includes(search))
      && (!position || player.position === position)
      && (!team || player.team === team)
    ));
  }, [rankings, name, position, team]);

  const add = (player: FantasyRankingRow) => {
    if (canAddBestBallPlayer(selected, player)) setSelectedIds((current) => [...current, player.playerId]);
  };
  const remove = (playerId: number) => setSelectedIds((current) => current.filter((id) => id !== playerId));

  return <div className="space-y-6">
    <section className="grid gap-5 xl:grid-cols-[1fr_360px]">
      <div className="rounded-2xl border bg-card p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div><p className="text-xs font-bold uppercase tracking-widest text-blue-700">My 20-player build</p><h2 className="text-2xl font-black">Roster construction</h2></div>
          <span className={`rounded-full px-3 py-1 text-xs font-bold ${status.valid ? "bg-emerald-100 text-emerald-800" : "bg-amber-100 text-amber-900"}`}>{status.valid ? "VALID" : `${status.size}/20`}</span>
        </div>
        <div className="mt-4 grid grid-cols-4 gap-2">{BEST_BALL_POSITIONS.map((value) => <div key={value} className="rounded-xl bg-muted p-3 text-center"><p className="text-xs font-bold text-muted-foreground">{value}</p><p className="text-xl font-black">{status.counts[value]}<span className="text-xs font-normal text-muted-foreground"> / {BEST_BALL_TARGETS[value]}</span></p></div>)}</div>
        <p className="mt-2 text-xs text-muted-foreground">Targets use DraftKings&apos; default auto-draft guardrails: 3 QB, 6 RB, 8 WR, 3 TE. Manual drafts may use other valid constructions.</p>
        <div className="mt-4 flex flex-wrap gap-1.5">{status.gates.map((gate) => <span key={gate.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ${gate.pass ? "bg-emerald-100 text-emerald-800" : "bg-slate-100 text-slate-600"}`}>{gate.pass ? "✓" : "○"} {gate.label}</span>)}</div>
        <div className="mt-5 max-h-72 space-y-1 overflow-auto">{selected.length ? selected.map((player, index) => <div key={player.playerId} className="flex items-center gap-3 rounded-lg border px-3 py-2 text-sm"><span className="w-6 text-xs font-bold text-muted-foreground">{index + 1}</span><div className="min-w-0 flex-1"><b>{player.name}</b><span className="ml-2 text-xs text-muted-foreground">{player.position} · {player.team} · Bye {player.byeWeek ?? "—"}</span></div><button onClick={() => remove(player.playerId)} className="text-xs font-semibold text-red-700">Remove</button></div>) : <p className="rounded-xl bg-muted p-5 text-center text-sm text-muted-foreground">Add players from the board to track a DraftKings build.</p>}</div>
        {selected.length > 0 && <button onClick={() => setSelectedIds([])} className="mt-3 text-xs font-semibold text-red-700 hover:underline">Clear roster</button>}
      </div>

      <aside className="space-y-4">
        <div className="rounded-2xl border bg-slate-950 p-5 text-white"><p className="text-xs font-bold uppercase tracking-widest text-blue-300">Weekly starting lineup</p><p className="mt-2 text-2xl font-black">1 QB · 2 RB · 3 WR</p><p className="text-lg font-bold">1 TE · 1 FLEX</p><p className="mt-2 text-sm text-slate-300">DraftKings automatically selects the highest-scoring eight players each week. The other 12 scores do not count.</p></div>
        <div className="rounded-2xl border bg-card p-5"><h3 className="font-bold">Tournament rounds</h3><div className="mt-3 space-y-2 text-sm"><p><b>Round 1:</b> Weeks 1–14</p><p><b>Round 2:</b> Week 15</p><p><b>Round 3:</b> Week 16</p><p><b>Final:</b> Week 17</p></div></div>
      </aside>
    </section>

    <section className="rounded-2xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950"><b>Current ranking basis:</b> the top 260 eligible players from our season-long PPR board plus current ADP. The DraftKings 300-yard passing and 100-yard rushing/receiving bonuses, weekly spike distributions, player correlations, and Weeks 15–17 matchups are not yet incorporated into the rank.</section>

    <section className="space-y-3">
      <div className="grid gap-3 rounded-2xl border bg-card p-3 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px_auto] lg:items-end">
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="Search player" className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground" /></label>
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Position<select value={position} onChange={(event) => setPosition(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All positions</option>{BEST_BALL_POSITIONS.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Team<select value={team} onChange={(event) => setTeam(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All teams</option>{teams.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <button onClick={() => { setName(""); setPosition(""); setTeam(""); }} className="rounded-lg border px-3 py-2 text-sm font-semibold">Clear filters</button>
      </div>

      <div className="overflow-x-auto rounded-2xl border bg-card"><table className="w-full min-w-[1050px] text-sm"><thead className="bg-muted text-left text-xs uppercase text-muted-foreground"><tr><th className="p-3">Skill rank</th><th className="p-3">Player</th><th className="p-3">Signals</th><th className="p-3">ADP</th><th className="p-3">2025 FPTS</th><th className="p-3">2026 PPR base</th><th className="p-3">Roster</th></tr></thead><tbody>{filtered.map((player) => {
        const skillRank = rankings.findIndex((row) => row.playerId === player.playerId) + 1;
        const selectedPlayer = selectedIds.includes(player.playerId);
        const canAdd = canAddBestBallPlayer(selected, player);
        return <tr key={player.playerId} className={`border-t align-top ${selectedPlayer ? "bg-blue-50" : "hover:bg-muted/40"}`}><td className="p-3 text-lg font-black">{skillRank}</td><td className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{player.position} · {player.team ?? "FA"} · Bye {player.byeWeek ?? "—"}</p></td><td className="max-w-[310px] p-3"><div className="flex flex-wrap gap-1">{player.indicators.slice(0,3).map((badge) => <span key={badge.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div></td><td className="p-3">{player.adp?.toFixed(1) ?? "—"}</td><td className="p-3">{player.fantasyPoints2025?.toFixed(1) ?? "—"}</td><td className="p-3 font-semibold">{player.ourProjectedPoints?.toFixed(1) ?? "—"}<ProjectionNotation details={player.projectionDetails} /></td><td className="p-3">{selectedPlayer ? <button onClick={() => remove(player.playerId)} className="rounded-lg border border-red-300 px-3 py-1.5 text-xs font-bold text-red-700">Remove</button> : <button disabled={!canAdd} onClick={() => add(player)} className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-bold text-white disabled:opacity-35">Add</button>}</td></tr>;
      })}</tbody></table></div>
    </section>
  </div>;
}
