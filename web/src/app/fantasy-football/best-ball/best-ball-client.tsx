"use client";

import { memo, useCallback, useMemo, useState, useSyncExternalStore } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import {
  BEST_BALL_POSITIONS,
  BEST_BALL_ROUNDS,
  BEST_BALL_TARGETS,
  BEST_BALL_TEAM_COUNT,
  canAddBestBallPlayer,
  getBestBallRosterStatus,
  parseBestBallDraftState,
  type BestBallPosition,
} from "@/lib/fantasy-football/best-ball";
import { buildSnakeSlots } from "@/lib/fantasy-football/draft-engine";
import ProjectionNotation from "../rankings/projection-notation";

const ROSTER_EVENT = "dfs-vegas-best-ball-draft";
const DRAFT_SLOTS = buildSnakeSlots(BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);

function subscribeToDraft(onChange: () => void) {
  window.addEventListener("storage", onChange);
  window.addEventListener(ROSTER_EVENT, onChange);
  return () => {
    window.removeEventListener("storage", onChange);
    window.removeEventListener(ROSTER_EVENT, onChange);
  };
}

function emptyDraftSnapshot() { return JSON.stringify({ userSlot: 1, playerIds: [] }); }

type BoardRowProps = {
  player: FantasyRankingRow;
  skillRank: number;
  canDraft: boolean;
  onDraft: (playerId: number) => void;
};

const BestBallBoardRow = memo(function BestBallBoardRow({ player, skillRank, canDraft, onDraft }: BoardRowProps) {
  return <tr className="border-t align-top hover:bg-muted/40"><td className="p-3 text-lg font-black">{skillRank}</td><td className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{player.position} · {player.team ?? "FA"} · Bye {player.byeWeek ?? "—"}</p></td><td className="max-w-[310px] p-3"><div className="flex flex-wrap gap-1">{player.indicators.slice(0,3).map((badge) => <span key={badge.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div></td><td className="p-3">{player.adp?.toFixed(1) ?? "—"}</td><td className="p-3">{player.fantasyPoints2025?.toFixed(1) ?? "—"}</td><td className="p-3 font-semibold">{player.ourProjectedPoints?.toFixed(1) ?? "—"}<ProjectionNotation details={player.projectionDetails} /></td><td className="p-3"><button disabled={!canDraft} onClick={() => onDraft(player.playerId)} className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-bold text-white disabled:opacity-35">Add</button></td></tr>;
});

export default function BestBallClient({ rankings, rankingSetId }: { rankings: FantasyRankingRow[]; rankingSetId: number }) {
  const storageKey = `dfs-vegas:dk-best-ball-draft:v2:${rankingSetId}`;
  const [name, setName] = useState("");
  const [position, setPosition] = useState("");
  const [team, setTeam] = useState("");
  const [viewTeam, setViewTeam] = useState<number | null>(null);
  const getDraftSnapshot = useCallback(() => localStorage.getItem(storageKey) || emptyDraftSnapshot(), [storageKey]);
  const snapshot = useSyncExternalStore(subscribeToDraft, getDraftSnapshot, emptyDraftSnapshot);
  const draft = useMemo(() => parseBestBallDraftState(snapshot), [snapshot]);
  const playerById = useMemo(() => new Map(rankings.map((player) => [player.playerId, player])), [rankings]);
  const skillRankById = useMemo(() => new Map(rankings.map((player, index) => [player.playerId, index + 1])), [rankings]);
  const draftedIds = useMemo(() => new Set(draft.playerIds), [draft.playerIds]);
  const currentSlot = DRAFT_SLOTS[draft.playerIds.length] ?? null;
  const currentTeamSlot = currentSlot?.teamSlot ?? null;

  const writeDraft = useCallback((next: { userSlot: number; playerIds: number[] }) => {
    localStorage.setItem(storageKey, JSON.stringify(next));
    window.dispatchEvent(new Event(ROSTER_EVENT));
  }, [storageKey]);

  const rosters = useMemo(() => {
    const result = new Map<number, FantasyRankingRow[]>();
    for (let slot = 1; slot <= BEST_BALL_TEAM_COUNT; slot += 1) result.set(slot, []);
    draft.playerIds.forEach((playerId, index) => {
      const player = playerById.get(playerId);
      const draftSlot = DRAFT_SLOTS[index];
      if (player && draftSlot) result.get(draftSlot.teamSlot)?.push(player);
    });
    return result;
  }, [draft.playerIds, playerById]);

  const currentRoster = useMemo(() => currentTeamSlot ? rosters.get(currentTeamSlot) ?? [] : [], [currentTeamSlot, rosters]);
  const currentStatus = useMemo(() => getBestBallRosterStatus(currentRoster), [currentRoster]);
  const displayTeam = viewTeam ?? currentTeamSlot ?? draft.userSlot;
  const displayRoster = useMemo(() => rosters.get(displayTeam) ?? [], [displayTeam, rosters]);
  const displayStatus = useMemo(() => getBestBallRosterStatus(displayRoster), [displayRoster]);
  const teams = useMemo(() => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(), [rankings]);

  const draftPlayer = useCallback((playerId: number) => {
    const latest = parseBestBallDraftState(getDraftSnapshot());
    if (latest.playerIds.includes(playerId) || latest.playerIds.length >= DRAFT_SLOTS.length) return;
    const slot = DRAFT_SLOTS[latest.playerIds.length];
    const roster = latest.playerIds.flatMap((id, index) => DRAFT_SLOTS[index]?.teamSlot === slot.teamSlot ? [playerById.get(id)] : []).filter((player): player is FantasyRankingRow => Boolean(player));
    const player = playerById.get(playerId);
    if (player && canAddBestBallPlayer(roster, player)) writeDraft({ ...latest, playerIds: [...latest.playerIds, playerId] });
  }, [getDraftSnapshot, playerById, writeDraft]);

  const undoLastPick = () => {
    const latest = parseBestBallDraftState(getDraftSnapshot());
    writeDraft({ ...latest, playerIds: latest.playerIds.slice(0, -1) });
  };
  const resetDraft = () => writeDraft({ userSlot: draft.userSlot, playerIds: [] });
  const setUserSlot = (userSlot: number) => {
    writeDraft({ ...draft, userSlot });
    setViewTeam(null);
  };

  const canDraftPosition = useMemo<Record<BestBallPosition, boolean>>(() => ({
    QB: Boolean(currentSlot) && currentStatus.size < 20 && currentStatus.counts.QB < 5,
    RB: Boolean(currentSlot) && currentStatus.size < 20,
    WR: Boolean(currentSlot) && currentStatus.size < 20,
    TE: Boolean(currentSlot) && currentStatus.size < 20 && currentStatus.counts.TE < 5,
  }), [currentSlot, currentStatus]);

  const filtered = useMemo(() => {
    const search = name.trim().toLocaleLowerCase();
    return rankings.filter((player) => (
      !draftedIds.has(player.playerId)
      && (!search || player.name.toLocaleLowerCase().includes(search))
      && (!position || player.position === position)
      && (!team || player.team === team)
    ));
  }, [rankings, draftedIds, name, position, team]);

  return <div className="space-y-6">
    <section className="rounded-2xl border bg-slate-950 p-5 text-white">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div><p className="text-xs font-bold uppercase tracking-widest text-blue-300">12-team · 20-round snake</p><h2 className="mt-1 text-2xl font-black">User-controlled draft room</h2><p className="mt-1 text-sm text-slate-300">Every Add records the current team&apos;s pick and advances the snake automatically.</p></div>
        <label className="text-xs font-bold uppercase tracking-wide text-slate-300">My draft position<select value={draft.userSlot} onChange={(event) => setUserSlot(Number(event.target.value))} className="mt-1 block rounded-lg border border-white/20 bg-slate-900 px-3 py-2 text-sm font-bold text-white">{Array.from({ length: 12 }, (_, index) => <option key={index + 1} value={index + 1}>Slot {index + 1}</option>)}</select></label>
      </div>
      <div className="mt-5 grid gap-3 md:grid-cols-[1fr_auto] md:items-center"><div>{currentSlot ? <><p className="text-sm text-slate-300">On the clock</p><p className="text-3xl font-black">{currentSlot.teamSlot === draft.userSlot ? "My Team" : `Team ${currentSlot.teamSlot}`} <span className="text-lg text-blue-300">· Pick {currentSlot.overallPick}/240 · Round {currentSlot.round}</span></p></> : <><p className="text-sm text-emerald-300">Draft complete</p><p className="text-3xl font-black">240 picks recorded</p></>}</div><div className="flex gap-2"><button disabled={!draft.playerIds.length} onClick={undoLastPick} className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold disabled:opacity-30">Undo last pick</button><button disabled={!draft.playerIds.length} onClick={resetDraft} className="rounded-lg border border-red-400/40 px-3 py-2 text-sm font-semibold text-red-200 disabled:opacity-30">Reset draft</button></div></div>
      <div className="mt-5 grid grid-cols-3 gap-2 md:grid-cols-6 xl:grid-cols-12">{Array.from({ length: 12 }, (_, index) => {
        const slot = index + 1;
        const count = rosters.get(slot)?.length ?? 0;
        const isCurrent = slot === currentTeamSlot;
        const isMine = slot === draft.userSlot;
        return <button key={slot} onClick={() => setViewTeam(slot)} className={`rounded-lg border p-2 text-left text-xs ${isCurrent ? "border-blue-300 bg-blue-500/25 ring-1 ring-blue-300" : isMine ? "border-emerald-400/60 bg-emerald-500/15" : "border-white/15 bg-white/5"}`}><b>{isMine ? "MY" : `T${slot}`}</b><span className="block text-slate-300">{count}/20</span></button>;
      })}</div>
    </section>

    <section className="grid gap-5 xl:grid-cols-[1fr_360px]">
      <div className="rounded-2xl border bg-card p-5">
        <div className="flex flex-wrap items-start justify-between gap-3"><div><p className="text-xs font-bold uppercase tracking-widest text-blue-700">{viewTeam === null ? "Following the clock" : "Roster review"}</p><h2 className="text-2xl font-black">{displayTeam === draft.userSlot ? "My Team" : `Team ${displayTeam}`} roster</h2></div><div className="flex items-center gap-2">{viewTeam !== null && <button onClick={() => setViewTeam(null)} className="rounded-lg border px-2 py-1 text-xs font-semibold">Follow clock</button>}<span className={`rounded-full px-3 py-1 text-xs font-bold ${displayStatus.valid ? "bg-emerald-100 text-emerald-800" : "bg-amber-100 text-amber-900"}`}>{displayStatus.valid ? "VALID" : `${displayStatus.size}/20`}</span></div></div>
        <div className="mt-4 grid grid-cols-4 gap-2">{BEST_BALL_POSITIONS.map((value) => <div key={value} className="rounded-xl bg-muted p-3 text-center"><p className="text-xs font-bold text-muted-foreground">{value}</p><p className="text-xl font-black">{displayStatus.counts[value]}<span className="text-xs font-normal text-muted-foreground"> / {BEST_BALL_TARGETS[value]}</span></p></div>)}</div>
        <p className="mt-2 text-xs text-muted-foreground">Targets use DraftKings&apos; default auto-draft guardrails. Manual drafts may use other valid constructions.</p>
        <div className="mt-4 flex flex-wrap gap-1.5">{displayStatus.gates.map((gate) => <span key={gate.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ${gate.pass ? "bg-emerald-100 text-emerald-800" : "bg-slate-100 text-slate-600"}`}>{gate.pass ? "✓" : "○"} {gate.label}</span>)}</div>
        <div className="mt-5 max-h-72 space-y-1 overflow-auto">{displayRoster.length ? displayRoster.map((player, index) => <div key={player.playerId} className="flex items-center gap-3 rounded-lg border px-3 py-2 text-sm"><span className="w-6 text-xs font-bold text-muted-foreground">{index + 1}</span><div className="min-w-0 flex-1"><b>{player.name}</b><span className="ml-2 text-xs text-muted-foreground">{player.position} · {player.team} · Bye {player.byeWeek ?? "—"}</span></div></div>) : <p className="rounded-xl bg-muted p-5 text-center text-sm text-muted-foreground">No picks recorded for this team.</p>}</div>
      </div>

      <aside className="space-y-4"><div className="rounded-2xl border bg-card p-5"><p className="text-xs font-bold uppercase tracking-widest text-blue-700">Current-team needs</p><div className="mt-3 grid grid-cols-4 gap-2">{BEST_BALL_POSITIONS.map((value) => <div key={value} className="text-center"><p className="text-xs text-muted-foreground">{value}</p><p className="font-black">{currentStatus.counts[value]}/{BEST_BALL_TARGETS[value]}</p></div>)}</div></div><div className="rounded-2xl border bg-slate-950 p-5 text-white"><p className="text-xs font-bold uppercase tracking-widest text-blue-300">Weekly starting lineup</p><p className="mt-2 text-2xl font-black">1 QB · 2 RB · 3 WR</p><p className="text-lg font-bold">1 TE · 1 FLEX</p><p className="mt-2 text-sm text-slate-300">DraftKings automatically selects the highest-scoring eight players each week.</p></div><div className="rounded-2xl border bg-card p-5"><h3 className="font-bold">Tournament rounds</h3><div className="mt-3 space-y-2 text-sm"><p><b>Round 1:</b> Weeks 1–14</p><p><b>Round 2:</b> Week 15</p><p><b>Round 3:</b> Week 16</p><p><b>Final:</b> Week 17</p></div></div></aside>
    </section>

    <section className="rounded-2xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950"><b>Current ranking basis:</b> the top 260 eligible players from our season-long PPR board plus current ADP. The DraftKings yardage bonuses, weekly spike distributions, player correlations, and Weeks 15–17 matchups are not yet incorporated into the rank.</section>

    <section className="space-y-3">
      <div className="grid gap-3 rounded-2xl border bg-card p-3 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px_auto] lg:items-end"><label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="Search player" className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground" /></label><label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Position<select value={position} onChange={(event) => setPosition(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All positions</option>{BEST_BALL_POSITIONS.map((value) => <option key={value} value={value}>{value}</option>)}</select></label><label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Team<select value={team} onChange={(event) => setTeam(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All teams</option>{teams.map((value) => <option key={value} value={value}>{value}</option>)}</select></label><button onClick={() => { setName(""); setPosition(""); setTeam(""); }} className="rounded-lg border px-3 py-2 text-sm font-semibold">Clear filters</button></div>
      <p className="text-xs text-muted-foreground">{filtered.length} available players · drafted players are removed from the board</p>
      <div className="overflow-x-auto rounded-2xl border bg-card"><table className="w-full min-w-[1050px] text-sm"><thead className="bg-muted text-left text-xs uppercase text-muted-foreground"><tr><th className="p-3">Skill rank</th><th className="p-3">Player</th><th className="p-3">Signals</th><th className="p-3">ADP</th><th className="p-3">2025 FPTS</th><th className="p-3">2026 PPR base</th><th className="p-3">Draft</th></tr></thead><tbody>{filtered.map((player) => <BestBallBoardRow key={player.playerId} player={player} skillRank={skillRankById.get(player.playerId) ?? 999} canDraft={canDraftPosition[player.position as BestBallPosition]} onDraft={draftPlayer} />)}</tbody></table>{filtered.length === 0 && <p className="border-t p-8 text-center text-sm text-muted-foreground">No available players match these filters.</p>}</div>
    </section>
  </div>;
}
