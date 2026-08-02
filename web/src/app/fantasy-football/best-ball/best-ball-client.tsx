"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import {
  BEST_BALL_POSITIONS,
  BEST_BALL_ROUNDS,
  BEST_BALL_TARGETS,
  BEST_BALL_TEAM_COUNT,
  canAddBestBallPlayer,
  getBestBallRosterStatus,
  parseBestBallDraftState,
  type BestBallDraftState,
  type BestBallPosition,
} from "@/lib/fantasy-football/best-ball";
import { buildSnakeSlots } from "@/lib/fantasy-football/draft-engine";
import BestBallDraftBoard from "./best-ball-draft-board";
import BestBallPlayerBoard from "./best-ball-player-board";
import BestBallAiAdvisor from "./best-ball-ai-advisor";

const DRAFT_SLOTS = buildSnakeSlots(BEST_BALL_TEAM_COUNT, BEST_BALL_ROUNDS);
const EMPTY_DRAFT: BestBallDraftState = { userSlot: 1, playerIds: [] };

function useBestBallDraft(storageKey: string) {
  const [draft, setDraft] = useState<BestBallDraftState>(EMPTY_DRAFT);
  const pendingDraftRef = useRef<BestBallDraftState | null>(null);
  const persistenceHandleRef = useRef<number | null>(null);
  const persistenceModeRef = useRef<"idle" | "timeout" | null>(null);

  const cancelPendingPersistence = useCallback(() => {
    if (persistenceHandleRef.current === null) return;
    const cancelIdle = (window as unknown as { cancelIdleCallback?: Window["cancelIdleCallback"] }).cancelIdleCallback;
    if (persistenceModeRef.current === "idle" && cancelIdle) cancelIdle.call(window, persistenceHandleRef.current);
    else window.clearTimeout(persistenceHandleRef.current);
    persistenceHandleRef.current = null;
    persistenceModeRef.current = null;
  }, []);

  const persistPendingDraft = useCallback(() => {
    if (pendingDraftRef.current) localStorage.setItem(storageKey, JSON.stringify(pendingDraftRef.current));
    pendingDraftRef.current = null;
    persistenceHandleRef.current = null;
    persistenceModeRef.current = null;
  }, [storageKey]);

  const schedulePersistence = useCallback((next: BestBallDraftState) => {
    pendingDraftRef.current = next;
    cancelPendingPersistence();
    const requestIdle = (window as unknown as { requestIdleCallback?: Window["requestIdleCallback"] }).requestIdleCallback;
    if (requestIdle) {
      persistenceModeRef.current = "idle";
      persistenceHandleRef.current = requestIdle.call(window, persistPendingDraft, { timeout: 400 });
    } else {
      persistenceModeRef.current = "timeout";
      persistenceHandleRef.current = window.setTimeout(persistPendingDraft, 0);
    }
  }, [cancelPendingPersistence, persistPendingDraft]);

  useEffect(() => {
    const initialLoad = window.setTimeout(() => {
      setDraft(parseBestBallDraftState(localStorage.getItem(storageKey) ?? ""));
    }, 0);
    const syncFromAnotherTab = (event: StorageEvent) => {
      if (event.key === storageKey) setDraft(parseBestBallDraftState(event.newValue ?? ""));
    };
    window.addEventListener("storage", syncFromAnotherTab);
    return () => {
      window.clearTimeout(initialLoad);
      window.removeEventListener("storage", syncFromAnotherTab);
      cancelPendingPersistence();
      persistPendingDraft();
    };
  }, [cancelPendingPersistence, persistPendingDraft, storageKey]);

  const updateDraft = useCallback((updater: (current: BestBallDraftState) => BestBallDraftState) => {
    setDraft((current) => {
      const next = updater(current);
      if (next !== current) schedulePersistence(next);
      return next;
    });
  }, [schedulePersistence]);

  return { draft, updateDraft };
}

export default function BestBallClient({ rankings, rankingSetId }: { rankings: FantasyRankingRow[]; rankingSetId: number }) {
  const storageKey = `dfs-vegas:dk-best-ball-draft:v2:${rankingSetId}`;
  const [viewTeam, setViewTeam] = useState<number | null>(null);
  const [activeView, setActiveView] = useState<"players" | "results">("players");
  const { draft, updateDraft } = useBestBallDraft(storageKey);
  const playerById = useMemo(() => new Map(rankings.map((player) => [player.playerId, player])), [rankings]);
  const currentSlot = DRAFT_SLOTS[draft.playerIds.length] ?? null;
  const currentTeamSlot = currentSlot?.teamSlot ?? null;

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

  const draftPlayer = useCallback((playerId: number) => {
    updateDraft((latest) => {
      if (latest.playerIds.includes(playerId) || latest.playerIds.length >= DRAFT_SLOTS.length) return latest;
      const slot = DRAFT_SLOTS[latest.playerIds.length];
      const roster = latest.playerIds.flatMap((id, index) => DRAFT_SLOTS[index]?.teamSlot === slot.teamSlot ? [playerById.get(id)] : []).filter((player): player is FantasyRankingRow => Boolean(player));
      const player = playerById.get(playerId);
      return player && canAddBestBallPlayer(roster, player) ? { ...latest, playerIds: [...latest.playerIds, playerId] } : latest;
    });
  }, [playerById, updateDraft]);

  const undoLastPick = () => updateDraft((latest) => latest.playerIds.length ? { ...latest, playerIds: latest.playerIds.slice(0, -1) } : latest);
  const resetDraft = () => updateDraft((latest) => latest.playerIds.length ? { userSlot: latest.userSlot, playerIds: [] } : latest);
  const setUserSlot = (userSlot: number) => {
    updateDraft((latest) => latest.userSlot === userSlot ? latest : { ...latest, userSlot });
    setViewTeam(null);
  };

  const canDraftPosition = useMemo<Record<BestBallPosition, boolean>>(() => ({
    QB: Boolean(currentSlot) && currentStatus.size < 20 && currentStatus.counts.QB < 5,
    RB: Boolean(currentSlot) && currentStatus.size < 20,
    WR: Boolean(currentSlot) && currentStatus.size < 20,
    TE: Boolean(currentSlot) && currentStatus.size < 20 && currentStatus.counts.TE < 5,
  }), [currentSlot, currentStatus]);

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

    <nav aria-label="Best Ball draft views" className="grid grid-cols-2 rounded-2xl border bg-card p-1.5 shadow-sm">
      <button onClick={() => setActiveView("players")} aria-pressed={activeView === "players"} className={`rounded-xl px-4 py-3 text-sm font-black transition ${activeView === "players" ? "bg-slate-950 text-white shadow" : "text-muted-foreground hover:bg-muted"}`}>Player Selection</button>
      <button onClick={() => setActiveView("results")} aria-pressed={activeView === "results"} className={`rounded-xl px-4 py-3 text-sm font-black transition ${activeView === "results" ? "bg-slate-950 text-white shadow" : "text-muted-foreground hover:bg-muted"}`}>Draft Results <span className="ml-1 text-xs font-semibold opacity-70">({draft.playerIds.length}/240)</span></button>
    </nav>

    {activeView === "results" ? (
      <BestBallDraftBoard rankings={rankings} playerIds={draft.playerIds} userSlot={draft.userSlot} />
    ) : <>

    <BestBallAiAdvisor
      key={`${rankingSetId}:${draft.userSlot}:${draft.playerIds.join(",")}`}
      rankingSetId={rankingSetId}
      userSlot={draft.userSlot}
      playerIds={draft.playerIds}
      onDraft={draftPlayer}
    />

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

    <BestBallPlayerBoard rankings={rankings} draftedPlayerIds={draft.playerIds} canDraftPosition={canDraftPosition} onDraft={draftPlayer} />
    </>}
  </div>;
}
